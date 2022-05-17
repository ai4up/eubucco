import os
import time
import ast
import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import Polygon
import rioxarray as rxr
import rasterio
from rasterio.features import shapes

from ufo_map.Utils.helpers import import_csv_w_wkt_to_gdf, save_csv_wkt, arg_parser, get_all_paths, write_stats
from preproc.db_set_up import fetch_GADM_info_country, clean_GADM_city_names


### WSF GENERIC #############


def get_file_names(file_dir):
    '''
    Get the list of all WSF tiles in a folder.
    '''
    File_Name = []
    for files in os.listdir(file_dir):
        if os.path.splitext(files)[1] == '.tif':
            File_Name.append(files)
    return File_Name


def create_wsf_bboxes(wsf_file_name):
    '''
    Create a geodataframe with the bounding box of each wsf tile
    for a list of tile names containing the lower-left corner coordinate of the bbox.

    Returns: gpd.GeoDataFrame with columns 'name' and 'geometry'
    '''
    bboxes = gpd.GeoSeries()
    for name in wsf_file_name:
        name = name.split('.')[0]
        bboxes = bboxes.append(gpd.GeoSeries(bbox_from_wsf_file_name(name)))
    wsf_bboxes = gpd.GeoDataFrame(crs="EPSG:4326", geometry=bboxes).reset_index(drop=True)
    wsf_bboxes['name'] = wsf_file_name
    return(wsf_bboxes)


def bbox_from_wsf_file_name(file_name):
    '''
    This function returns a bounding box as a shapely polygon taking
    as input a WSF file name, in degree with projection WGS4 (espg:4326).

    Example: WSF2019_v1_-100_16
    '''
    # get elements of the bounding box
    EW = np.arange(int(file_name.split('_')[2]), int(file_name.split('_')[2])+4, 2).tolist()
    NS = np.arange(int(file_name.split('_')[3]), int(file_name.split('_')[3])+4, 2).tolist()
    bbox = [EW[0], NS[0], EW[1], NS[1]]
    # get appropriate integers
    # latitude (w,e)
    # latitude (only n)
    return(Polygon([(bbox[0], bbox[1]), (bbox[0], bbox[3]),
                    (bbox[2], bbox[3]), (bbox[2], bbox[1])]))


### WSF 19 #############


def wsf_pixel_count_city(GADM_w_wsf,
                         city_name):
    '''
    Get the WSF pixel count for a city.
    '''
    # initialize pixel count
    count = 0
    # get the row of the city
    gadm_city = GADM_w_wsf[GADM_w_wsf.NAME_1 == city_name]
    # get GADM boundary
    gadm_boundary = gadm_city['geometry']
    # get list of wsf tiles for the boundary
    wsf_names = gadm_city.name.to_string(index=False).split()
    for wsf_name in wsf_names:
        # get the masked pixels for a tile
        with rasterio.open("C:/Users/jiawe/Documents/Nicola project/WSF/" + wsf_name + ".tif") as src:
            out_image, out_transform = rasterio.mask.mask(src, gadm_boundary, crop=True)
            out_meta = src.meta
        out_meta.update({"driver": "GTiff", "height": out_image.shape[1],
                         "width": out_image.shape[2], "transform": out_transform})
        # count the pixels retrieved and add them to the counter
        count += (out_image == 255).sum()
    return(count)


def wsf_pixel_counts_country(GADM_w_wsf,
                             path_wsf="C:/Users/jiawe/Documents/Nicola project/wsf/"):
    '''
    Get all WSF pixel counts in a country from WSF files.

    Returns: gpd.GeoDataFrame
    '''
    pixel_sum_array = [None]*len(GADM_w_wsf)

    for index, row in GADM_w_wsf.iterrows():
        city_name = row['NAME_1']
        print(city_name)
        if not city_name is None:
            pixel_sum_array[index] = wsf_pixel_count_city(GADM_w_wsf,
                                                          city_name)

    gdf = pd.DataFrame({'city_name': GADM_w_wsf['NAME_1'], 'pixel_count': pixel_sum_array})
    gdf = gdf["pixel_count"].groupby(gdf["city_name"]).sum()

    return(gdf)


### WSF EVO low #############


def polygonize_wsf(path_in, boundary84, crs):
    '''
        Masks the relevant WSF tiles with a GADM city boundary, polygonize and reproject
        the pixels into vector geometries

        Returns: gpd.GeoDataFrame
    '''
    img = rxr.open_rasterio(path_in, masked=True).squeeze().rio.clip(boundary84.geometry)
    img = img.rio.reproject(crs)
    out = gpd.GeoDataFrame.from_features(list({'properties': {'age_wsf': v},
                                               'geometry': s}
                                              for i, (s, v)
                                              in enumerate(shapes(img.values.astype(np.float32),
                                                                  transform=img.rio.transform()))
                                              )
                                         )
    return out.set_crs(crs)


def clean_age_wsf(wsf):
    '''
        Cleans ages not in existing value range (1985-2015)

        Returns: gpd.GeoDataFrame
    '''
    wsf = wsf[wsf.age_wsf < 2016]
    wsf = wsf[1984 < wsf.age_wsf]
    wsf['age_wsf'] = [val if 1984 < val < 2016 else 0 for val in wsf.age_wsf]
    return wsf.reset_index(drop=True)


def join_db_wsf(gdf, wsf):
    '''
        Joins the WSF polygons with building footprints. The age value kept is
        the one of the pixel with the largest intersection with the building.

        Returns:
            * gpd.GeoDataFrame with the joined ids and age attribute
            * float number of buildings for which no age attribute could be retrieved
    '''
    len_start = len(gdf)
    join = gpd.sjoin(gdf, wsf)
    join['area'] = [row.geometry.intersection(wsf.loc[row.index_right].geometry).area
                    for _, row in join.iterrows()]
    join = join.sort_values('area', ascending=False).drop_duplicates('id').sort_index()
    unmatched_bldgs = len_start-len(join)
    join = pd.merge(gdf, join, how='left', on='id')[['id', 'age_wsf']]
    return join, len_start, unmatched_bldgs


### WSF EVO high #############


def create_wsf_evo_matching(gadm_country_code):
    '''
        Matches cities with their respective WSF tiles.

        Creates: wsf-evo-matching.csv file with city name and WSF files overlaying a given city
                 as a list of paths

        Returns: None
    '''
    GADM_file, country_name, level_city, _ = fetch_GADM_info_country(gadm_country_code)

    wsf_bboxes = create_wsf_bboxes(get_file_names('/p/projects/eubucco/data/0-raw-data/wsf-evo/'))

    GADM_file = clean_GADM_city_names(GADM_file, country_name, level_city)

    out = gpd.sjoin(GADM_file, wsf_bboxes).groupby('city_name')['name'].apply(list).reset_index(name='file_paths')

    out['file_paths'] = [['/p/projects/eubucco/data/0-raw-data/wsf-evo/' + name for name in names]
                         for names in out.file_paths]

    out.to_csv(os.path.join('/p/projects/eubucco/data/2-database-city-level', country_name,
                            country_name+'_wsf-evo-matching.csv'), index=False)

    print('Created wsf evo matching file.')


def create_wsf_age(gadm_country_code,
                   left_over=False,
                   path_stats='/p/projects/eubucco/stats/3-wsf'):
    '''
        Matches buildings from the database to WSF evo to retrieve the age data from 1985 to 2015
        for all avaible buildings for city.

        Sucessively, masks the relevant WSF tiles with a GADM city boundary, polygonize and reproject
        the pixels into vector geometries that can be joined with the building footprints. The age
        value kept is the one of the pixel with the largest intersection with the building.

        Creates:
            * wsf-evo_age.csv: building id + age matched from wsf
            * wsf-evo_geoms.csv: polygonized pixels with positive values within the GADM city mask
                                 with the age values from the raster band as an attribute

        Returns: None
    '''
    start = time.time()

    _, country_name, _, local_crs = fetch_GADM_info_country(gadm_country_code)

    city_idx = arg_parser(['i']).i
    print(city_idx)

    paths = {}
    for file in ['geom', 'boundary', 'wsf-evo_geoms', 'wsf-evo_age']:
        if left_over == False:
            paths[file] = get_all_paths(country_name, filename=file)[city_idx]
        else:
            paths[file] = get_all_paths(country_name, filename=file, left_over=left_over)[city_idx]

    city_name = os.path.split(list(paths.values())[0])[-1][:-9]
    print(city_name)

    buildings = import_csv_w_wkt_to_gdf(paths['geom'], local_crs)
    boundary = import_csv_w_wkt_to_gdf(paths['boundary'], geometry_col='boundary_GADM_WGS84', crs=local_crs)

    wsf_matching = pd.read_csv(os.path.join('/p/projects/eubucco/data/2-database-city-level', country_name,
                                            country_name+'_wsf-evo-matching.csv'))
    paths_wsf = ast.literal_eval(wsf_matching[wsf_matching.city_name == city_name].file_paths.iloc[0])

    print('Parsing...')
    wsf = gpd.GeoDataFrame(crs=local_crs)
    for path in paths_wsf:
        wsf = wsf.append(polygonize_wsf(path, boundary, local_crs))

    wsf = clean_age_wsf(wsf)

    print('Matching...')
    buildings, n_bldgs, unmatched_bldgs = join_db_wsf(buildings, wsf)
    buildings.to_csv(paths['wsf-evo_age'], index=False)

    save_csv_wkt(wsf, paths['wsf-evo_geoms'])

    duration = time.time() - start
    stats = {'city_idx': city_idx, 'n_bldgs': n_bldgs, 'unmatched_bldgs': unmatched_bldgs}
    filename = f'{city_idx}_stat-parts'
    write_stats(stats, duration, path_stats, filename)


# CITY-LEVEL FT


def create_city_level_ft_area(path_sheet='gadm_table.csv',
                              path_root_folder='/p/projects/eubucco/data/0-raw-data/gadm',
                              path_out='/p/projects/eubucco/data/3-ml-inputs/wsf/'):
    '''
        Creates total footprint area per city.
    '''
    city_idx = arg_parser(['i']).i
    print(city_idx)

    GADM_sheet = pd.read_csv(os.path.join(path_root_folder, path_sheet))
    country_name = GADM_sheet.iloc[city_idx].country_name

    print(country_name)

    paths = get_all_paths(country_name, 'geom')

    cities = [None] * len(paths)
    regions = [None] * len(paths)
    tot_ft_area = [None] * len(paths)

    for n, path in enumerate(paths):

        city_name = os.path.split(path)[-1][:-9]
        print(city_name)

        cities[n] = city_name
        regions[n] = os.path.normpath(path).split(os.path.sep)[-3]

        try:
            tot_ft_area[n] = sum(import_csv_w_wkt_to_gdf(path, 3035).geometry.area)
        except:
            tot_ft_area[n] = 0

    out = pd.DataFrame()
    out['city_name'] = cities
    out['region_name'] = regions
    out['tot_ft_area'] = tot_ft_area
    out.to_csv(os.path.join(path_out, f'tot_ft_area_{country_name}.csv'), index=False)
