import os
import sys
import socket
import pandas as pd
import geopandas as gpd
import shapely
from shapely.geometry import shape
import ast
import glob
from datetime import date
import time
import psutil
from geojsplit import geojsplit
from pyrosm.data import sources
import subprocess
from pathlib import Path
import json

from ufo_map.Utils.helpers import *
from ufo_map.Preprocessing.parsing import *

pd.options.mode.chained_assignment = None

# set variables OSM
countries_w_sub_regions_osm = ['france', 'germany', 'italy', 'netherlands', 'poland']
dict_regions = {}
dict_regions['germany'] = sources.subregions.germany.available
dict_regions['italy'] = sources.subregions.italy.available
dict_regions['netherlands'] = sources.subregions.netherlands.available
dict_regions['poland'] = sources.subregions.poland.available
dict_regions['france'] = sources.subregions.france.available
not_france_mainland = ['guadeloupe', 'guyane', 'martinique', 'mayotte', 'reunion']
dict_regions['france'] = [region for region in dict_regions['france'] if region not in not_france_mainland]
buildings_cols = ['height', 'building:levels', 'start_date', 'building', 'building:use', 'amenity', 'building:material']
# non_buildings_cols = ['place','landuse','natural','water']

# set global variable for crs
CRS_UNI = 'EPSG:3035'

# LOW


def match_attrib_table(gdf, dataset_name, ID, file_path):
    '''
    Matches geometry and attribute files by ID when those are provided
    as two separate files.

    Returns: gpd.GeoDataFrame
    '''
    if dataset_name == 'abruzzo-gov':
        gdf_attrib = gpd.read_file(os.path.split(file_path)[0] + '/CR359G_UN_VOL_SUP.shp').to_crs(CRS_UNI)
        gdf = gdf[['CR01_IDOBJ', 'CR01_DESCR', 'geometry']]
        gdf_attrib = gdf_attrib[['CR359_UN_2', 'geometry']]
        gdf = gpd.sjoin(gdf_attrib, gdf, op='within')
        return(gdf.sort_values('CR359_UN_2', ascending=False).drop_duplicates('CR01_IDOBJ').sort_index())

    elif dataset_name == 'piemonte-gov':
        gdf_attrib = gpd.read_file(os.path.split(file_path)[0] + '/un_vol.shp')
        gdf_attrib = gdf_attrib[['UN_VOL_AV', 'geometry']]
        print(len(gdf))
        print(len(gdf_attrib))
        print('Spatial join in progress...')
        return(gpd.sjoin(gdf_attrib, gdf, op='within'))

    else:

        if dataset_name == 'lazio-gov':
            df_attrib = gpd.read_file(os.path.split(file_path)[0] + '/EDIFC_EDIFC_USO.dbf')
            ID_right = 'CLASSREF'
            df_attrib = df_attrib[['CLASSREF', 'EDIFC_USO']]

        if dataset_name == 'lombardia-gov':
            ID_right = ID
            # height
            df_attrib = gpd.read_file(os.path.split(file_path)[0] + '/Unita_volumetrica.dbf')
            df_attrib = df_attrib[['cr_edf_uui', 'un_vol_av']]
            df_attrib = df_attrib.sort_values('un_vol_av', ascending=False).drop_duplicates('cr_edf_uui').sort_index()
            gdf = gdf.merge(df_attrib, left_on=ID, right_on=ID_right, how='left')
            # type
            df_attrib = gpd.read_file(os.path.split(file_path)[0] + '/Categoria_uso_edificio.dbf')
            df_attrib = df_attrib[['cr_edf_uui', 'edifc_uso']]

        if dataset_name == 'sardegna-gov':
            ID_right = 'CorpUnVol'
            # we take the file path + the first 8 letters to get the ascending numbering of the 3 files in sardegna
            df_attrib = gpd.read_file(os.path.split(file_path)[0] +
                                      '/' +
                                      os.path.split(file_path)[1][0:8] +
                                      '_02_UNITA_VOLUMETRICA.dbf')
            df_attrib = df_attrib.sort_values('Altezza', ascending=False).drop_duplicates('CorpUnVol').sort_index()
            df_attrib = df_attrib[['CorpUnVol', 'Altezza']]

        if dataset_name == 'calabria-gov':
            ID_right = ID
            df_attrib = gpd.read_file(os.path.split(file_path)[0] + '/EDIFC_EDIFC_USO.dbf')
            df_attrib = df_attrib[['CLASSREF', 'EDIFC_USO']]

        if dataset_name == 'slovenia-gov':
            df_attrib_type = pd.read_csv(os.path.split(file_path)[0] + '/KS_SLO_KPR_20210724.csv', sep=';')
            df_attrib_height = pd.read_csv(os.path.split(file_path)[0] + '/KS_SLO_KST_20210724.csv', sep=';')
            df_attrib_link = pd.read_csv(os.path.split(file_path)[0] + '/KS_SLO_KDS_20210724.csv', sep=';')
            # merge type and link
            df_attrib_type = df_attrib_type.sort_values(
                'NAM_SIF', ascending=True).drop_duplicates('DST_SID').sort_index()
            df_attrib = pd.merge(df_attrib_link, df_attrib_type, left_on='DST_SID', right_on='DST_SID', how='left')
            # merge link and gdf, height and gdf remaining for final return merge
            df_attrib = df_attrib.sort_values('ETAZA', ascending=False).drop_duplicates('STA_SID').sort_index()
            df_attrib = df_attrib[['STA_SID', 'NAM_SIF', 'ETAZA']]
            df_attrib_height = df_attrib_height[['STA_SID', 'H2', 'H3']]
            gdf = pd.merge(gdf, df_attrib_height, left_on='SID', right_on='STA_SID', how='left')
            ID_right = 'STA_SID'

        return(gdf.merge(df_attrib, left_on=ID, right_on=ID_right, how='left'))


def pt(dataset_name):
    return(True if dataset_name == 'hamburg-gov' else False)


def get_params(i, path_or_dict):
    '''
    Get parameter dictionary from an input csv.
    Takes on line i of the input csv.
    Converts each variable from col_list into a dictionary.
    '''
    if type(path_or_dict)==str:
        p = pd.read_csv(path_or_dict).iloc[i - 1]
    elif type(path_or_dict)==dict:
        p = pd.Series(path_or_dict)
    else:
         sys.exit('get_params: type of path_or_dict not supported.')
    
    print(p)

    raw_type_map = p['type_map']
    p['type_map'] = ast.literal_eval(str(raw_type_map)) if pd.notna(raw_type_map) else None # converts either a dict or None
    
    raw_extra_attrib = p['extra_attrib']
    p['extra_attrib'] = ast.literal_eval(str(raw_extra_attrib)) if pd.notna(raw_extra_attrib) else None  # converts either a list, a dict or None
    
    col_list = ['id', 'height', 'type_source', 'age', 'floors', 'footprint']
    var_map = p[col_list].copy()
    for key in var_map.index:
        val = var_map[key]
        if isinstance(val, str) and val.strip() == 'None':
            var_map[key] = None
        elif pd.isna(val):
            var_map[key] = None
    p['var_map'] = var_map.to_dict()
    return(p.drop(labels=col_list))


def get_stats(df_result_attrib,
              dataset_name,
              file_paths,
              count_multipoly,
              max_ram_percent,
              end,
              dict_val_result
              ):
    '''
    Reports stats on the parsing run.

    Returns: pd.DataFrame
    '''

    # get time
    h_div = divmod(end, 3600)
    m_div = divmod(h_div[1], 60)

    stats = {}
    stats['dataset_name'] = dataset_name
    stats['n_bldgs'] = len(df_result_attrib)

    print("stats:")
    print(df_result_attrib)

    for col in ['height', 'type_source', 'age', 'floors']:
        len_col_vals = len([item for item in np.array(df_result_attrib[col]) if item != ''])
        stats['n_{}'.format(col)] = len_col_vals
        stats['frac_{}'.format(col)] = round(len_col_vals / stats['n_bldgs'], 3)

    stats['set_bldg_types'] = str(list(set(df_result_attrib.type_source)))
    stats['n_files'] = len(file_paths)
    stats['n_multi_p'] = count_multipoly
    stats['ram_alloc'] = round(psutil.virtual_memory().total / (1024 * 1024 * 1024), 2)
    stats['max_ram_used'] = max_ram_percent
    stats['date'] = date.today().strftime("%d/%m/%Y")
    stats['duration'] = '{} h {} m {} s'.format(h_div[0], m_div[0], round(m_div[1], 0))
    # append validation stats in case they are not empty
    if dict_val_result['invalid_geom_id']:
        stats['ID_invalid_geoms'] = [dict_val_result['invalid_geom_id']]
    if dict_val_result['empty_geom_id']:
        stats['ID_empty_geoms'] = [dict_val_result['empty_geom_id']]
    if dict_val_result['null_geom_id']:
        stats['ID_null_geoms'] = [dict_val_result['null_geom_id']]

    return(pd.DataFrame(stats, index=['0']))


def get_file_paths(dataset_name, path_input_folder, extra, extension):
    '''
    Fetches a list of path to individual files for a given source.
        * Base case: all files are in one folder
        * In Spain, both buildings and buildingparts are together and differenciation
        * For some shp cases, there are nested structure that we chose not to erase.

    Returns: list of paths

    ## TODO: establish list of sources where recursivity is needed.

    '''
    if dataset_name == 'spain-gov':
        file_paths = [f for f in glob.glob(path_input_folder
                                           + "/*building.gml")]

    elif dataset_name == 'sardegna-gov':
        path1 = path_input_folder + '/DBGT_CU1_02_EDIFICIO_INGOMBRO_SUOLO.shp'
        path2 = path_input_folder + '/DBGT_CU2_02_EDIFICIO_INGOMBRO_SUOLO.shp'
        path3 = path_input_folder + '/DBGT_CU3_02_EDIFICIO_INGOMBRO_SUOLO.shp'
        file_paths = [path1, path2, path3]

    elif dataset_name in ['lazio-gov', 'lombardia-gov', 'trentino-alto-adige-gov',
                          'calabria-gov', 'abruzzo-gov', 'piemonte-gov']:
        file_paths = [f for f in Path(path_input_folder).rglob(f'{extra}.shp')]

    elif dataset_name in ['valle-d-aosta-gov']:
        file_paths = [f for f in Path(path_input_folder).rglob('*' + f'{extra}.SHP')]

    elif dataset_name in ['vantaa-gov', 'helsinki-gov']:
        file_paths = [f for f in Path(path_input_folder).rglob('*.gml')]

    elif dataset_name == 'flanders-gov':
        file_paths = ['/p/projects/eubucco/data/0-raw-data/gvt-data/belgium/flanders/flanders-3035-raw.csv']

    elif dataset_name == 'brno-gov':
        file_paths = ['/p/projects/eubucco/data/0-raw-data/gvt-data/czechia/brno/brno-3035-raw.csv']
    
    elif path_input_folder.rsplit('/', 1)[1] == 'germany':
        file_paths = [os.path.join(path_input_folder, dataset_name + '.' + extension)]
    
    else:
        file_paths = [f for f in glob.glob(path_input_folder
                                           + "/*.{}".format(extension))]

    return(file_paths)


def get_extra_attribs(gdf, extra_attrib, extension, var_map, extra, file_path,id):
    '''Returns a pd.DataFrame with non-standard attributes potentially chosen.'''
    # create id if missing
    if extension in ['shp', 'dxf', 'pbf','csv.gz', 'pq', 'parquet']:
        # if shp format and have id, take id column
        if var_map['id'] is not None:
            gdf['id'] = gdf[var_map['id']]
        # if shp format and don't have id, create one
        else:
            gdf['id'] = id

    elif extension in ['gml', 'xml']:
        # if gml and no id, create one
        if 'id' not in gdf.columns:
            gdf['id'] = id

    if isinstance(extra_attrib, list):
        return(gdf[['id'] + extra_attrib])
    if isinstance(extra_attrib, dict) and extra == 'compute_height':
        return(gdf[['id', 'max_height'] + list(extra_attrib.keys())])
    if extra_attrib is None and extra == 'compute_height' and extension == 'gml':
        return(gdf[['id', 'max_height']])

    elif isinstance(extra_attrib, dict):
        return(gdf[['id'] + list(extra_attrib.keys())])
    else:
        sys.exit('Extra attributes are not specified in the correct way (either list or dict).')


def has_at_least_one_value(array):
    '''Returns true if one item in the array at least in not None.
    '''
    return any(np.not_equal(array, None))


def pbf_to_gdf_via_geojson(path_pbf, buildings_cols):  # non_buildings_cols
    '''
    Parses a pbf file by creating a temporary geojson which is loaded by batches.
    Keeps only desired columns not to blow up to RAM.

    Returns: gpd.GeoDataFrame
    '''
    print('Loading...')
    columns = buildings_cols + ['geometry']  # + non_buildings_cols
    path_geojson = '/p/projects/eubucco/data/tmp/tmp_osm_{}.geojson'.format(os.path.split(path_pbf)[-1].split('.')[0])

    # create geojson with only OSM polygons
    subprocess.run("osmium export --geometry-types=polygon -O -o  {} {}".format(path_geojson, path_pbf),
                   shell=True, check=True)

    # read geojson
    geojson = geojsplit.GeoJSONBatchStreamer(path_geojson)
    # import OSM polygons from Geojson to gdf
    osm_gdf = gpd.GeoDataFrame()

    for index, feature_collection in enumerate(geojson.stream(batch=10000)):
        # load new polys for the batch size
        new_polys = gpd.GeoDataFrame.from_features(feature_collection['features'])
        # add potential columns we want to keep that are not populated at all in the batch
        for column in columns:
            if column not in new_polys.columns:
                new_polys[column] = ''
        # remove columns we do not want
        new_polys = new_polys[columns]
        # append to gdf
        osm_gdf = osm_gdf.append(new_polys)

    # remove tmp geojson
    os.remove(path_geojson)
    # all non buildings to None
    osm_gdf = osm_gdf.replace({np.nan: None, '': None})

    # osm_gdf.to_csv('/p/projects/eubucco/data/--tmp/osm_gdf.csv')
    # print('In total, there are {} polygons in the OSM file.'.format(len(osm_gdf)))
    # print('There are {} land-use-identified polygons in the OSM file.'.format(len(osm_gdf.landuse.dropna())))
    # print('There are {} natural-identified polygons in the OSM file.'.format(len(osm_gdf.natural.dropna())))
    # print('There are {} water-identified polygons in the OSM file.'.format(len(osm_gdf.water.dropna())))
    # print('There are {} place-identified polygons in the OSM file.'.format(len(osm_gdf['place'].dropna())))

    return osm_gdf


# def remove_non_buildings_osm(osm_gdf,relevant_cols,keep=None):
#     '''
#     Keeps OSM buildings that have at least one value or do not have any values for several
#     OSM attributes / gdf columns.

#     Returns: gpd.GeoDataFrame
#     '''
#     print('Removing...')
#     # get values
#     cols_values = osm_gdf[relevant_cols].values
#     # check if there is at one value not null
#     osm_gdf['drop_row'] = np.array(list(map(has_at_least_one_value,cols_values)))
#     # kick these out!
#     if keep:
#         # keep the rows where there is at least one value =! None
#         osm_gdf = osm_gdf[osm_gdf['drop_row']==True].reset_index(drop=True)
#     else:
#         # keep the rows where there is no None values
#         osm_gdf = osm_gdf[osm_gdf['drop_row']==False].reset_index(drop=True)

#     osm_gdf.drop(columns=['drop_row'],inplace=True)
#     print('There are {} polygons left.'.format(len(osm_gdf)))

#     return osm_gdf


def clean_buildings_osm(osm_gdf):
    '''
    Cleans OSM float variables and types variables preparsed.

    Returns: gpd.GeoDataFrame
    '''
    print('Cleaning...')
    # cleaning heights, levels and age
    for col in ['height', 'building:levels', 'start_date']:
        osm_gdf[col] = osm_gdf[col].replace({None: np.nan})
        osm_gdf[col] = osm_gdf[col].astype(str).str.extract("(\\d+\\.?(\\d+)?)")[0]
        osm_gdf[col] = osm_gdf[col].astype(float)
        # to keep it coherent with rest we set all np.nan to ''
        osm_gdf.loc[osm_gdf[col].isnull(), col] = ''

    # check if type info
    osm_gdf = osm_gdf.replace({'yes': None})
    type_cols_values = osm_gdf[['building', 'building:use', 'amenity']].values
    osm_gdf.insert(4, 'has_type', np.array(list(map(has_at_least_one_value, type_cols_values))))

    return osm_gdf


def explode_spain_multipoly(gdf):
    '''
        Explodes multipolygon buildings into singles buildings for Spain.
        Renames buildings ids for multipolygons.
    '''
    count = len(get_indexes_multipoly(gdf))
    len1 = len(gdf)
    gdf = gdf.explode()
    print(f'Additional buildings from multipolygons: {len(gdf)-len1}')
    indexes = [item[1] for item in gdf.index]
    gdf = gdf.droplevel(0).reset_index(drop=True)
    gdf['id'] = [id_ if ind == 0 else str(id_) + '_part_' + str(ind) for id_, ind in zip(gdf.id, indexes)]
    return(gdf, count)


# MID


def parse_osm_buildings(path_pbf):
    '''
    This functions reads and converts OSM pbf files into gdp.GeoDataframe extracting
    only buildings and attributes of interest.

    Returns: gpd.GeoDataFrame
    '''
    columns = buildings_cols + ['geometry']  # + non_buildings_cols
    osm_gdf = pbf_to_gdf_via_geojson(path_pbf, buildings_cols)  # non_buildings_cols)

    # # method 1: removing buildings from non buildings columns
    # osm_gdf = remove_non_buildings_osm(osm_gdf,non_buildings_cols,keep=False)
    # # method 2: removing buildings with no building attributes
    # osm_gdf = remove_non_buildings_osm(osm_gdf,buildings_cols,keep=True)
    osm_gdf = osm_gdf.dropna(subset=['building'])

    # remove useless columns
    # osm_gdf = osm_gdf[buildings_cols+['geometry']]

    # cleaning buildings
    osm_gdf = clean_buildings_osm(osm_gdf)

    # reset index
    osm_gdf = osm_gdf.reset_index(drop=True)

    # ensure that there are only geometries in the geometries
    non_poly_indexes = [
        i for i,
        g in enumerate(
            osm_gdf.geometry) if type(g) not in [
            shapely.geometry.multipolygon.MultiPolygon,
            shapely.geometry.polygon.Polygon]]
    if non_poly_indexes != []:
        print('Non geometry objects in the geometry column!!:')
        print(non_poly_indexes)
        osm_gdf = osm_gdf.drop(osm_gdf.index[non_poly_indexes])

    ## testing###
    print(len(osm_gdf))
    print(osm_gdf)

    return osm_gdf


def clean_geom(gdf, dataset_name):
    '''
    Returns original dataframe with only valid 2D polygons.
    '''
    # initialise validation list
    list_geom_is_empty = []
    list_geom_is_null = []

    # in case gdf contains empty geometries
    if any(gdf.geometry.isnull()) or any(gdf.geometry.is_empty):

        # get id of buildings with empty or null geometries
        if 'id' in gdf.columns:
            list_geom_is_empty = list(gdf.id.loc[gdf.geometry.is_empty])
            list_geom_is_null = list(gdf.id.loc[gdf.geometry.isnull()])
        else:
            # if no id given save original index before deleting them
            list_geom_is_empty = list(gdf.loc[gdf.geometry.is_empty].index)
            list_geom_is_empty = [str(i) for i in list_geom_is_empty]
            list_geom_is_null = list(gdf.loc[gdf.geometry.isnull()].index)
            list_geom_is_null = [str(i) for i in list_geom_is_null]

        # remove rows with empty or null geometries
        len1 = len(gdf)
        gdf = gdf.loc[~gdf.geometry.is_empty]
        len2 = len(gdf)
        gdf = gdf.loc[~gdf.geometry.isnull()]
        len3 = len(gdf)
        print('WARNING (2)! Removed {} buildings with empty and {} with null geoms.'.format(len1 - len2, len2 - len3))

    if dataset_name == 'spain-gov':
        gdf, count = explode_spain_multipoly(gdf)
    else:
        gdf, count = combined_multipoly_to_poly(gdf)

    gdf = drop_z(gdf)

    return(gdf, count, list_geom_is_empty, list_geom_is_null)


def parse_microsoft(file_path,
                  dataset_name,
                  var_map):
    '''
    Parser function for Microsoft data provided as zipped csvs with json structure.
    Function design to parse a single file at the time.
    Ensures that cleaned footprint geometries and attributes are present.
    Reports the number of multipolygons encountered and removed.

    Returns: tuple(pd.DataFrame,integer,dict)
    '''
    
    gdf = pd.read_json(file_path,lines=True)
    gdf['geometry'] = gdf['geometry'].apply(shape)
    gdf = gpd.GeoDataFrame(pd.json_normalize(gdf['properties']),geometry=gdf['geometry'],crs=4326)

    # reproject to uni crs!
    gdf = gdf.to_crs(CRS_UNI)

    # check for invalid geometries
    dict_val = {}
    # do we need to check here? gdf would not be created with wrong geom, no?
    dict_val['invalid_geom'] = []

    # in case we have an id, save ids and temporarily rename column id
    if var_map['id'] is not None:
        # a) save id in invalid geom
        dict_val['invalid_geom_id'] = list(gdf[var_map['id']].loc[gdf.geometry.is_valid == False])
        # b) rename id col in gdf, to simplify collecton of IDs in clean_geom
        gdf = gdf.rename(columns={var_map['id']: 'id'})
    else:
        # where we don't have an id, we save index of original df
        dict_val['invalid_geom_id'] = list(gdf.loc[gdf.geometry.is_valid == False].index)
        # to save time only convert to str if dict_val is not empty
        if dict_val['invalid_geom_id']:
            dict_val['invalid_geom_id'] = [str(i) for i in dict_val['invalid_geom_id']]

    # remove unecessary geometrical info, convert geoms to wkt and delecte empty or null geoms
    gdf, count_multipoly, dict_val['empty_geom_id'], dict_val['null_geom_id'] = clean_geom(gdf, dataset_name)

    # rename gdf id col to old name, to later capture it in clean_attributes
    # TODO: future version should name gdf columns already in parsing_tabular to be coherent with parsing_gml
    if var_map['id'] is not None:
        gdf = gdf.rename(columns={'id': var_map['id']})

    return(gdf, count_multipoly, dict_val)


def parse_tabular(file_path,
                  dataset_name,
                  var_map):
    '''
    Parser function for government data provided as shp or dxf.
    Function design to parse a single file at the time.
    Ensures that cleaned footprint geometries in wkt and attributes are present.
    Reports the number of multipolygons encountered and removed.

    Returns: tuple(pd.DataFrame,integer,dict)
    '''

    # read file as gpd
    if 'osm' in dataset_name:
        gdf = parse_osm_buildings(file_path)
        gdf = gdf.set_crs('EPSG:4326')
    # edge case flanders (only inserted due to the PIK clutser having issues with local crs leading to inf geoms)
    elif dataset_name == 'flanders-gov':
        gdf = import_csv_w_wkt_to_gdf(file_path, crs=CRS_UNI)
    # edge case brno (only inserted due to the PIK clutser having issues with local crs leading to inf geoms)
    elif dataset_name == 'brno-gov':
        gdf = import_csv_w_wkt_to_gdf(file_path, crs=CRS_UNI)
    elif file_path.endswith('.pq') or file_path.endswith('.parquet'):
        gdf = gpd.read_parquet(file_path)
    else:
        gdf = gpd.read_file(file_path)

    # reproject to uni crs!
    gdf = gdf.to_crs(CRS_UNI)

    # get the ID col name from variable mapper
    try:
        ID = var_map['id']
    except BaseException:
        pass

    # (if relevant) match attribute file
    if dataset_name in ['lazio-gov', 'lombardia-gov', 'slovenia-gov', 'sardegna-gov',
                        'piemonte-gov', 'calabria-gov', 'abruzzo-gov']:
        gdf = match_attrib_table(gdf, dataset_name, ID, file_path)

    # (if relevant) filter rows and do matching walls/ground
    # NOTE TO SELF / TODO: for DK, we need first the footprints from the INSPIRE dataset
    if dataset_name == 'prague-gov':
        gdf = walls_to_height_shp(gdf, ID, elem_shp=['TYP', 'zakladova deska', 'svisla obvodova stena'])
    if dataset_name == 'brno-gov':
        gdf = walls_to_height_shp(gdf, ID, elem_shp=['TYP', 'zakladova_deska', 'svisla_stena'])
    if dataset_name == 'flanders-gov':
        # only take type ‘building on the ground’ and remove annex
        gdf = gdf[gdf.ENTITEIT == 'Gbg']
    if dataset_name == 'sicilia-gov':
        gdf['computed_height'] = gdf.quotagrond - gdf.quotaterra
    # calculate height from 3d polygons
    if dataset_name == 'toscana-gov':
        # convert comma to dot and then to float in every row that is not None
        gdf['q_gronda'] = np.where(
            gdf.q_gronda.notnull(),
            gdf['q_gronda'].str.replace(
                ',',
                '.').apply(
                pd.to_numeric),
            None)
        gdf['q_terra'] = np.where(
            gdf.q_terra.notnull(),
            gdf['q_terra'].str.replace(
                ',',
                '.').apply(
                pd.to_numeric),
            None)
        # compute height from subtracting 'q_terra' from 'q_gronda'
        gdf['computed_height'] = np.where(
            gdf.q_terra.notnull() & gdf.q_gronda.notnull(),
            gdf['q_gronda'] - gdf['q_terra'],
            None)

    if dataset_name == 'netherlands-gov':
        gdf['computed_height'] = gdf.H_DAK_70P - gdf.H_MAAIVELD
    if dataset_name == 'slovenia-gov':
        gdf['computed_height'] = gdf.H2 - gdf.H3

    # check for invalid geometries
    dict_val = {}
    # do we need to check here? gdf would not be created with wrong geom, no?
    dict_val['invalid_geom'] = []

    # in case we have an id, save ids and temporarily rename column id
    if var_map['id'] is not None:
        # a) save id in invalid geom
        dict_val['invalid_geom_id'] = list(gdf[var_map['id']].loc[gdf.geometry.is_valid == False])
        # b) rename id col in gdf, to simplify collecton of IDs in clean_geom
        gdf = gdf.rename(columns={var_map['id']: 'id'})
    else:
        # where we don't have an id, we save index of original df
        dict_val['invalid_geom_id'] = list(gdf.loc[gdf.geometry.is_valid == False].index)
        # to save time only convert to str if dict_val is not empty
        if dict_val['invalid_geom_id']:
            dict_val['invalid_geom_id'] = [str(i) for i in dict_val['invalid_geom_id']]

    # remove unecessary geometrical info, convert geoms to wkt and delecte empty or null geoms
    gdf, count_multipoly, dict_val['empty_geom_id'], dict_val['null_geom_id'] = clean_geom(gdf, dataset_name)

    # rename gdf id col to old name, to later capture it in clean_attributes
    # TODO: future version should name gdf columns already in parsing_tabular to be coherent with parsing_gml
    if var_map['id'] is not None:
        gdf = gdf.rename(columns={'id': var_map['id']})

    return(gdf, count_multipoly, dict_val)


def add_floors_spain_gml(gdf, file_path, local_crs, crs_uni=CRS_UNI, dataset_name='spain-gov'):
    '''
    Parse a buildingpart gml file associated to a building gml file in the
    spanish cadaster data, takes the max number of floors per building and
    add it to a gdf where the main building gml file for the city has been
    parsed.

    '''
    file_path_part = file_path[:-4] + 'part' + file_path[-4:]

    gml_bld_part = etree.parse(file_path_part)
    gml_bld_part_root = gml_bld_part.getroot()
    bldg_part_elem = 'bu-ext2d:BuildingPart'
    bldg_part_elem_list = get_bldg_elements(gml_bld_part, gml_bld_part_root, bldg_part_elem)

    footprints = get_footprints('gml:Surface//gml:posList',
                                bldg_part_elem_list,
                                gml_bld_part_root, local_crs, mode='2d')

    gdf_parts = gpd.GeoDataFrame(geometry=gpd.GeoSeries([item[0] for item in footprints]),
                                 crs=local_crs).to_crs(crs_uni)

    gdf_parts['floors'] = get_var_attrib('bu-ext2d:numberOfFloorsAboveGround',
                                         bldg_part_elem_list,
                                         gml_bld_part_root,
                                         dataset_name)

    return pd.merge(gdf,
                    gpd.sjoin(gdf, gdf_parts, how='left', predicate='contains').groupby('id')['floors'].max(),
                    how='left',
                    left_on='id',
                    right_index=True)


def parse_gml(file_path,
              dataset_name,
              bldg_elem,
              var_map,
              local_crs,
              extra,
              extra_attrib,
              n_files
              ):
    '''
    Parser function for government data provided as gml or xml.
    Function design to parse a single file at the time.
    Ensures that cleaned footprint geometries in wkt and attributes are present.
    Reports the number of multipolygons encountered and removed.

    If extra variables need to be added, they need to be specified in the parameter `extra_attrib`.
    This parameter should be a dictionary with the following structure:
        extra_attrib = {'variable_name_1':[<'var_attrib' or 'uni_attrib'>,'variable_element_name'],
                      'variable_name2': [...}

    Returns: tuple(pd.DataFrame,integer)
    '''

    # open file and get root element
    if socket.gethostname() == '60-MCC':
        file_path = r'{}'.format(file_path)

    gml = etree.parse(file_path)
    gml_root = gml.getroot()

    # get list of building elements
    bldg_elem_list = get_bldg_elements(gml, gml_root, bldg_elem)
    if n_files == 1:
        print(f'Element list retrieved: {len(bldg_elem_list)} elements')
        print('RAM use: {}%'.format(psutil.virtual_memory().percent))

    print(f'Element list retrieved: {len(bldg_elem_list)} elements')

    # check CRS spain
    if dataset_name == 'spain-gov':
        try:
            local_crs = bldg_elem_list[0].findall(".//gml:Envelope", gml_root.nsmap)[0].attrib['srsName']
        except BaseException:
            print('First building had no CRS')

    # get list of building attributes
    cols = {}
    if var_map['id'] is not None:
        cols['id'] = get_ids(bldg_elem_list, gml_root)
    if var_map['height'] is not None:
        cols['height'] = get_var_attrib(var_map['height'], bldg_elem_list, gml_root, dataset_name)

    if var_map['floors'] is not None:
        cols['floors'] = get_var_attrib(var_map['floors'], bldg_elem_list, gml_root, dataset_name)

    # edge case: type is given as xlink
    if dataset_name in ['cyprus-gov', 'czechia-gov', 'mecklenburg-vorpommern-gov']:
        if dataset_name == 'cyprus-gov':
            key = '{http://www.opengis.net/gml/3.2}remoteSchema'
        if dataset_name == 'czechia-gov':
            key = '{http://www.w3.org/1999/xlink}title'
        if dataset_name == 'mecklenburg-vorpommern-gov':
            key = '{http://www.w3.org/1999/xlink}href'
        cols['type_source'] = get_curr_use_attrib(var_map['type_source'], bldg_elem_list, gml_root, key)
    elif var_map['type_source'] is not None:
        cols['type_source'] = get_uni_attrib(var_map['type_source'], bldg_elem_list, gml_root, dataset_name)

    if var_map['age'] is not None:
        cols['age'] = get_uni_attrib(var_map['age'], bldg_elem_list, gml_root, dataset_name)

    # get additional building attributes
    if extra_attrib is not None:
        for var in extra_attrib.keys():
            if extra_attrib[var][0] == 'var_attrib':
                cols[var] = get_var_attrib(extra_attrib[var][1], bldg_elem_list, gml_root, dataset_name)
            elif extra_attrib[var][0] == 'uni_attrib':
                cols[var] = get_uni_attrib(extra_attrib[var][1], bldg_elem_list, gml_root, dataset_name)
            elif extra_attrib[var][0] == 'classify_roof':
                cols[var] = get_roof_type_from_lod2(extra_attrib[var][1], bldg_elem_list, gml_root)
            else:
                sys.exit('Var type is is unknown.')

    # get footprints
    if extra == '2d':
        mode = '2d'
    else:
        mode = '3d'

    # get_footprints returns a list of list including a polygon and a marker indicating an invalid polygon
    list_geoms_index = get_footprints(var_map['footprint'], bldg_elem_list, gml_root, local_crs, dataset_name,
                                      pt=pt(dataset_name),
                                      # solid=extra,
                                      mode=mode)

    if dataset_name != 'hamburg-gov':
        # allocating geometries and invalid marker to respective column
        cols['geometry'] = [item[0] for item in list_geoms_index]
        # for now we keep all the FALSE, as we have to match to an ID, which in
        # some cases comes first in clean_attributes
        cols['invalid_geom'] = [item[1] for item in list_geoms_index]
    else:
        cols['geometry'] = list_geoms_index
        cols['invalid_geom'] = [False] * len(list_geoms_index)

    # (if needed) compute heights
    if extra == 'compute_height':
        cols['height'] = get_min_heights_roof(bldg_elem_list, gml_root)
        cols['max_height'] = get_max_heights_wall(bldg_elem_list, gml_root)

    if (extra == 'solid' and var_map['height'] is None):
        cols['height'] = get_max_heights_wall(bldg_elem_list, gml_root, wall_elem='gml:surfaceMember//gml:posList')

    # for col in cols.keys(): print(f'{col}: {len(cols[col])}')

    # assemble gdf
    gdf = gpd.GeoDataFrame(cols).set_geometry('geometry')
    gdf = gdf.set_crs(local_crs)
    print('set to local crs: ', local_crs)
    # reproject to crs_uni
    gdf = gdf.to_crs(CRS_UNI)
    print('reprojected to crs_uni: ', gdf.crs)

    if dataset_name == 'helsinki-gov':
        gdf['height'] = gdf['LowestRoof'].astype(float) - gdf['GroundLevel'].astype(float)

    # get building ids with invalid geoms
    dict_val = {}
    # in case we have and id save id in invalid geom
    if 'id' in gdf.columns:
        dict_val['invalid_geom_id'] = list(gdf.id.loc[gdf.invalid_geom])
    else:
        # where we don't have it, we save id of original df
        dict_val['invalid_geom_id'] = list(gdf.loc[gdf.invalid_geom].index)
        dict_val['invalid_geom_id'] = [str(i) for i in dict_val['invalid_geom_id']]
    # remove unecessary geometrical info, and get empty and null geom indexes

    gdf, count_multipoly, dict_val['empty_geom_id'], dict_val['null_geom_id'] = clean_geom(gdf, dataset_name)

    # edge case: spain
    if dataset_name == 'spain-gov':
        gdf = add_floors_spain_gml(gdf, file_path, local_crs)

    gdf['geometry'] = [geom.wkt for geom in gdf.geometry]

    if dataset_name == 'spain-gov':
        return(gdf, count_multipoly, dict_val, local_crs)
    else:
        return(gdf, count_multipoly, dict_val)


def clean_attributes(df,
                     extension,
                     dataset_name,
                     var_map,
                     # type_map,
                     file_path,
                     extra,
                     id_counter
                     ):
    '''
    Harmonizes building attributes for ingestion in the database.
    Removes unecessary variables and cleans relevant ones.
    Separates attributes and geometries in two dataframes that can be matched
    again by ID.

    Returns: tuple(pd.DataFrame,pd.DataFrame)
    '''

    if extension in ['shp', 'dxf', 'pbf','csv.gz', 'pq', 'parquet']:
        # get variables/columns existing in the inputs
        list_var_source = [var_map[i] for i in var_map.keys() if var_map[i] is not None]
        list_var = [i for i in var_map.keys() if var_map[i] is not None]
        # add computed height if applicable
        if extra == 'compute_height' and extension == 'shp':
            df = df[list_var_source + ['computed_height', 'geometry']]
            df = df.rename(columns={var_map[elem]: elem for elem in var_map.keys()})
            df = df.rename(columns={'computed_height': 'height'})
        else:
            # filter relevant columns, using variable names in params
            df = df[list_var_source + ['geometry']]
            # rename relevant columns (reverse keys and values)
            df = df.rename(columns={var_map[elem]: elem for elem in var_map.keys()})
    elif extension in ['gml', 'xml']:
        # get variables/columns existing in the inputs
        list_var = [i for i in var_map.keys() if i not in ['footprint', 'wall'] and var_map[i] is not None]

        if extra == 'compute_height':
            df = df[list_var + ['height', 'max_height', 'geometry']]

        if (extra == 'solid' and var_map['height'] is None):
            df = df[list_var + ['height', 'geometry']]

    else:
        sys.exit('Cleaning attributes: The extension provided is unknown.')

    print('Variables parsed: {}'.format(list_var))

    # add missing columns
    cols = ['id', 'height', 'type_source', 'type', 'age', 'floors', 'source_file']
    new_cols = [col for col in cols if col not in list_var]
    if extra == 'compute_height' or (extra == 'solid' and var_map['height'] is None) or dataset_name == 'helsinki-gov':
        new_cols.remove('height')
    if dataset_name == 'spain-gov':
        new_cols.remove('floors')
    for col in new_cols:
        df[col] = ''

    # read source_file name
    df['source_file'] = os.path.split(file_path)[-1].split('.')[0]

    # create id if missing
    if 'id' not in list_var:
        df = df.reset_index(drop=True)
        #df['id'] = df.index
        df['id'] = range(id_counter, len(df) + id_counter)
        df['id'] = df['source_file'] + '_' + df['id'].apply(str)
        id_counter += len(df)

    if dataset_name == 'sardegna-gov':
        # add file identifier to id col to avoid id duplicates across files
        file_id = os.path.split(file_path)[1][5:8]
        df['id'] = df.id + '_' + file_id

    # reorder cols
    df = df[cols + ['geometry']]

    # harmonize missing values
    for col in df:
        df[col] = ['' if item in (None, np.nan) else item for item in np.array(df[col])]

    # separate geometries and attributes (both with id) separately; return id
    # counter for datasets with no id in raw data to create unique id
    return(df[['id', 'geometry']], df.drop(columns=['geometry']), id_counter)


# HIGH

def parse(path_to_param_file='/p/projects/eubucco/git-eubucco/database/preprocessing/1-parsing/inputs-parsing.csv',
          path_output='/p/projects/eubucco/data/1-intermediary-outputs',
          path_stats='/p/projects/eubucco/stats/1-parsing/',
          test_run=False,
          arg_as_param=None
          ):
    '''
    Wrapper function to parse an arbitraty number of files for a given open government
    building data source / OpenStreetMap: fetches a specific set of attributes and footprints in WKT.

    Currently supports the following formats: .gml, .xml, .shp, .dxf, .pbf, .pq

    The function creates a unique dataframe for the source that is split into two files
    for attributes and geometries that can be matched by id, and possibly a third file for
    non-standard but relevant attributes. A stat file is created to save main info about the run.

    This function needs to be executed with an argument parser to be linked to csv containing
    inputs parameters and where each for corresponds to a source, e.g.
    `python test-main-parsing.py -i 1` -> reads in row 1 of the input parameter csv.

    A number of workarounds are implemented for specific edge cases, either hard coded in the
    function for given dataset names when the situation occurs only for one dataset, or
    enabled as an option e.g. via the `extra` parameter in the input parameter csv.

    Returns: None.

    Saves to disk:  * geometries+ids in wkt as csv
                    * attributes+ids as csv
                    * (additional attributes+ids, if relevant as csv)
                    * stat file as csv
    '''
    start = time.time()
    max_ram_percent = psutil.virtual_memory().percent
    tot_count_multipoly = 0

    param = arg_as_param
    if arg_as_param is not None:
        p = get_params(param, path_to_param_file)

    else:
        p = get_params(param, json.load(sys.stdin))

    # import parameters
    print(p['country'])
    print(p['dataset_name'])
    print(p['extension'])
    # for testing
    print(p['var_map'])
    print("\n========================\n")

    # get list of file paths
    file_paths = get_file_paths(p['dataset_name'], p['path_input_folder'], p['extra'], p['extension'])
    if test_run:
        file_paths = file_paths[0:2]
        print(file_paths)

    # create empty df
    df_results_geom, df_results_attrib = pd.DataFrame(), pd.DataFrame()
    if p['extra_attrib'] is not None or (p['extra_attrib'] is None and p['extra'] ==
                                         'compute_height' and p['extension'] == 'gml'):
        df_results_extra_attrib = pd.DataFrame()

    # intitialise validation dict
    dict_val_result = {'invalid_geom_id': [], 'empty_geom_id': [], 'null_geom_id': []}

    list_faulty_files = []
    local_crs_files = []  # for spain
    id_counter = 0  # counter that is needed for datasets with no id and several raw files

    # loop through paths
    for n, file_path in enumerate(file_paths):

        print('{}/{}'.format(n + 1, len(file_paths)))
        print(file_path)

        if p['extension'] in ['shp', 'dxf', 'pbf', 'pq', 'parquet']:
            gdf, count_multipoly, dict_val = parse_tabular(file_path,
                                                           p['dataset_name'],
                                                           p['var_map']
                                                           )
        elif p['extension'] == 'csv.gz':
            gdf, count_multipoly, dict_val = parse_microsoft(file_path,
                                                           p['dataset_name'],
                                                           p['var_map']
                                                           )        
        
        elif p['extension'] in ['gml', 'xml']:
            if p['dataset_name'] == 'spain-gov':
                gdf, count_multipoly, dict_val, local_crs_file = parse_gml(file_path, p['dataset_name'], p['bldg_elem'],
                                                                           p['var_map'], p['local_crs'], p['extra'],
                                                                           p['extra_attrib'], len(file_paths))
                local_crs_files.append(local_crs_file)

            else:
                gdf, count_multipoly, dict_val = parse_gml(file_path, p['dataset_name'], p['bldg_elem'],
                                                           p['var_map'], p['local_crs'], p['extra'],
                                                           p['extra_attrib'], len(file_paths))                
        else:
            sys.exit('Parsing: The extension provided is unknown.')

        df_result_geom, df_result_attrib, id_counter = clean_attributes(gdf,
                                                                        p['extension'],
                                                                        p['dataset_name'],
                                                                        p['var_map'],
                                                                        # p['type_map'],
                                                                        file_path,
                                                                        p['extra'],
                                                                        id_counter
                                                                        )

        if p['extra_attrib'] is not None or (p['extra_attrib'] is None and p['extra']
                                             == 'compute_height' and p['extension'] == 'gml'):
            df_result_extra_attrib = get_extra_attribs(gdf,
                                                       p['extra_attrib'],
                                                       p['extension'],
                                                       p['var_map'],
                                                       p['extra'],
                                                       file_path,
                                                       df_result_attrib.id)

        # append part to main results/metrics
        df_results_geom = pd.concat([df_results_geom, df_result_geom], ignore_index=True)
        df_results_attrib = pd.concat([df_results_attrib, df_result_attrib], ignore_index=True)

        if p['extra_attrib'] is not None or (p['extra_attrib'] is None and p['extra']
                                             == 'compute_height' and p['extension'] == 'gml'):
            df_results_extra_attrib = pd.concat([df_results_extra_attrib, df_result_extra_attrib], ignore_index=True)

        # validation update: append to dict_val_result
        dict_val_result['invalid_geom_id'] += dict_val['invalid_geom_id']
        dict_val_result['empty_geom_id'] += dict_val['empty_geom_id']
        dict_val_result['null_geom_id'] += dict_val['null_geom_id']

        # stats update
        tot_count_multipoly += count_multipoly
        ram_percent = psutil.virtual_memory().percent
        print('RAM use: {}%'.format(ram_percent))
        if ram_percent > max_ram_percent:
            max_ram_percent = ram_percent
    print("\n========================\n")

    # full stats
    end = time.time() - start
    df_stats = get_stats(
        df_results_attrib,
        p['dataset_name'],
        file_paths,
        tot_count_multipoly,
        max_ram_percent,
        end,
        dict_val_result)
    print(f'Fault files: {list_faulty_files}')
    if p['dataset_name'] == 'spain-gov':
        print(f'CRS: {set(local_crs_files)}')

    # save
    df_stats.to_csv(os.path.join(path_stats, p['dataset_name'] + '_stat.csv'), index=False)
    print(df_stats.iloc[0])

    os.makedirs(os.path.join(path_output, p['country']),exist_ok=True)

    df_results_geom.to_csv(os.path.join(path_output, p['country'], p['dataset_name'] + '-3035_geoms.csv'), index=False)
    print('Geometries saved successfully.')

    df_results_attrib.to_csv(os.path.join(path_output, p['country'], p['dataset_name'] + '_attrib.csv'), index=False)
    print('Attributes saved successfully.')

    if p['extra_attrib'] is not None or (p['extra_attrib'] is None and p['extra'] ==
                                         'compute_height' and p['extension'] == 'gml'):
        df_results_extra_attrib.to_csv(
            os.path.join(
                path_output,
                p['country'],
                p['dataset_name'] +
                '_extra_attrib.csv'),
            index=False)
        print('Extra attributes saved successfully.')


def parse_osm_split(
        i,
        path_to_param_file='/p/projects/eubucco/git-eubucco/database/preprocessing/1-parsing/inputs-parsing.csv',
        path_output='/p/projects/eubucco/data/1-intermediary-outputs',
        path_stats='/p/projects/eubucco/stats/1-parsing/',
        test_run=False):
    '''
    Wrapper function to parse an large osm countries in parallel
    i = position in inputs-parsing.csv,
    args.i = pbf file per country
    '''
    start = time.time()
    max_ram_percent = psutil.virtual_memory().percent
    tot_count_multipoly = 0

    # import parameters
    p = get_params(i, path_to_param_file)
    print(p['country'])
    print(p['dataset_name'])
    print(p['extension'])
    # for testing
    print(p['var_map'])
    print("\n========================\n")

    # get list of file paths
    file_paths = get_file_paths(p['dataset_name'], p['path_input_folder'], p['extra'], p['extension'])
    if test_run:
        file_paths = file_paths[0:2]
        print(file_paths)

    # create empty df
    df_results_geom, df_results_attrib = pd.DataFrame(), pd.DataFrame()
    if p['extra_attrib'] is not None or (p['extra_attrib'] is None and p['extra'] ==
                                         'compute_height' and p['extension'] == 'gml'):
        df_results_extra_attrib = pd.DataFrame()

    # intitialise validation dict
    dict_val_result = {'invalid_geom_id': [], 'empty_geom_id': [], 'null_geom_id': []}

    list_faulty_files = []

    args = arg_parser(['i'])
    print(args.i)

    # loop through paths
    # for n,file_path in enumerate(file_paths):
    n = args.i
    file_path = file_paths[args.i]

    print('{}/{}'.format(n, len(file_paths) - 1))
    print(file_path)

    gdf, count_multipoly, dict_val = parse_tabular(file_path,
                                                   p['dataset_name'],
                                                   p['var_map']
                                                   )

    df_result_geom, df_result_attrib, id_counter = clean_attributes(gdf,
                                                                    p['extension'],
                                                                    p['dataset_name'],
                                                                    p['var_map'],
                                                                    # p['type_map'],
                                                                    file_path,
                                                                    p['extra'],
                                                                    0)  # 0 as we add the continous id counter in the concate func

    if p['extra_attrib'] is not None or (p['extra_attrib'] is None and p['extra'] ==
                                         'compute_height' and p['extension'] == 'gml'):
        df_result_extra_attrib = get_extra_attribs(gdf,
                                                   p['extra_attrib'],
                                                   p['extension'],
                                                   p['var_map'],
                                                   p['extra'],
                                                   file_path)

    # append part to main results/metrics
    df_results_geom = df_results_geom.append(df_result_geom)
    df_results_attrib = df_results_attrib.append(df_result_attrib)
    if p['extra_attrib'] is not None or (p['extra_attrib'] is None and p['extra'] ==
                                         'compute_height' and p['extension'] == 'gml'):
        df_results_extra_attrib = df_results_extra_attrib.append(df_result_extra_attrib)

    # validation update: append to dict_val_result
    dict_val_result['invalid_geom_id'] += dict_val['invalid_geom_id']
    dict_val_result['empty_geom_id'] += dict_val['empty_geom_id']
    dict_val_result['null_geom_id'] += dict_val['null_geom_id']

    # stats update
    tot_count_multipoly += count_multipoly
    ram_percent = psutil.virtual_memory().percent
    print('RAM use: {}%'.format(ram_percent))
    if ram_percent > max_ram_percent:
        max_ram_percent = ram_percent

    print("\n========================\n")

    # full stats
    end = time.time() - start
    df_stats = get_stats(
        df_results_attrib,
        p['dataset_name'],
        file_paths,
        tot_count_multipoly,
        max_ram_percent,
        end,
        dict_val_result)
    print(f'Fault files: {list_faulty_files}')

    df_results_geom.to_csv(
        os.path.join(
            path_output,
            p['country'],
            'osm',
            p['dataset_name'] +
            '_' +
            str(n) +
            '-3035_geoms.csv'),
        index=False)
    print('Geometries saved successfully.')

    df_results_attrib.to_csv(
        os.path.join(
            path_output,
            p['country'],
            'osm',
            p['dataset_name'] +
            '_' +
            str(n) +
            '_attrib.csv'),
        index=False)
    print('Attributes saved successfully.')

    if p['extra_attrib'] is not None or (p['extra_attrib'] is None and p['extra'] ==
                                         'compute_height' and p['extension'] == 'gml'):
        df_results_extra_attrib.to_csv(
            os.path.join(
                path_output,
                p['country'],
                'osm',
                p['dataset_name'] +
                '_' +
                str(n) +
                '_extra_attrib.csv'),
            index=False)
        print('Extra attributes saved successfully.')

    # save
    df_stats.to_csv(os.path.join(path_stats, 'osm', p['dataset_name'] + '_' + str(n) + '_stat.csv'), index=False)
    print(df_stats.iloc[0])
