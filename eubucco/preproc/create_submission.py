import os
import glob
import shutil

import pandas as pd

from ufo_map.Utils.helpers import *


CRS_UNI = 'EPSG:3035'


def get_city_size(df, all_paths):
    """
    function to get file sizes for the chosen files for all cities in a country
    """
    for path in all_paths:
        # reset counters
        tot_size = 0

        # get paths of folder
        path_glob = path + '*'
        path_parts = glob.glob(path_glob)

        # loop through relevant endings
        for ending in ['geom.csv', '_attrib.csv', 'boundary.csv']:
            paths_ending = [p for p in path_parts if ending in p]

            if ending == '_attrib.csv':
                paths_ending = [k for k in paths_ending if 'extra' not in k]
                paths_ending = [k for k in paths_ending if 'source' not in k]

            if len(paths_ending) == 1:
                # sum up file sizes for city
                tot_size += os.stat(paths_ending[0]).st_size

        # assign to df
        df.loc[df.city_name == os.path.split(path)[-1], 'file_size'] = tot_size

    return df


def get_region_parts(df, file_lim):
    """function to determine parts of region based on file lim"""
    df['part_marker'] = 0
    for reg_id in set(df.id_marker_region):
        tot_siz = 0
        p = 0
        df_temp = df.loc[df.id_marker_region == reg_id]
        for idx, siz in zip(list(df_temp.index), df_temp.file_size):
            tot_siz += siz
            if tot_siz < file_lim:
                df_temp.loc[idx, 'part_marker'] = 'p' + str(p)
            else:
                p += 1
                print('In {} determining part {}'.format(reg_id, p))
                df_temp.loc[idx, 'part_marker'] = 'p' + str(p)
                tot_siz = 0
        df.loc[df.id_marker_region == reg_id, 'part_marker'] = df_temp.part_marker
    print('------')
    print('all parts for regions determined')
    return df


def merge_city_files(path):
    """function to merge attrib and geom file per city on id"""
    # read in files
    bool_empty = False
    try:
        df_attrib = pd.read_csv(os.path.join(path + '_attrib.csv'))
        df_geom = import_csv_w_wkt_to_gdf(os.path.join(path + '_geom.csv'), CRS_UNI)
    except BaseException:
        bool_empty = True

    if not bool_empty:
        # take only relevant columns of attrib file
        df_attrib = df_attrib[['id', 'height', 'age', 'type', 'type_source', 'floors', 'source_file']]

        # merge on id
        df_temp = df_geom.merge(df_attrib, on='id')
        df_temp = df_temp[['id', 'height', 'age', 'type', 'id_source', 'type_source', 'geometry']]
    else:
        df_temp = pd.DataFrame()
        print('!!! empty city!!!')
    return df_temp, bool_empty


def concate_country(df, all_paths, db_version, out_folder):
    """ function to concate all cities per country and save as one file
    """
    df_out = pd.DataFrame()
    i = 0
    for path in all_paths:
        print('concate whole country {}/{}'.format(i, len(all_paths) - 1))
        # merge city files
        df_temp, bool_empty = merge_city_files(path)
        # add to output
        if not bool_empty:
            df_out = pd.concat([df_out, df_temp])
        # increase counter
        i += 1

    # create out path and remove dots
    file_name = 'v' + str(db_version) + '-' + df['id_marker_country'][0]
    to_gpkg_zip(df_out, file_name, out_folder)
    to_csv_zip(df_out[['id', 'height', 'age', 'type', 'id_source', 'type_source']], file_name, out_folder)


    print('Num bldgs saved: {}'.format(len(df_out)))


def concate_regions(df_reg_temp, all_paths, db_version, out_folder):
    """function to concate all cities per region and save per region"""

    # split all paths into elems to filter cities
    path_split = [(path, os.path.normpath(path).split(os.path.sep)) for path in all_paths]

    # get a list of all regions that are below the file_lim
    lst_reg_id = set(df_reg_temp.id_marker_region)

    print('saving files for {} regions'.format(len(lst_reg_id)))

    # intialise
    n_bldgs = 0
    # loop through each region
    for reg_id in lst_reg_id:
        # get list of all cities in the region
        lst_city_per_reg = list(df_reg_temp.loc[df_reg_temp.id_marker_region == reg_id].city_name)
        # get all paths per region
        paths_reg = [path[0] for path in path_split if path[1][-1] in lst_city_per_reg]
        # intialise
        df_out = pd.DataFrame()
        i = 0
        # loop through each city
        for path in paths_reg:
            print('concate region {} {}/{}'.format(reg_id, i, len(paths_reg) - 1))
            # merge city files
            df_temp, bool_empty = merge_city_files(path)
            # add to output
            if not bool_empty:
                df_out = pd.concat([df_out, df_temp])
            # increase counter
            i += 1
        
        print('saving files for region {}'.format(reg_id))
        file_name = 'v' + str(db_version) + '-' + str(reg_id)
        to_gpkg_zip(df_out, file_name, out_folder)
        to_csv_zip(df_out[['id', 'height', 'age', 'type', 'id_source', 'type_source']], file_name, out_folder)

        n_bldgs += len(df_out)

    print('-----')
    print('all region files saved successfully.')
    print('Num bldgs saved: {}'.format(n_bldgs))


def concate_cities(df_reg_temp, all_paths, db_version, out_folder):
    """function to concate all cities per region and save per region"""

    # split all paths into elems to filter cities
    path_split = [(path, os.path.normpath(path).split(os.path.sep)) for path in all_paths]

    # get a list of all regions that are below the file_lim
    lst_reg_id = set(df_reg_temp.id_marker_region)

    print('saving files for {} regions'.format(len(lst_reg_id)))

    # intialise
    n_bldgs = 0
    # loop through each region
    for reg_id in lst_reg_id:
        # get the parts per region
        lst_reg_part = set(df_reg_temp.loc[df_reg_temp.id_marker_region == reg_id].part_marker)
        df_reg_parts = df_reg_temp.loc[df_reg_temp.id_marker_region == reg_id]
        # intialise
        j = 0
        for part in lst_reg_part:
            print('Conactenating for region {} part {}/{}'.format(reg_id, j, len(lst_reg_part) - 1))
            # get list of all cities in the region per part
            lst_city_per_reg = list(df_reg_parts.loc[df_reg_temp.part_marker == part].city_name)
            # get all paths per region
            paths_reg = [path[0] for path in path_split if path[1][-1] in lst_city_per_reg]
            # intialise
            df_out = pd.DataFrame()
            # concate all cities for part
            for path in paths_reg:
                print(path.rsplit('/', 1)[1])
                # merge city files
                df_temp, bool_empty = merge_city_files(path)
                # add to output
                if not bool_empty:
                    df_out = pd.concat([df_out, df_temp])

            # save geopackage and csv zip for region
            file_name = 'v' + str(db_version) + '-' + str(reg_id) + '-' + part
            to_gpkg_zip(df_out, file_name, out_folder)
            to_csv_zip(df_out[['id', 'height', 'age', 'type', 'id_source', 'type_source']], file_name, out_folder)

            # count num bldgs
            n_bldgs += len(df_out)
            j += 1

        # print
        print('saved all files for region {}'.format(reg_id))
        # counter for parts

    print('-----')
    print('all part-region files saved successfully.')
    print('Num bldgs saved: {}'.format(n_bldgs))


def to_csv_zip(df, file_name, directory):
    file_name = file_name.replace('.', '_') + '.csv.zip'
    file_path = os.path.join(directory, file_name)
    df.to_csv(file_path, index=False, compression='zip')


def to_gpkg_zip(df, file_name, directory):
    file_name = file_name.replace('.', '_') + '.gpkg'
    file_path = os.path.join(directory, file_name)
    df.to_file(file_path, driver='GPKG', index=False)
    shutil.make_archive(file_path, 'zip', directory, file_name)
    os.remove(file_path)


def concate_release(
    country,
    db_version=0.1,
    file_lim=2,
    path_db_folder='/p/projects/eubucco/data/2-database-city-level-v0_1',
    path_root_id='/p/projects/eubucco/data/0-raw-data/id_look_up/country-ids',
    out_folder='/p/projects/eubucco/data/5-v0_1'):

    # convert to byte
    file_lim = file_lim * 1e9

    print(country)

    # get all paths
    all_paths = get_all_paths(country, path_root_folder=path_db_folder)

    # read in ids
    df_ids = pd.read_csv(os.path.join(path_root_id, country + '_ids.csv'))

    # get file sizes for all cities
    print('determining city sizes')
    df = get_city_size(df_ids, all_paths)

    path_file_size = os.path.join(out_folder, 'city_sizes')
    if not os.path.exists(path_file_size):    
            os.makedirs(path_file_size)
    df.to_csv(os.path.join(path_file_size, country + '_file_sizes.csv'), index=False)

    # add region marker
    df['id_marker_country'] = df.id_marker.str.rsplit('.', 0).apply(lambda x: x[0])
    df['id_marker_region'] = df.id_marker.str.rsplit('.').apply(lambda x: x[0] + '.' + x[1])

    # if country file size smaller file lim -> concate country
    if df.file_size.sum() < file_lim:
        print('concatenating on country lvl as sum of files equals {} GB'.format(round(df.file_size.sum() * 1e-9, 3)))
        concate_country(df, all_paths, db_version, out_folder)
    else:
        print('concatenating below country lvl as sum of files equals {} GB'.format(round(df.file_size.sum() * 1e-9, 3)))
        # check which regions are smaller than file_lim
        df_regions = df.groupby(['region_name', 'id_marker_region']).sum().reset_index()
        # create lst of ids for regions that are smaller and larger than file_lim
        lst_in_ids = list(df_regions.loc[df_regions.file_size <= file_lim].id_marker_region)
        lst_out_ids = list(df_regions.loc[df_regions.file_size > file_lim].id_marker_region)
        # concate regions that are smaller than file lim
        concate_regions(df.loc[df.id_marker_region.isin(lst_in_ids)], all_paths, db_version, out_folder)

        if len(lst_out_ids) > 0:
            # calculate citiy parts and save them
            df = get_region_parts(df, file_lim)
            # save
            df[['id_marker', 'country_name', 'region_name', 'city_name', 'part_marker']].to_csv(
                os.path.join(out_folder, 'id_codes', country + '_id_codes.csv'), index=False)
            # concate remaining cities
            concate_cities(df.loc[df.id_marker_region.isin(lst_out_ids)], all_paths, db_version, out_folder)

    print('All files saved. Closing run.')
