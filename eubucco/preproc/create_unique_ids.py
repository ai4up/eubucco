import pandas as pd
import os
import glob

from ufo_map.Utils.helpers import *
from preproc.parsing import get_params

# declare global var
CRS_UNI = 'EPSG:3035'

# def func to assign unqiue ids


def assign_unqiue_ids(path, len_df, df_id_mapper, db_version):
    # get city name
    city = os.path.split(path)[-1]
    # get id code
    str_id_code = df_id_mapper.loc[df_id_mapper.city_name == city].id_marker.values[0]
    # create series with length df
    return ('v' + str(db_version) + '-' + str_id_code + '-' + pd.Series(range(len_df)).astype('string'))


def create_id(db_version=0.1,
              path_to_param_file='/p/projects/eubucco/git-eubucco/database/preprocessing/1-parsing/inputs-parsing.csv',
              path_old_db_folder='/p/projects/eubucco/data/2-database-city-level',
              path_new_db_folder='/p/projects/eubucco/data/2-database-city-level-v0_1',
              path_root_id='/p/projects/eubucco/data/0-raw-data/id_look_up/country-ids'):

    #import argparser
    args = arg_parser(['i'])
    print(args.i)
    # import parameters
    p = get_params(args.i, path_to_param_file)

    print('adding unqiue ids to: ')
    print(p['country'])
    print('----------')

    # read in id mapper
    df_id_mapper = pd.read_csv(os.path.join(path_root_id, p['country'] + '_ids.csv'))
    # read in all paths
    list_city_paths = get_all_paths(p['country'], path_root_folder=path_old_db_folder)

    i = 0
    # for path in list_city_paths:
    for path in list_city_paths:
        print('looping through path {} / {}'.format(i, len(list_city_paths) - 1))

        # get all parts of folder
        path_glob = path + '*'
        path_parts = glob.glob(path_glob)

        for ending in ['_geom', '_attrib', '_attrib_source', '_extra_attrib']:
            # get path
            paths_ending = [p for p in path_parts if ending in p]
            # check for attrib ending
            if ending == '_attrib':
                paths_ending = [k for k in paths_ending if 'extra' not in k]
                paths_ending = [k for k in paths_ending if 'source' not in k]

            # only change ids if one file per ending and if not 0
            if len(paths_ending) == 1:
                # open file and rename if col
                df = pd.read_csv(paths_ending[0])
                df = df.rename(columns={"id": "id_source"})

                # assign unqiue ids
                df['id'] = assign_unqiue_ids(path, len(df), df_id_mapper, db_version)

                # dropping geoms in attrib file
                if ending == '_attrib':
                    if 'geometry' in df.columns:
                        print('dropping geom col for {}'.format(ending))
                        df = df.drop(columns='geometry')

                # save df in new folder structure
                df.to_csv(paths_ending[0].replace(path_old_db_folder, path_new_db_folder), index=False)

        # rename ids in buffer to id_source
        paths_ending = [p for p in path_parts if '_buffer' in p]
        if len(paths_ending) == 1:
            # open file and rename if col
            df = pd.read_csv(paths_ending[0])
            df = df.rename(columns={"id": "id_source"})
            df.to_csv(paths_ending[0].replace(path_old_db_folder, path_new_db_folder), index=False)

        # increase counter
        i += 1

    print('created unqiue ids. closing run.')
