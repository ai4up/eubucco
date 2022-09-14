import pandas as pd
import os
import glob

from ufo_map.Utils.helpers import *
from preproc.parsing import get_params

CRS_UNI = 'EPSG:3035'


def assign_unqiue_ids(path, len_df, df_id_mapper, db_version):
    city = os.path.split(path)[-1]
    city_id_code = df_id_mapper.loc[df_id_mapper.city_name == city].id_marker.values[0]
    return ('v' + str(db_version) + '-' + city_id_code + '-' + pd.Series(range(len_df)).astype('string'))

def save_df(df_tmp,path):
    if '/' in path:
        outname = path.rsplit('/',1)[1]
        outdir = path.rsplit('/',1)[0]
        if not os.path.exists(outdir):
            os.mkdir(outdir)
        fullname = os.path.join(outdir, outname)    
    else: fullname=path

    df_tmp.to_csv(fullname, index=False)

def create_id(country,
              db_version=0.1,
              path_old_db_folder='/p/projects/eubucco/data/2-database-city-level',
              path_new_db_folder='/p/projects/eubucco/data/2-database-city-level-v0_1',
              path_root_id='/p/projects/eubucco/data/0-raw-data/id_look_up/country-ids'):

    df_id_mapper = pd.read_csv(os.path.join(path_root_id, country + '_ids.csv'))
    city_paths = get_all_paths(country, path_root_folder=path_old_db_folder)

    for i, path in enumerate(city_paths):
        print('looping through path {} / {}'.format(i, len(city_paths) - 1))

        # define new, globally unique id for all buildings in _attrib.csv
        file_path = path + '_attrib.csv'
        df = pd.read_csv(file_path)
        df = df.drop_duplicates(subset='id') # drop all left over duplicates
        df.rename(columns={'id': 'id_source'}, inplace=True)
        df.sort_values(by='id_source', inplace=True)
        df['id'] = assign_unqiue_ids(file_path, len(df), df_id_mapper, db_version)
        id_source_id_mapping = dict(zip(df['id_source'], df['id']))

        if 'geometry' in df.columns:
            df = df.drop(columns='geometry')

        #df.to_csv(file_path.replace(path_old_db_folder, path_new_db_folder), index=False)
        save_df(df, file_path.replace(path_old_db_folder, path_new_db_folder))

        # update other files to use the new id as well
        for ending in ['_geom', '_buffer', '_attrib_source', '_extra_attrib']:
            file_path = path + ending + '.csv'

            if not os.path.isfile(file_path):
                print(f'Warning: file {file_path} does not exist')
                continue

            df = pd.read_csv(file_path)
            df.rename(columns={'id': 'id_source'}, inplace=True)

            if not 'buffer' in file_path:
                df['id'] = df['id_source'].map(id_source_id_mapping)

            #df.to_csv(file_path.replace(path_old_db_folder, path_new_db_folder), index=False)
            save_df(df, file_path.replace(path_old_db_folder, path_new_db_folder))

    print('created unqiue ids. closing run.')
