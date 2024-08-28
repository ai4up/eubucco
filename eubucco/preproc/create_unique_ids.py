import pandas as pd
import os
import glob

from ufo_map.Utils.helpers import *

def assign_unqiue_ids(path, len_df, df_id_mapper, db_version):
    city = os.path.split(path)[-1]
    city_id_code = df_id_mapper.loc[df_id_mapper.city_name == city].id_marker.values[0]
    return ('v' + str(db_version) + '-' + city_id_code + '-' + pd.Series(range(len_df)).astype('string'))

def save_df(df_tmp,path):
    if '/' in path:
        outname = path.rsplit('/',1)[1]
        outdir = path.rsplit('/',1)[0]
        if not os.path.exists(outdir):
            os.makedirs(outdir)
        fullname = os.path.join(outdir, outname)    
    else: fullname=path

    df_tmp.to_csv(fullname, index=False)


def test_ids(len_df, ids_df,source_ids_df,dupls_source_id_df,dupls_id_df):
    if not len(set(len_df)) == 1: raise ValueError('Error! Files do not have same length')
    if any(dupls_id_df): raise ValueError('Error! Dupl ids found')
    if any(dupls_source_id_df): raise ValueError('Error! Dupl source_ids found')
    if not all([set(el)==set(ids_df[0]) for el in ids_df]): raise ValueError('Error! Ids do not match across files')
    if not all([set(el)==set(source_ids_df[0]) for el in source_ids_df]): raise ValueError('Error! Source_ids do not match across files')
    return True


def create_id(country,
              db_version=0.1,
              path_old_db_folder='/p/projects/eubucco/data/2-database-city-level',
              path_new_db_folder='/p/projects/eubucco/data/2-database-city-level-v0_1',
              path_root_id='/p/projects/eubucco/data/0-raw-data/id_look_up/country-ids'):

    df_id_mapper = pd.read_csv(os.path.join(path_root_id, country + '_ids.csv'))
    city_paths = get_all_paths(country, path_root_folder=path_old_db_folder)

    for i, path in enumerate(city_paths):
        print('looping through path {} / {}'.format(i, len(city_paths) - 1))
        try:
            # define new, globally unique id for all buildings in _attrib.csv
            file_path = path + '_attrib.csv'
            df = pd.read_csv(file_path)
            df = df.drop_duplicates(subset='id').reset_index(drop=True) # drop all left over duplicates
            df.rename(columns={'id': 'id_source'}, inplace=True)
            df.sort_values(by='id_source', inplace=True)
            df['id'] = assign_unqiue_ids(path, len(df), df_id_mapper, db_version)
            id_source_id_mapping = dict(zip(df['id_source'], df['id']))

            if 'geometry' in df.columns:
                df = df.drop(columns='geometry')

            save_df(df, file_path.replace(path_old_db_folder, path_new_db_folder))

            # intialize variables for checking that there are no id errors across files
            len_df = [len(df)]
            ids_df = [df.id.tolist()]
            source_ids_df = [df.id_source.tolist()]
            dupls_id_df = [df.duplicated(subset='id').any()]
            dupls_source_id_df = [df.duplicated(subset='id_source').any()]

            # update other files to use the new id as well
            for ending in ['_geom', '_buffer', '_attrib_source', '_extra_attrib']:
                file_path = path + ending + '.csv'

                if not os.path.isfile(file_path):
                    print(f'Warning: file {file_path} does not exist')
                    continue
                
                df = pd.read_csv(file_path) 
                df = df.drop_duplicates(subset='id').reset_index(drop=True) # drop all left over duplicates
                df.rename(columns={'id': 'id_source'}, inplace=True)
                
                if not 'buffer' in file_path:
                    df['id'] = df['id_source'].map(id_source_id_mapping)
                
                if ending in ['_geom','_attrib_source','_extra_attrib']: 
                    len_df.append(len(df))
                    ids_df.append(df.id.tolist())
                    source_ids_df.append(df.id_source.tolist())
                    dupls_id_df.append(df.duplicated(subset='id').any())
                    dupls_source_id_df.append(df.duplicated(subset='id_source').any())
                    
                    save_df(df, file_path.replace(path_old_db_folder, path_new_db_folder))
        except:
            print(f'Missing files for {path}')
        test_ids(len_df, ids_df, source_ids_df,dupls_source_id_df,dupls_id_df)

    print('created unqiue ids. closing run.')
