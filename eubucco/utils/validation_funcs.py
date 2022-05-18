import subprocess
import pandas as pd
import numpy as np
import os
import geopandas as gpd
from shapely import wkt

from ufo_map.Utils.helpers import *
from ufo_map.Preprocessing.parsing import *
from preproc.parsing import get_params


def get_num_rows(
        path_input_parsing='/p/projects/eubucco/git-eubucco/database/preprocessing/1-parsing/inputs-parsing.csv',
        path_root='/p/projects/eubucco/data/1-intermediary-outputs',
        path_out='/p/projects/eubucco/stats/1-parsing/validation'):
    """
    Function to count number of rows in 5 different csv file types to check if per dataset we have the same number
    of bldg in all files + we can use it to check where we might miss some files in our documentation
    """

    # go through each row in inputs-parsing.csv
    df = pd.read_csv(path_input_parsing)
    list_datasets = list(df['dataset_name'])

    df['num_geoms'] = 0
    df['num_attrib'] = 0
    df['num_x_attrib'] = 0
    df['num_sources'] = 0
    df['num_match'] = 0

    for index, row in df.iterrows():
        print(row['dataset_name'])
        path_geom = os.path.join(path_root, row['country'], row['dataset_name'] + '-3035_geoms.csv')
        path_attrib = os.path.join(path_root, row['country'], row['dataset_name'] + '_attrib.csv')
        path_x_attrib = os.path.join(path_root, row['country'], row['dataset_name'] + '_extra_attrib.csv')
        path_sources = os.path.join(path_root, row['country'], row['dataset_name'] + '_attrib_sources.csv')
        path_match = os.path.join(path_root, row['country'], row['dataset_name'] + '-matching_attrib.csv')

        # getting nums of rows for each file
        try:
            num_geoms = int(subprocess.check_output('wc -l ' + path_geom, shell=True).split()[0]) - 1
        except BaseException:
            num_geoms = 0

        try:
            num_attrib = int(subprocess.check_output('wc -l ' + path_attrib, shell=True).split()[0]) - 1
        except BaseException:
            num_attrib = 0

        try:
            num_x_attrib = int(subprocess.check_output('wc -l ' + path_x_attrib, shell=True).split()[0]) - 1
        except BaseException:
            num_x_attrib = 0

        try:
            num_sources = int(subprocess.check_output('wc -l ' + path_sources, shell=True).split()[0]) - 1
        except BaseException:
            num_sources = 0

        try:
            num_match = int(subprocess.check_output('wc -l ' + path_match, shell=True).split()[0]) - 1
        except BaseException:
            num_match = 0

        df.loc[index, 'num_geoms'] = num_geoms
        df.loc[index, 'num_attrib'] = num_attrib
        df.loc[index, 'num_x_attrib'] = num_x_attrib
        df.loc[index, 'num_sources'] = num_sources
        df.loc[index, 'num_match'] = num_match

        print('geom:{}, attrib:{},x_attrib:{},num_sources:{},num_match:{}'.format(
            num_geoms, num_attrib, num_x_attrib, num_sources, num_match))
        print('....')

    df_out = df[['country', 'dataset_name', 'num_geoms', 'num_attrib', 'num_x_attrib', 'num_sources', 'num_match']]
    df_out.to_csv(os.path.join(path_out, 'overview-bldg-num-across-files.csv'))
    print('saved stats file.closing run.')


def geom_validation(
        path_input_parsing='/p/projects/eubucco/git-eubucco/database/preprocessing/1-parsing/inputs-parsing.csv',
        path_root='/p/projects/eubucco/data/1-intermediary-outputs',
        path_out='/p/projects/eubucco/stats/1-parsing/validation/'):
    """
    Function to check geoms and report invalid geoms
    """

    # go through each row in inputs-parsing.csv
    df = pd.read_csv(path_input_parsing)
    list_datasets = list(df['dataset_name'])

    for index, row in df.iterrows():
        print(row['dataset_name'])
        path_geom = os.path.join(path_root, row['country'], row['dataset_name'] + '-3035_geoms.csv')

        try:
            chunks = pd.read_csv(path_geom, chunksize=int(3E5))
        except BaseException:
            chunks = ''

        if chunks:
            for idx, chunk in enumerate(chunks):
                gdf = gpd.GeoDataFrame(chunk, geometry=chunk['geometry'].apply(wkt.loads), crs='EPSG:3035')
                if not chunk.empty:
                    break
            # check for duplicated ids
            num_gen_dupl = len(gdf.loc[gdf.duplicated()])
            num_id_dupl = len(gdf.loc[gdf.duplicated(subset='id')])

            # analyse if geometries are valid
            print(gdf.is_valid.value_counts())
            gdf['bool'] = gdf.is_valid
            num_true = gdf['bool'].values.sum()
            num_false = (~gdf['bool']).values.sum()
            print(gdf.geometry.head(3))

        else:
            num_true = np.nan
            num_false = np.nan
            num_gen_dupl = np.nan
            num_id_dupl = np.nan

        print('-------------')

        df.loc[index, 'num_valid'] = num_true
        df.loc[index, 'num_invalid'] = num_false
        df.loc[index, 'num_gen_dupl'] = num_gen_dupl
        df.loc[index, 'num_id_dupl'] = num_id_dupl

    df_out = df[['country', 'dataset_name', 'num_valid', 'num_invalid', 'num_gen_dupl', 'num_id_dupl']]
    df_out.to_csv(os.path.join(path_out, 'overview-bldg-geoms.csv'))
    print('saved stats file.closing run.')


def get_num_rows_city(
        path_input_parsing='/p/projects/eubucco/git-eubucco/database/preprocessing/1-parsing/inputs-parsing.csv',
        path_out='/p/projects/eubucco/stats/1-parsing/validation'):
    """
    Function to go though all files of a country and count num of bldgs per city in all present files.

    """
    args = arg_parser(['i'])
    print(args.i)
    # import parameters
    p = get_params(args.i, path_input_parsing)

    print(p['country'])

    # read in city names and create df
    list_city_paths = get_all_paths(p['country'])
    list_city_names = [os.path.split(city_path)[-1] for city_path in list_city_paths]
    df = pd.Series(list_city_paths, name='paths').to_frame()
    df['city_name'] = list_city_names

    df['num_geoms'] = 0
    df['num_attrib'] = 0
    df['num_x_attrib'] = 0
    df['num_sources'] = 0
    df['num_match'] = 0

    for city, city_path in zip(list_city_names, list_city_paths):
        print(city)
        path_geom = city_path + '_geom.csv'
        path_attrib = city_path + '_attrib.csv'
        path_x_attrib = city_path + '_extra_attrib.csv'
        path_sources = city_path + '_attrib_sources.csv'
        path_match = city_path + '-matching_attrib.csv'

        # getting nums of rows for each file
        try:
            num_geoms = int(subprocess.check_output('wc -l ' + path_geom, shell=True).split()[0]) - 1
        except BaseException:
            num_geoms = 0

        try:
            num_attrib = int(subprocess.check_output('wc -l ' + path_attrib, shell=True).split()[0]) - 1
        except BaseException:
            num_attrib = 0

        try:
            num_x_attrib = int(subprocess.check_output('wc -l ' + path_x_attrib, shell=True).split()[0]) - 1
        except BaseException:
            num_x_attrib = 0

        try:
            num_sources = int(subprocess.check_output('wc -l ' + path_sources, shell=True).split()[0]) - 1
        except BaseException:
            num_sources = 0

        try:
            num_match = int(subprocess.check_output('wc -l ' + path_match, shell=True).split()[0]) - 1
        except BaseException:
            num_match = 0

        df.loc[df.city_name == city, 'num_geoms'] = num_geoms
        df.loc[df.city_name == city, 'num_attrib'] = num_attrib
        df.loc[df.city_name == city, 'num_x_attrib'] = num_x_attrib
        df.loc[df.city_name == city, 'num_sources'] = num_sources
        df.loc[df.city_name == city, 'num_match'] = num_match

        if num_geoms > 0:
            print('geom:{}, attrib:{},x_attrib:{},num_sources:{},num_match:{}'.format(
                num_geoms, num_attrib, num_x_attrib, num_sources, num_match))
        print('....')

    # save
    df_out = df[['city_name', 'num_geoms', 'num_attrib', 'num_x_attrib', 'num_sources', 'num_match']]
    df_out.to_csv(os.path.join(path_out, p['country'] + '-num-across-cities_3e4.csv'))
    print('saved stats file.closing run.')
