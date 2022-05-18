import pandas as pd
import multiprocessing
import time
import os

from preproc.parsing import get_params
from ufo_map.Utils.helpers import *
from ufo_map.Preprocessing.parsing import *


def reproj_to_3035_in_db(country_names=['france', 'netherlands'],
                         local_crs={'france': 2154, 'netherlands': 28992}):
    '''
        Reprojects geom files to Europe-wide crs directly in the db folders.
    '''

    failed = []

    for country_name in country_names:

        print('\n=====================\n')
        print(country_name)
        print('\n=====================\n')

        for path in get_all_paths(country_name, 'geom') + get_all_paths(country_name, 'buffer'):

            print(path)

            try:
                save_csv_wkt(import_csv_w_wkt_to_gdf(path, local_crs[country_name]).to_crs(3035), path)

            except BaseException:
                print('FAILED')
                failed = failed.append(path)

        print(failed)
        textfile = open("failed_crs.txt", "w")
        for element in failed:
            textfile.write(element + "\n")
        textfile.close()


def hang(gdf_0_row, crs_uni, return_dict):
    print('entering test conversion process')
    return_dict[1] = gdf_0_row.to_crs(crs_uni).geometry


def reproject(
        crs_uni='EPSG:3035',
        path_root='/p/projects/eubucco/data/1-intermediary-outputs/',
        path_to_param_file='/p/projects/eubucco/git-eubucco/database/preprocessing/1-parsing/inputs-parsing.csv',
        path_to_stats_file='/p/projects/eubucco/git-eubucco/database/preprocessing/1-parsing/stats/reproj/failed_reproj.csv'):

    # get argparser
    args = arg_parser(['i'])
    print(args.i)
    # import parameters
    p = get_params(args.i, path_to_param_file)
    print(p['dataset_name'])
    print(p['country'])
    print('----------')

    ## read in data
    gdf = import_csv_w_wkt_to_gdf(
        os.path.join(
            path_root,
            p['country'],
            p['dataset_name'] +
            '_geom.csv'),
        p['local_crs'])
    gdf_0_row = gdf.iloc[[0]]

    print('loaded geom data')
    # start multiprocess
    # intialise manager to get return value from subprocess
    if 'gov' in p['dataset_name']:
        manager = multiprocessing.Manager()
        return_dict = manager.dict()

        m = multiprocessing.Process(target=hang, args=(gdf_0_row, crs_uni, return_dict))
        m.start()
        # we give the crs conversion of one geom max 15 secs
        time.sleep(60)
        m.terminate()
        m.join()
        print('test conversion process exiting...')

    #####
    # in osm case set return_dict = True and convert
    else:
        return_dict = {1}

    if return_dict:
        print('converting the whole damn thing')
        print('--> --> --> ---> -->')
        gdf = gdf.to_crs(crs_uni)
        print('conversion successfull!')
        # saving an image of 10% of all buildings of this file

        # saving to 1-intermediary-outputs
        gdf.to_csv(os.path.join(path_root, p['country'], p['dataset_name'] + '-3035_geoms.csv'))
        print('saved reproj geom successfully')
    else:
        print('conversion did not work, we need to run this file locally')
        df_stats = pd.read_csv(path_to_stats_file)
        save_dict = dict()
        save_dict['country'] = p['country']
        save_dict['dataset_name'] = p['dataset_name']
        save_dict['local_crs'] = p['local_crs']
        df_stats = df_stats.append(save_dict, ignore_index=True)
        df_stats.to_csv(path_to_stats_file)
        print('saved to failed_repoj.csv successfully')


def reproject_osm(
        crs_uni='EPSG:3035',
        path_root='/p/projects/eubucco/data/1-intermediary-outputs/',
        path_to_param_file='/p/projects/eubucco/git-eubucco/database/preprocessing/1-parsing/inputs-parsing.csv',
        path_to_stats_file='/p/projects/eubucco/git-eubucco/database/preprocessing/1-parsing/stats/reproj/failed_reproj.csv'):

    # get argparser
    args = arg_parser(['i'])
    print(args.i)
    # import parameters
    p = get_params(args.i, path_to_param_file)
    print(p['dataset_name'])
    print(p['country'])
    print('----------')

    ## read in data
    gdf = import_csv_w_wkt_to_gdf(
        os.path.join(
            path_root,
            p['country'],
            p['dataset_name'] +
            '_geom.csv'),
        p['local_crs'])

    print('loaded geom data')
    # start multiprocess

    print('converting the whole damn thing')
    print('--> --> --> ---> -->')
    gdf = gdf.to_crs(crs_uni)
    print(gdf.crs)
    print('conversion successfull!')
    # saving an image of 10% of all buildings of this file

    # saving to 1-intermediary-outputs
    gdf.to_csv(os.path.join(path_root, p['country'], p['dataset_name'] + '-3035_geoms.csv'))
    print('saved reproj geom successfully')


def reproject_osm_split(
        crs_uni='EPSG:3035',
        path_root='/p/projects/eubucco/data/1-intermediary-outputs/',
        path_to_param_file='/p/projects/eubucco/git-eubucco/database/preprocessing/1-parsing/inputs-parsing.csv'):

    # get argparser
    args = arg_parser(['i'])
    print(args.i)
    # import parameters
    p = get_params(args.i, path_to_param_file)
    print(p['dataset_name'])
    print(p['country'])
    print('----------')

    # define dict with number of files
    dict_file_num = {'france': 27, 'germany': 33, 'italy': 5, 'poland': 18, 'netherlands': 15}

    # in germany we only take osm files where we don't have gov data!
    if p['country'] == 'germany':
        num_files = [1, 3, 8, 15, 16, 20, 22]
    else:
        num_files = range(dict_file_num[p['country']])

    for k in num_files:
        ## read in data
        try:
            gdf = import_csv_w_wkt_to_gdf(
                os.path.join(
                    path_root,
                    p['country'],
                    'osm',
                    p['country'] +
                    '-osm_' +
                    str(k) +
                    '_geom.csv'),
                p['local_crs'])

            print('loaded geom data: {}'.format(k))
            # start multiprocess

            print('converting the whole damn thing')
            print('--> --> --> ---> -->')
            gdf = gdf.to_crs(crs_uni)
            print(gdf.crs)
            print('conversion successfull!')
            # saving an image of 10% of all buildings of this file

            # saving to 1-intermediary-outputs
            gdf.to_csv(
                os.path.join(
                    path_root,
                    p['country'],
                    'osm',
                    p['country'] +
                    '-osm_' +
                    str(k) +
                    '3035_geoms.csv'),
                index=False)
            print('saved {}/{} reproj geom successfully'.format(k, num_files))
        except BaseException:
            print('skipped {}/{} geom '.format(k, len(num_files)))
