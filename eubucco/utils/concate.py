import os
import pandas as pd
import psutil
import ast

from ufo_map.Utils.helpers import get_all_paths, arg_parser, import_csv_w_wkt_to_gdf
from preproc.db_set_up import fetch_GADM_info_country
from preproc.parsing import get_params


def validity_merge_db(df_geom,df_attrib):
    '''
        Function to check that attributes+ids and geometries+ids dataframe that have 
        been split can be reassembled. 
    '''
    if len(df_geom)!=len(df_attrib): 
      raise 'Error: lengths do not match'

    if len(df_geom.loc[~df_geom.id.isin(df_attrib.id)])>0: 
      raise 'Error: ids do not match'

    if any(df_geom.duplicated(subset=['id'])): 
      raise 'Error: there are duplicated ids' 

    return True


def compare_overviews(left_col,s,df_overview):
    '''
        Compare the city-level overview stats computed before and after chunking.

        In:
            * left_col: name of the column to test for in the chunk df
            * s: series reproducing the stat at the city level
            * df_overview: dataframe with the previously computed stats
    '''
    df_compare = pd.merge(df_overview[['city',left_col]],
         s,
         left_on='city',
         right_index=True
        )

    if any(df_compare[left_col] != df_compare['right_col']):
        print(f' ({left_col}) Error: Values for the {len(df_compare[left_col] != df_compare.right_col)} / {len(df_compare)} cities are not identical')

    else: print(f'({left_col}) All values are identical') 

    

def concat_city_level_fts(country_names):
    '''
    '''
    for country_name in country_names:
        df = pd.DataFrame()
        for path in get_all_paths(country_name, 'city_level_fts'):
            try:
                d = pd.read_csv(path)
                d['city_name'] = os.path.split(path)[-1][:-19]
                df = pd.concat([df, d], axis=0)
            except BaseException:
                print(f'error with {path}')
        print(df.head())
        df.to_csv(
            os.path.join(
                '/p/projects/eubucco/data/3-ml-inputs',
                country_name +
                '_city_level_fts.csv'),
            index=False)
    print('Done')


def concatenate_all_fts(gadm_country_code,
                        city_name=None,
                        city_list=None,
                        path_storage_folder='/p/projects/eubucco/data/3-ml-inputs'):
    '''
        Concatenatess all files into one large csv.

        Args:
        gadm_country_code:
        city_name:
        city_list:
        path_storage_folder:

        Returns:
        none

    '''
    # intiailise ram count
    max_ram_percent = psutil.virtual_memory().percent

    # get country and city names from GADM
    _, country_name, _, local_crs = fetch_GADM_info_country(gadm_country_code)

    # get all paths
    city_paths = get_all_paths(country_name)

    # if city_name is given take only path of city
    if city_name is not None:
        index = [elem.rsplit('/')[-1] for elem in city_paths].index(city_name)
        city_paths = [city_paths[index]]

    # if city_list is given, take only cities of indices
    if city_list is not None:
        start = city_list[0]
        end = city_list[1]
        city_paths = city_paths[start:end]

    # get all paths
    # fts_names = ['bld_fts','bld_d_fts','block_fts','block_d_fts','int_fts',
    #         'str_fts','sbb_fts','city_level_fts']
    fts_names = ['wsf-evo_age', 'lu_fts']

    # intialise output df
    df_out = pd.DataFrame()

    # loop through all paths:
    for i in range(len(city_paths)):

        print('-----')
        print('{} of {}'.format(i, len(city_paths)))
        print('adding ', city_paths[i].rsplit('/')[-1])

        # catch if fts files not found and skip city
        try:
            # merge to df_city and afterwards delete to free up RAM
            df_ft0 = pd.read_csv(get_all_paths(country_name, fts_names[0])[i])
            df_ft = pd.read_csv(get_all_paths(country_name, fts_names[1])[i])
            df_city = pd.merge(df_ft0, df_ft, on='id', how='left')
            del df_ft0, df_ft

            # get all endings except from the first 2 and the last one (which is the city level)

            # COMMENTED OUT

            # for file in fts_names[2:-1]:
            #     path_ft = get_all_paths(country_name,file)[i]
            #     df_ft = pd.read_csv(path_ft)
            #     df_city = pd.merge(df_city,df_ft, on='id',how='left')
            #     del df_ft

            # # add city level features to all rows and afterwards del df_ft
            # df_ft = pd.read_csv(get_all_paths(country_name,fts_names[-1])[i])
            # for col in df_ft.columns:
            #     df_city[col]=df_ft[col].values[0]
            # del df_ft

            # # add city names as col
            # df_city['city'] = city_paths[i].rsplit('/')[-1]

            # COMMENTED OUT

            # add to output df
            df_out = pd.concat([df_out, df_city])
            del df_city

        except FileNotFoundError:
            print('No fts found for ', city_paths[i].rsplit('/')[-1])

        ram_percent = psutil.virtual_memory().percent
        print('RAM use: {}%'.format(ram_percent))

    # reset index
    df_out = df_out.reset_index(drop=True)

    # define path_out
    if city_name is not None:
        path_out = os.path.join(path_storage_folder, country_name + '_' + city_name + "_fts.csv")
    elif city_list is not None:
        path_out = os.path.join(
            path_storage_folder,
            country_name +
            '_cities_' +
            str(start) +
            '_' +
            str(end) +
            "_fts.csv")
    else:
        path_out = os.path.join(path_storage_folder, country_name + "_fts.csv")

    # save as csv
    df_out.to_csv(path_out, index=False)

    # final print
    print('----')
    print('successfully merged all feature files')


def concatenate_osm(
        path_to_param_file='/p/projects/eubucco/git-eubucco/database/preprocessing/1-parsing/inputs-parsing.csv',
        path_output='/p/projects/eubucco/data/1-intermediary-outputs',
        path_stats='/p/projects/eubucco/stats/1-parsing'):
    """
    Function to concatenate osm-split files from parallelized parsing

    """
    # define dict with number of files
    dict_file_num = {'france': 27, 'germany': 6, 'italy': 5, 'poland': 18, 'netherlands': 15}

    args = arg_parser(['i'])
    print(args.i)
    p = get_params(args.i, path_to_param_file)

    print(p['country'])
    print(p['dataset_name'])
    num_files = dict_file_num[p['country']]
    print('files: ', num_files)

    # initialise
    df_results_stats = pd.DataFrame()
    df_results_geom = pd.DataFrame()
    df_results_attrib = pd.DataFrame()
    df_results_extra_attrib = pd.DataFrame()
    lst_types = []

    for n in range(num_files):
        print('adding file {} of {}'.format(n, num_files))

        # read in csv files
        df_stats = pd.read_csv(os.path.join(path_stats, 'osm', p['dataset_name'] + '_' + str(n) + '_stat.csv'))
        df_geom = pd.read_csv(
            os.path.join(
                path_output,
                p['country'],
                'osm',
                p['dataset_name'] +
                '_' +
                str(n) +
                '-3035_geoms.csv'))
        df_attrib = pd.read_csv(
            os.path.join(
                path_output,
                p['country'],
                'osm',
                p['dataset_name'] +
                '_' +
                str(n) +
                '_attrib.csv'))
        df_extra_attrib = pd.read_csv(
            os.path.join(
                path_output,
                p['country'],
                'osm',
                p['dataset_name'] +
                '_' +
                str(n) +
                '_extra_attrib.csv'))

        # collect types
        lst_types.append(ast.literal_eval(df_stats.set_bldg_types.iloc[0]))

        # add to output df
        df_results_stats = pd.concat([df_results_stats, df_stats])
        df_results_geom = pd.concat([df_results_geom, df_geom])
        df_results_attrib = pd.concat([df_results_attrib, df_attrib])
        df_results_extra_attrib = pd.concat([df_results_attrib, df_attrib])

    # flatten list of type lists
    flat_list_types = [val for sublist in lst_types for val in sublist]

    # reset_index
    df_results_geom = df_results_geom.reset_index(drop=True)
    df_results_attrib = df_results_attrib.reset_index(drop=True)
    df_results_extra_attrib = df_results_extra_attrib.reset_index(drop=True)

    """
    # rename id to avoid duplicates across osm parts
    # split id str to keep old source file
    df_id = df_results_geom['id'].str.split('_',expand = True)
    # add if numbering as range over all osm parts
    df_id[1] = range(len(df_results_geom))
    df_id[1] = df_id[1].apply(str)
    # add cols back together
    df_results_geom['id'] = df_id[0]+'_'+df_id[1]
    print('id column: ',df_results_geom.id.values)
    print('num bldgs: ',len(df_results_geom))
    # asssign id to other files
    df_results_attrib.id = df_results_geom.id
    df_results_extra_attrib.id = df_results_geom.id
    """
    # save
    df_results_geom.to_csv(os.path.join(path_output, p['country'], p['dataset_name'] + '-3035_geoms.csv'), index=False)
    print('Geometries saved successfully.')

    df_results_attrib.to_csv(os.path.join(path_output, p['country'], p['dataset_name'] + '_attrib.csv'), index=False)
    print('Attributes saved successfully.')

    df_results_extra_attrib.to_csv(
        os.path.join(
            path_output,
            p['country'],
            p['dataset_name'] +
            '_extra_attrib.csv'),
        index=False)
    print('Extra Attributes saved successfully.')

    dict_stats = {'dataset_name': [df_results_stats.iloc[0].dataset_name],
                  'n_bldgs': [sum(df_results_stats.n_bldgs)],
                  'n_height': [sum(df_results_stats.n_height)],
                  'frac_height': [round(sum(df_results_stats.n_height) / sum(df_results_stats.n_bldgs), 2)],
                  'n_type_source': [sum(df_results_stats.n_type_source)],
                  'frac_type_source': [round(sum(df_results_stats.n_type_source) / sum(df_results_stats.n_bldgs), 2)],
                  'n_age': [sum(df_results_stats.n_age)],
                  'frac_age': [round(sum(df_results_stats.n_age) / sum(df_results_stats.n_bldgs), 2)],
                  'n_floors': [sum(df_results_stats.n_floors)],
                  'frac_floors': [round(sum(df_results_stats.n_floors) / sum(df_results_stats.n_bldgs), 2)],
                  'set_bldg_types': [list(set(flat_list_types))],
                  'n_files': [str(num_files)]
                  }

    df_results_stats_out = pd.DataFrame(dict_stats)
    df_results_stats_out.to_csv(os.path.join(path_stats, p['dataset_name'] + '_stat.csv'), index=False)
    print('Stats saved successfully.')


def concate_flanders():
    path_raw = '/p/projects/eubucco/data/0-raw-data/gvt-data/belgium/flanders/local_parsed/flanders/'

    df_out = pd.DataFrame()
    for i in range(21):
        print('adding file {} of {}'.format(i, 20))
        df = pd.read_csv(os.path.join(path_raw, 'GRBGebL1D1_1_3035_' + str(i) + '.csv'))
        df_out = pd.concat([df_out, df])
    print('saving files')
    df_out.to_csv('/p/projects/eubucco/data/0-raw-data/gvt-data/belgium/flanders/flanders-3035-raw.csv')
    print('all saved - closing run.')


CRS_UNI=3035

def load(path_points,path_polys,crs=CRS_UNI):
    gdf_point = gpd.read_file(path_points).to_crs(crs)
    gdf_polygon = ufo_helpers.import_csv_w_wkt_to_gdf(path_polys, crs = crs)
    return gdf_point,gdf_polygon


def check_duplicates(df,id_col):
    '''
        Checks duplicates on ids and drops them in cases some exist.
    '''
    n_duplicates = len(df.loc[df.duplicated(subset=id_col)])
    if n_duplicates>0:
        print(f'Warning! Found {n_duplicates} duplicates!')
        df = df.drop_duplicates(subset=id_col)
    return df


def sjoin_point_polygon(gdf_polygon,gdf_point,buffer_size=20):
    ''' 
        Joins every point (with buffer of `buffer_size` meters) intersecting polygon
        to polygon.

    '''
    gdf_point['geometry_point'] = gdf_point.geometry
    gdf_point['geometry'] = gdf_point.geometry.buffer(buffer_size)
    return gpd.sjoin(gdf_polygon, gdf_point, how="left", predicate='intersects')


def get_unique_points(gdf,id_col='ID'):
    '''
        Keeps the point that is closest to the polygon for each polygon,
        in cases where several points have been matched to a polygon.

        The distance is computed from the edge of the polygon closest
        to the point. 
        
        If points are within the polygon, distance is 0 and the
        point kept is the first one in appearing in the df.
    '''
    gdf['distance'] = gdf.geometry.distance(gdf.geometry_point)
    return gdf.sort_values('distance', ascending=True).drop_duplicates(id_col)


def get_unique_polys(gdf,id_col='id'):
    '''
    Removes all duplicated polygons, based on shortest distance to point.
    '''
    return gdf.sort_values('distance', ascending=True).drop_duplicates(id_col).dropna()


def point_to_poly(path_point, path_polygon,id_col_point ='ID',id_col_polygon='id', buffer_size=20):
    '''
    Matches one point out of manny to closest polygon. 
    '''
    gdf_point,gdf_polygon=load(path_point,path_polygon,crs=CRS_UNI)
    gdf_point = check_duplicates(gdf_point,id_col_point)
    gdf_polygon = check_duplicates(gdf_polygon,id_col_polygon)
    gdf_sjoin = sjoin_point_polygon(gdf_polygon,gdf_point,buffer_size)
    gdf_sjoin_unique = get_unique_points(gdf_sjoin,id_col='ID')
    return get_unique_polys(gdf_sjoin_unique,id_col='id')

