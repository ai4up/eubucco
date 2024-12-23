import pandas as pd
import glob
import numpy as np
import os, sys
import glob
from shapely import wkt
import geopandas as gpd
import ast
from pathlib import Path

from ufo_map.Utils.helpers import get_all_paths, arg_parser
from preproc.parsing import get_params

# Declare global variables
CRS_UNI = 'EPSG:3035'
DIG = 3

# Low level helpers
# helper functions


def get_path_w_ending(ending, path_parts):
    # get all paths ending
    paths_ending = [p for p in path_parts if ending in p]
    # sort paths which contain 'attrib'
    if ending == '_attrib':
        paths_ending = [k for k in paths_ending if '_extra_attrib' not in k]
        paths_ending = [k for k in paths_ending if '_attrib_source' not in k]
    return paths_ending


def get_n_files(path_parts):
    """creates list of num of files per ending
    """
    n_files = []
    for ending in ['_geom', '_attrib', '_source', '_extra', '_buffer', '_boundary']:
        paths_ending = [p for p in path_parts if ending in p]
        if ending == '_attrib':
            paths_ending = [k for k in paths_ending if '_extra_attrib' not in k]
            paths_ending = [k for k in paths_ending if '_attrib_source' not in k]

        if not paths_ending:
            n = 0
        else:
            n = len(paths_ending)
        n_files.append(n)
    return n_files


def get_paths_dataset(
        dataset_name,
        ending,
        path_inputs_csv='/p/projects/eubucco/git-eubucco/database/preprocessing/1-parsing/inputs-parsing.csv',
        path_database='/p/projects/eubucco/data/2-database-city-level') -> list:
    '''
        Gets the paths of all cities in the db for a given dataset.

        Parameters:
        * dataset_name (str)
        * path_inputs_csv (default)
        * paths_database (default) <- parent folder for the 2-database-city-level

        Returns: list
    '''
    inputs_parsing = pd.read_csv(path_inputs_csv)

    print(dataset_name)

    # get relevant level and name
    if isinstance(dataset_name, str):
        gadm_level = inputs_parsing.loc[inputs_parsing.dataset_name == dataset_name].gadm_level.values[0]
        gadm_name = inputs_parsing.loc[inputs_parsing.dataset_name == dataset_name].gadm_name.values[0]
    elif isinstance(dataset_name, int):
        gadm_level = inputs_parsing.iloc[dataset_name].gadm_level
        gadm_name = inputs_parsing.iloc[dataset_name].gadm_name
        dataset_name = inputs_parsing.iloc[dataset_name].dataset_name
        print(dataset_name)
    else:
        sys.exit('Dataset name not recognized.')

    country_name = inputs_parsing.loc[inputs_parsing.dataset_name == dataset_name].country.values[0]
    with open(os.path.join(path_database, country_name, 'paths_' + country_name + '.txt')) as f:
        paths = [line.rstrip() + '_' + ending + '.csv' for line in f]

    paths = [(path, os.path.normpath(path).split(os.path.sep)) for path in paths]

    # for extra cases where additional regions/cities are provided as lists, the above will return []
    if dataset_name == 'spain-osm':
        # converst str of list in list
        gadm_name = ast.literal_eval(gadm_name)
        # get paths if region is list gadm_names
        paths = [path[0] for path in paths if path[1][-3] in gadm_name]

    elif dataset_name == 'italy-osm':
        # we split italy in two parts;

        # a) first we get city paths that are not covered by gov data
        # converst str of list in list
        gadm_name = ast.literal_eval(gadm_name)
        # get paths if region is list gadm_names
        paths_a = [path[0] for path in paths if path[1][-2] in gadm_name]

        # b) then we get the remaining osm paths
        # first we set gadm level and gadm_name to rest
        inputs_parsing[inputs_parsing.dataset_name == 'italy-osm'].gadm_level = 'rest'
        inputs_parsing[inputs_parsing.dataset_name == 'italy-osm'].gadm_name = 'rest'
        # then we take same code as above for getting the paths for 'rest'
        gadm_level = list(inputs_parsing[inputs_parsing.country == country_name].gadm_level)
        gadm_name = list(inputs_parsing[inputs_parsing.country == country_name].gadm_name)
        # exclude the rest command from the list
        gadm_level = [lev for lev in gadm_level if lev not in ['rest', np.nan]]
        gadm_name = [nam for nam in gadm_name if nam not in ['rest', np.nan]]
        for level, name in zip(gadm_level, gadm_name):
            if level == 'region_name':
                paths = [path for path in paths if name != path[1][-3]]
            elif level == 'city_name':
                paths = [path for path in paths if name != path[1][-2]]
        paths_b = [path[0] for path in paths]

        # c) finally add both together
        paths = paths_a + paths_b

    elif gadm_level == 'region_name':
        paths = [path[0] for path in paths if gadm_name == path[1][-3]]

    elif gadm_level == 'city_name':
        paths = [path[0] for path in paths if gadm_name == path[1][-2]]

    elif gadm_level == 'rest':
        gadm_level = list(inputs_parsing[inputs_parsing.country == country_name].gadm_level)
        gadm_name = list(inputs_parsing[inputs_parsing.country == country_name].gadm_name)
        # exclude the rest command from the list
        gadm_level = [lev for lev in gadm_level if lev not in ['rest', np.nan]]
        gadm_name = [nam for nam in gadm_name if nam not in ['rest', np.nan]]

        for level, name in zip(gadm_level, gadm_name):
            if level == 'region_name':
                paths = [path for path in paths if name != path[1][-3]]
            elif level == 'city_name':
                paths = [path for path in paths if name != path[1][-2]]
        paths = [path[0] for path in paths]

    elif gadm_level == 'all':
        paths = [path[0] for path in paths]

    # in case we are on a mounted volume (which doesn't start with /p/projects...)
    if path_database.split('/',2)[1] !='p':
        paths = [p.split('/p/projects/eubucco/data/')[1] for p in paths]
        paths = [path_database +'/' + l.split('/',1)[1] for l in paths]     

    return paths, dataset_name


def remove_u0(df, paths_ending, ending, bid_0):
    """
    func to remove unnamed_col: 0 column and set 1 in array to mark in which file we removed the col
    """
    # check if 'Unnamed: 0' col in df
    if 'Unnamed: 0' in df.columns:
        print('dropping Unnamed_0 for ', ending)
        # print(paths_ending[0])
        df = df.drop(columns='Unnamed: 0')
        # save to path in case dropped
        df.to_csv(paths_ending[0], index=False)
        # based on ending set bid
        dict_ending = {'_geom': 0, '_attrib': 1, '_attrib_source': 2, '_extra_attrib': 3, '_buffer': 4, '_boundary': 5}
        bid_0[dict_ending[ending]] = 1
    return bid_0, df


def metrics_foot(df, dict_city, case=None):
    """Func to calculate area related stats from geom file incl. area metrics and num metrics
    """
    list_a_foot = ('a_tot', 'a_mean', 'a_med', 'a_max', 'a_min', 'a_25', 'a_75', 'a_n_0', 'a_p_0', 'a_n_10', 'a_p_10')
    if case == 'zeros':
        for stat in list_a_foot:
            dict_city[stat] = 0
    elif case == 'nan':
        for stat in list_a_foot:
            dict_city[stat] = np.nan
    else:
        dict_city['a_tot'] = round(np.sum(df.geometry.area), DIG)
        dict_city['a_mean'] = round(np.mean(df.geometry.area), DIG)
        dict_city['a_med'] = round(np.median(df.geometry.area), DIG)
        dict_city['a_max'] = round(np.max(df.geometry.area), DIG)
        dict_city['a_min'] = round(np.min(df.geometry.area), DIG)
        dict_city['a_25'] = round(np.percentile(df.geometry.area, 25), DIG)
        dict_city['a_75'] = round(np.percentile(df.geometry.area, 75), DIG)
        dict_city['a_n_0'] = len(df.loc[df.geometry.area <= 0])
        dict_city['a_p_0'] = round(dict_city['a_n_0'] / len(df), DIG)
        dict_city['a_n_10'] = len(df.loc[df.geometry.area <= 10])
        dict_city['a_p_10'] = round(dict_city['a_n_10'] / len(df), DIG)
    return dict_city


def metrics_height(df, dict_city, case=None):
    """Func to calculate height stats from attrib file
    """
    list_height_attrib = (
        'height_n',
        'height_p',
        'height_n_inf_0',
        'height_n_0_2',
        'height_n_3_5',
        'height_n_6_10',
        'height_n_11_15',
        'height_n_16_25',
        'height_n_26_inf',
        'height_n_inf_0',
        'height_p_0_2',
        'height_p_3_5',
        'height_p_6_10',
        'height_p_11_15',
        'height_p_16_25',
        'height_p_26_inf',
        'height_mean',
        'height_med',
        'height_max',
        'height_min')
    # if no attrib file could be found
    if case == 'zeros':
        for stat in list_height_attrib:
            dict_city[stat] = 0
    # if several attrib files could be found (error!)
    elif case == 'nan':
        for stat in list_height_attrib:
            dict_city[stat] = np.nan
    # calc height stats (default)
    else:
        dict_city['height_n'] = len(df.height.loc[~df.height.isna()])
        dict_city['height_p'] = round(dict_city['height_n'] / len(df), 2)
        dict_city['height_n_inf_0'] = len(df[(df.height <= 0)])
        dict_city['height_n_0_2'] = len(df[((df.height > 0) & (df.height <= 2))])
        dict_city['height_n_3_5'] = len(df[((df.height > 2) & (df.height <= 5))])
        dict_city['height_n_6_10'] = len(df[((df.height > 5) & (df.height <= 10))])
        dict_city['height_n_11_15'] = len(df[((df.height > 10) & (df.height <= 15))])
        dict_city['height_n_16_25'] = len(df[((df.height > 15) & (df.height <= 25))])
        dict_city['height_n_26_inf'] = len(df[(df.height > 25)])
        dict_city['height_p_inf_0'] = round(dict_city['height_n_inf_0'] / len(df), DIG)
        dict_city['height_p_0_2'] = round(dict_city['height_n_0_2'] / len(df), DIG)
        dict_city['height_p_3_5'] = round(dict_city['height_n_3_5'] / len(df), DIG)
        dict_city['height_p_6_10'] = round(dict_city['height_n_6_10'] / len(df), DIG)
        dict_city['height_p_11_15'] = round(dict_city['height_n_11_15'] / len(df), DIG)
        dict_city['height_p_16_25'] = round(dict_city['height_n_16_25'] / len(df), DIG)
        dict_city['height_p_26_inf'] = round(dict_city['height_n_26_inf'] / len(df), DIG)
        dict_city['height_mean'] = round(np.mean(df.height), DIG)
        dict_city['height_med'] = round(np.median(df.height), DIG)
        dict_city['height_max'] = round(np.max(df.height), DIG)
        dict_city['height_min'] = round(np.min(df.height), DIG)
    return dict_city


def metrics_age(df, dict_city, case=None):
    """Func to calculate age stats from attrib file
    """
    list_age_attrib = ('age_n', 'age_p', 'age_n_0_1800', 'age_n_1801_1900', 'age_n_1901_1950',
                       'age_n_1951_2000', 'age_n_2001_2022', 'age_n_2023_inf', 'age_p_0_1800', 'age_p_1801_1900',
                       'age_p_1901_1950', 'age_p_1951_2000', 'age_p_2001_2022', 'age_p_2023_inf',
                       'age_mean', 'age_med', 'age_max', 'age_min')

    # if no attrib file could be found
    if case == 'zeros':
        for stat in list_age_attrib:
            dict_city[stat] = 0
    # if several attrib file could be found (error!)
    elif case == 'nan':
        for stat in list_age_attrib:
            dict_city[stat] = np.nan
    # if str in age col
    elif set([type(x) for x in df.age if isinstance(x, str)]):
        for stat in list_age_attrib:
            dict_city[stat] = np.nan
    # calc age stats (default)
    else:
        dict_city['age_n'] = len(df.age.loc[~df.age.isna()])
        dict_city['age_p'] = round(dict_city['age_n'] / len(df), 2)
        dict_city['age_n_0_1800'] = len(df[(df.age <= 1800)])
        dict_city['age_n_1801_1900'] = len(df[((df.age > 1800) & (df.age <= 1900))])
        dict_city['age_n_1901_1950'] = len(df[((df.age > 1900) & (df.age <= 1950))])
        dict_city['age_n_1951_2000'] = len(df[((df.age > 1950) & (df.age <= 2000))])
        dict_city['age_n_2001_2022'] = len(df[((df.age > 2000) & (df.age <= 2022))])
        dict_city['age_n_2023_inf'] = len(df[(df.age > 2022)])
        dict_city['age_p_0_1800'] = round(dict_city['age_n_0_1800'] / len(df), DIG)
        dict_city['age_p_1801_1900'] = round(dict_city['age_n_1801_1900'] / len(df), DIG)
        dict_city['age_p_1901_1950'] = round(dict_city['age_n_1901_1950'] / len(df), DIG)
        dict_city['age_p_1951_2000'] = round(dict_city['age_n_1951_2000'] / len(df), DIG)
        dict_city['age_p_2001_2022'] = round(dict_city['age_n_2001_2022'] / len(df), DIG)
        dict_city['age_p_2023_inf'] = round(dict_city['age_n_2023_inf'] / len(df), DIG)
        dict_city['age_mean'] = round(np.mean(df.age), DIG)
        dict_city['age_med'] = round(np.median(df.age), DIG)
        dict_city['age_max'] = round(np.max(df.age), DIG)
        dict_city['age_min'] = round(np.min(df.age), DIG)
    return dict_city


def metrics_type(df, dict_city, case=None):
    """Func to calculate type stats from attrib file
    """
    list_type_attrib = ('type_n', 'type_p', 'type_n_res', 'type_n_non_res', 'type_p_res', 'type_p_non_res')
    # if no attrib file could be found
    if case == 'zeros':
        for stat in list_type_attrib:
            dict_city[stat] = 0
    # if several attrib file could be found (error!)
    elif case == 'nan':
        for stat in list_type_attrib:
            dict_city[stat] = np.nan
    # calc age stats (default)
    else:
        dict_city['type_n'] = len(df.type.loc[~df.type.isna()])
        dict_city['type_p'] = round(dict_city['type_n'] / len(df), DIG)
        dict_city['type_n_res'] = len(df.loc[df.type == 'residential'])
        dict_city['type_n_non_res'] = len(df.loc[df.type == 'non-residential'])
        dict_city['type_p_res'] = round(dict_city['type_n_res'] / len(df), DIG)
        dict_city['type_p_non_res'] = round(dict_city['type_n_non_res'] / len(df), DIG)
        # TODO add last type entry
    return dict_city


def metrics_buffer(dict_city, path_parts, bid_0):
    """func to get num of bldgs in buffer and remove unnamed_0
    # no additional values are added to dict_city, but can be in the future
    """
    path_buff = [p for p in path_parts if '_buffer' in p]
    if not path_buff:
        dict_city['n_bldg_buffer'] = 0
    elif len(path_buff) > 1:
        dict_city['n_bldg_buffer'] = np.nan
    else:
        df = pd.read_csv(path_buff[0])
        dict_city['n_bldg_buffer'] = len(df)
        bid_0, df = remove_u0(df, path_buff, '_buffer', bid_0)
    return dict_city


def metrics_boundary(dict_city, path_parts, bid_0):
    """func to remove unnamed 0 in boundary
    # no additional values are added to dict_city, but can be in the future
    """
    path_boundary = [p for p in path_parts if '_boundary' in p]
    if path_boundary:
        if len(path_boundary) == 1:
            df = pd.read_csv(path_boundary[0])
            bid_0, df = remove_u0(df, path_boundary, '_boundary', bid_0)
    return dict_city


###########################
### Mid Level Assigner ####
def calc_file_attribs(df, dict_city, paths_ending, ending, bid_0):
    """
    Func to manage calculation of relevant metrics for geom, attrib and source files
    """
    # load file and remove "Unnamed: 0" col
    if ending == '_geom':
        # read in as gdf
        df = gpd.GeoDataFrame(df, geometry=df['geometry'].apply(wkt.loads), crs=CRS_UNI)
        # get number of bldgs in city
        dict_city['bldgs_n_tot'] = len(df)
        # area metrics
        dict_city = metrics_foot(df, dict_city)
        # check if 'Unnamed: 0' col and remove & set flag
        bid_0, df = remove_u0(df, paths_ending, ending, bid_0)
        # save num bldgs in attrib file
        num_bldgs = len(df)
        # save ids
        list_ids = list(df.id)

    # read in other files
    else:
        # calc metrics from attributes
        if ending == '_attrib':
            # calculate height metrics
            dict_city = metrics_height(df, dict_city)
            # calculate age metrics
            dict_city = metrics_age(df, dict_city)
            # calculate type metrics
            dict_city = metrics_type(df, dict_city)

        # check if 'Unnamed: 0' col and remove & set flag
        bid_0, df = remove_u0(df, paths_ending, ending, bid_0)
        # save num bldgs in attrib file
        num_bldgs = len(df)
        # save ids
        list_ids = list(df.id)

    return dict_city, list_ids, num_bldgs, bid_0


def calc_file_attribs_err(dict_city, paths_ending, ending):
    """Func to manage assignment of zeros or nans in case we have 0 or more than 1 file in the city foler for ending
    """
    # determine if we fill in 0s or nans, depending on whether we have no or more than one file of ending
    if len(paths_ending) == 0:
        case = 'zeros'
        num_bldgs = 0
        list_ids = []
    else:
        case = 'nan'
        num_bldgs = np.nan
        list_ids = [np.nan]

    # create empty df for metrics functions
    df_empty = pd.DataFrame()
    # check geom file
    if ending == '_geom':
        if len(paths_ending) == 0:
            # if geom file is not there, set bldgs_n_tot to 0
            dict_city['bldgs_n_tot'] = 0
        else:
            # if geom file is several times there, set bldgs_n_tot to np.nan
            dict_city['bldgs_n_tot'] = np.nan
        # assign nan to footprint metrics
        dict_city = metrics_foot(df_empty, dict_city, case)

    elif ending == '_attrib':
        # calculate height metrics
        dict_city = metrics_height(df_empty, dict_city, case)
        # calculate age metrics
        dict_city = metrics_age(df_empty, dict_city, case)
        # calculate type metrics
        dict_city = metrics_type(df_empty, dict_city, case)
    return dict_city, list_ids, num_bldgs


def metrics_main(dict_city, path_parts):
    """
    Main func to summarise stats related to geom, attrib and attrib_source_files
    """
    # intialise bid_0 for marking where we delete col 'Unnamed: 0"
    bid_0 = [0, 0, 0, 0, 0, 0]
    # initialse lists
    num_bldgs_files = []
    list_ids_files = []
    # check if consistent number of files is present
    for ending in ['_geom', '_attrib', '_attrib_source']:
        # re-set edge case boolean for handling empty files
        bool_edge_case = False

        # get paths_ending
        paths_ending = get_path_w_ending(ending, path_parts)

        # if we have only one file with ending (default case)
        if len(paths_ending) == 1:
            print('default case for {}'.format(ending))
            # try reading in csv
            try:
                df = pd.read_csv(paths_ending[0])
            except BaseException:
                print('Warning!: found empty csv which could not be read.')
                df = pd.DataFrame()
            # check if csv has content if yes, calc attribs
            if not df.empty:
                dict_city, list_ids, num_bldgs, bid_0 = calc_file_attribs(df, dict_city, paths_ending, ending, bid_0)
            else:
                bool_edge_case = True

        # if we have several, no files for ending or we have edge case that file is empty
        if ((len(paths_ending) != 1) or bool_edge_case):
            print('err case for {}'.format(ending))
            dict_city, list_ids, num_bldgs = calc_file_attribs_err(dict_city, paths_ending, ending)

        # append num_bldgs, list_ids
        num_bldgs_files.append(num_bldgs)
        list_ids_files.append(list_ids)

    # return
    return dict_city, num_bldgs_files, list_ids_files, bid_0


def metrics_extra(dict_city, path_extra, bid_0, num_bldgs_files, list_ids_files):
    """
    Func to summarise stats related extra_attribs
    """
    # if we have only one extra_attrib file
    if len(path_extra) == 1:
        # take str of path
        #path_extra = path_extra[0]
        # read in df_extra
        df = pd.read_csv(path_extra[0])
        # check if 'Unnamed: 0' col and remove & set flag
        bid_0, df = remove_u0(df, path_extra, '_extra_attrib', bid_0)
        # check for col names to save extra attribs
        dict_city['extra_attribs'] = [[col for col in df.columns if col != 'id']]
        # save num bldgs in attrib file
        num_bldgs = len(df)
        # save ids
        list_ids = list(df.id)
        # append num_bldgs, list_ids
        # num_bldgs_files.append(num_bldgs)
        list_ids_files.append(list_ids)

    # if we have several extra_attrib files, set all dict values to nan (error!)
    elif len(path_extra) > 1:
        # bid_0, list_ids_files, num_bldgs_files stay unaltered
        # add np.nan to extra_attribs col
        dict_city['extra_attribs'] = [np.nan]

    # if we have no extra_attrib file, set all dict values to 0
    else:
        # bid_0, list_ids_files, num_bldgs_files stay unaltered
        # add 0 to extra_attribs col
        dict_city['extra_attribs'] = [0]

    return dict_city, num_bldgs_files, list_ids_files, bid_0


##############
### MAIN #####
def create_stats_main(country,
                    db_version=0.1,
                    path_db_folder='/p/projects/eubucco/data/2-database-city-level-v0_1',
                    path_out='/p/projects/eubucco/stats/2-db-set-up/overview',
                    path_root_id='/p/projects/eubucco/data/0-raw-data/id_look_up/country-ids'):


    print(country)
    print('----------')

    # get all paths for this country
    paths = get_all_paths(country, path_root_folder=path_db_folder)

    # read in id mapper
    df_id_mapper = pd.read_csv(os.path.join(path_root_id, country + '_ids.csv'))

    # intitalise output df
    df_out = pd.DataFrame()

    print('starting to loop through all file paths')
    # loop through each path per country
    for path in paths:
        # A) Initialse per city (given by path)
        # A.1) get all files for path
        path_glob = path + '*'
        path_parts = glob.glob(path_glob)

        # A.2) intialise dict
        list_path = os.path.normpath(path).split(os.path.sep)

        # get id of city
        city_id = df_id_mapper.loc[df_id_mapper.city_name == list_path[-1]].id_marker.values[0]
        # add db version
        city_id_version = 'v' + str(db_version) + '-' + city_id
        # create dict
        dict_city = {'id': city_id_version, 'region': list_path[-3], 'city': list_path[-1]}

        print('region: {}, city: {}'.format(dict_city['region'], dict_city['city']))
        # get overview of number of files
        dict_city['n_files'] = str(get_n_files(path_parts))

        # B) Calculate main metrics for files with individual bldgs per city
        # B.1) calculate main metrics for geoms, attribs and attribs_source
        dict_city, num_bldgs_files, list_ids_files, bid_0 = metrics_main(dict_city, path_parts)

        # B.2) Calculate metrics for extra attrib
        path_extra = [p for p in path_parts if 'extra_attrib' in p]
        dict_city, num_bldgs_files, list_ids_files, bid_0 = metrics_extra(
            dict_city, path_extra, bid_0, num_bldgs_files, list_ids_files)

        # B.3) Calculte cross file stats
        # check if num of bldgs is same across all files
        dict_city['const_bldgs_file'] = len(set(num_bldgs_files)) <= 1

        # depending on wheter we have an extra_attrib file adjust n_if_files for comparsion
        #if dict_city['extra_attribs'][0]==0:n_id_files=3
        # else: n_id_files=4
        # only check for const num of bldgs across geom, attrib and source file
        n_id_files = 3

        # check if ids are const across all files
        list_id_0 = list_ids_files[0]
        if len([True for lst in list_ids_files if lst == list_id_0]) < n_id_files:
            dict_city['const_id_files'] = False
        else:
            dict_city['const_id_files'] = True

        # C) Check additional files
        # C.1) Analyse buffer file
        dict_city = metrics_buffer(dict_city, path_parts, bid_0)
        # C.2) Analyse boundary file
        dict_city = metrics_boundary(dict_city, path_parts, bid_0)

        # assign 0 bid to output
        dict_city['unnamed_0_bits'] = str(bid_0)

        # print(dict_city)
        # for key in dict_city:
        #    print('{}: {}'.format(key,dict_city[key]))

        # D) add dict as new col to df_out
        # df append dict to city_name
        df_out = df_out.append(dict_city, ignore_index=True)

    print('saving collected overview')    
    Path(path_out).mkdir(parents=True, exist_ok=True)
    # check if files are present in path_out to not overwrite and instead add counter
    path_save = os.path.join(path_out, country + '_overview*')
    files_present = glob.glob(path_save)
    if len(files_present) > 0:
        # add count to ending and save
        df_out.to_csv(os.path.join(path_out, country +
                      '_overview_' + str(len(files_present)) + '.csv'), index=False)
    else:
        df_out.to_csv(os.path.join(path_out, country + '_overview.csv'), index=False)
    print('Everything saved successully. Closing run.')



def create_overview_laus(country,
                         path_db_folder,
                         source,
                         path_out='/p/projects/eubucco/stats/2-db-set-up/overview/v1'):
    """
        Function checks every NUTS3 according to the path file, and compute metrics for each LAU present.
        Function then allocates 0s for LAUs present in the path file but not in the data.
    """

    paths = get_all_paths(country, path_root_folder=path_db_folder)
    nuts3_path = list(set([os.path.split(x)[0] for x in paths]))

    df_stats = pd.DataFrame()

    for path in nuts3_path:
        
        try:
            df = gpd.read_file(f'{path}.gpkg')
            df['area'] = round(df.geometry.area,0)
            df = df.groupby('LAU_ID').agg(area=('area', 'sum'),     
                                                  n_bldgs=('area', 'count')).reset_index()   
            df.insert(0,'NUTS3_ID', path.split('/')[-1])
            df_stats = pd.concat([df_stats,df])
        except:
            print(f'{path} missing.')

    df_all = pd.DataFrame([path.split('/')[-2:] for path in paths], columns=["NUTS3_ID", "LAU_ID"])

    if df_stats.empty:
        df_stats = df_all
        df_stats.insert(2,'area',0)
        df_stats.insert(3,'n_bldgs',0)

    else:
        missing = df_all[~df_all.LAU_ID.isin(df_stats.LAU_ID)]
        missing.insert(2,'area',0)
        missing.insert(3,'n_bldgs',0)
        df_stats = pd.concat([df_stats,missing])

    print(df_stats.n_bldgs.sum())

    df_stats.to_csv(os.path.join(path_out,f'{source}_{country}_overview.csv'),index=False)
    print('Done!')