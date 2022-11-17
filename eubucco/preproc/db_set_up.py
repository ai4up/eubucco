import pandas as pd
import geopandas as gpd
from shapely import wkt
import os
from pathlib import Path
import time
from datetime import date
import glob
import ast

import ufo_map.Utils.helpers as ufo_helpers
from ufo_map.Feature_engineering.urban_atlas import building_in_ua
from utils.extra_cases import average_flanders_dupls

# declare global var
CRS_UNI = 'EPSG:3035'

# LOW
"""
def check_edge_cases(gadm_name):
    # tiny helper function to hardcode city_name changes that pd.read_csv does not support.
    #    So far we have the following edges cases (add in future f.e. København):
    #    - Thueringen --> Thüringen

    # TODO take care of praha edge case
    #if not isinstance(gadm_name,str):
    #gadm_name= [name.replace('Thueringen', 'Thüringen') for name in gadm_name]
    #gadm_name= [name.replace('Praha - zapad','Praha - západ') for name in gadm_name]

    if gadm_name == 'Thueringen':
        gadm_name = 'Thüringen'
    return gadm_name
"""


def mask_gadm(gadm_file, dataset_name, country_name, path_inputs_parsing):
    """
    Function to create a temporary gadm file that only consists of gadm bounds of the specific gov or osm dataset.
    Case 1: one dataset per country -> no masking, we take all gadm bounds for this countryname
    Case 2: current dataset is remaining one of several datasets in a country (f.e. austria-osm), marked with "rest" in gadm_level and gadm_name col,
            exclude all gadm bounds of regions/cities of other datasets in this country
    Case 3: current dataset is one of several in a country, take only gadm bound relevant for this dataset

    """
    inputs_parsing = pd.read_csv(path_inputs_parsing)

    # get gadm_level and gadm_name
    gadm_level = inputs_parsing.loc[inputs_parsing.dataset_name == dataset_name].gadm_level.values[0]
    gadm_name = inputs_parsing.loc[inputs_parsing.dataset_name == dataset_name].gadm_name.values[0]

    # check if gadm_level is 'all'; then no value is given and we mask all
    if gadm_level == 'all':
        gadm_file_temp = gadm_file
    # if rest, mask all apart from the other gov datasets
    elif gadm_level == 'rest':
        # read in the gadm_level and gadm_name info from inputs_parsing
        gadm_level = list(inputs_parsing[inputs_parsing.country == country_name].gadm_level)
        gadm_name = list(inputs_parsing[inputs_parsing.country == country_name].gadm_name)
        # exclude the rest command from the list
        gadm_level = ([lev for lev in gadm_level if lev != 'rest'])
        gadm_name = [nam for nam in gadm_name if nam != 'rest']
        # assign to GADM_temp all regions and cities apart from the ones in gadm_level and gad_names
        gadm_file_temp = gadm_file
        for i in range(len(gadm_level)):
            # only if gadm_level is not nan (f.e. because we left out friuli in italy,
            # but its still in inputs-parsing.csv)
            if not is_nan(gadm_level[i]):
                # check for potential edge cases, f.e. Thüringen
                #gadm_name[i] = check_edge_cases(gadm_name[i])
                # mask
                gadm_file_temp = gadm_file_temp.loc[gadm_file_temp[gadm_level[i]] != gadm_name[i]]

    # in case gadm_level is nan - raise error
    elif is_nan(gadm_level):
        raise ValueError("No gadm_level provided")

    # edge case to fill up empty cities in italy or spain
    elif dataset_name in ['italy-osm', 'spain-osm']:
        gadm_name = ast.literal_eval(gadm_name)
        gadm_file_temp = gadm_file.loc[gadm_file[gadm_level].isin(gadm_name)]

    # if value given, sjoin only with regions/cities of gov dataset
    else:
        #gadm_name = check_edge_cases(gadm_name)
        gadm_file_temp = gadm_file.loc[gadm_file[gadm_level] == gadm_name]
    return gadm_file_temp


def is_nan(x):
    return (x != x)


def get_city_per_bldg(gdf_bldg, gdf_GADM):
    """
    uses the ufo-maps urban atlas function to allocate check for each bldg with which gadm
    bound it has largest intersection.
    """

    # get list of geoms for calculations
    geometries = list(gdf_bldg.geometry)
    gadm_geometries = list(gdf_GADM.geometry)
    # get sindex of gadm geoms
    gadm_sindex = gdf_GADM.sindex
    # classes are the list of cities in gadm file
    gadm_classes = list(gdf_GADM.city_name)
    # get list with one city per bldg
    bldg_in_city_list = building_in_ua(geometries, gadm_sindex, gadm_geometries, gadm_classes)
    return bldg_in_city_list


def remove_dupls(gdf, file_name, str_subset=None):
    """
    Function to remove duplicates
    """
    # check for duplicates and report
    len_before_dupl = len(gdf)
    gdf = gdf.drop_duplicates()
    if len_before_dupl != len(gdf):
        print('Dropped {} duplicates in {}'.format(len_before_dupl - len(gdf), file_name))
    len_before_dupl = len(gdf)
    if str_subset:
        gdf = gdf.drop_duplicates(subset=[str_subset])
        if len_before_dupl != len(gdf):
            print('Dropped {} {} duplicates in {}'.format(len_before_dupl - len(gdf), str_subset, file_name))
    return gdf


def get_attribs(path_int_fol, country_name, dataset_name):
    """
    Function to rad in attrib and x-attrib files per dataset_name
    """
    path_attrib = os.path.join(path_int_fol, country_name, dataset_name + '_attrib.csv')
    path_x_attrib = os.path.join(path_int_fol, country_name, dataset_name + '_extra_attrib.csv')

    # read in attrib file
    bldg_attrib = pd.read_csv(path_attrib)
    len0 = len(bldg_attrib)
    print('Num attribs in raw file: {}'.format(len0))

    # HARDcoded extra cases flanders
    if dataset_name == 'flanders-gov':
        # 1. cut all duplicates with same rows
        bldg_attrib = bldg_attrib.drop_duplicates()
        len1 = len(bldg_attrib)
        # 2. solving it with groupby func and averaging per same ids
        bldg_attrib = average_flanders_dupls(bldg_attrib)
        len2 = len(bldg_attrib)
        print('Extra case flanders, dropped {} duplicates and merged {}'.format(len0 - len1, len1 - len2))

    # Check for x-attribs
    try:
        bldg_x_attrib = pd.read_csv(path_x_attrib)
        print('Extra attribute file found.')
    except BaseException:
        bldg_x_attrib = pd.DataFrame()
    len_x_0 = len(bldg_x_attrib)

    # check for duplicates and report
    print('Checking for duplicates in attrib files')
    bldg_attrib = remove_dupls(bldg_attrib, 'attrib', 'id')
    if not bldg_x_attrib.empty:
        bldg_x_attrib = remove_dupls(bldg_x_attrib, 'x-attrib', 'id')

    dict_attrib_nums = {
        'num_attrib0': len0,
        'num_attrib1': len(bldg_attrib),
        'num_x_attrib0': len_x_0,
        'num_x_attrib1': len(bldg_x_attrib)}

    return bldg_attrib, bldg_x_attrib, dict_attrib_nums


def create_new_df_source(df_attrib, dataset_name):
    # create new csv to store sources per building (row)
    df_sources = pd.DataFrame()
    df_sources['id'] = df_attrib['id']
    df_sources['height'] = df_attrib['source_file']
    df_sources['type_source'] = df_attrib['source_file']
    df_sources['type'] = df_attrib['source_file']
    df_sources['age'] = df_attrib['source_file']
    df_sources['floors'] = df_attrib['source_file']
    df_sources['dataset_name'] = dataset_name

    print('Checking for duplicates in source files')
    df_sources = remove_dupls(df_sources, 'df_sources', 'id')

    return df_sources


def fetch_GADM_info_country(country_name,
                            levels=None,
                            path_sheet='gadm_table.csv',
                            path_root_folder='/p/projects/eubucco/data/0-raw-data/gadm'):
    '''
        Goes in the GADM sheet and picks up the info.

        Returns:

        * GADM_file - gpd.GeoDataFrame
        * country_name - string
        * level_city or all_levels - string or list of string
        * local crs - string ('EPSG:XXXX')
    '''
    # open sheet
    GADM_sheet = pd.read_csv(os.path.join(path_root_folder, path_sheet))

    # filter by country name
    GADM_country = GADM_sheet[GADM_sheet['country_name'] == country_name]

    # get GADM city file
    GADM_file = gpd.read_file(
        os.path.join(
            path_root_folder,
            GADM_country.country_name.iloc[0],
            'gadm36_{}_{}.shp'.format(
                GADM_country.gadm_code.iloc[0],
                GADM_country.level_city.iloc[0])),
        crs=4326)

    if levels == 'all':
        return(GADM_file, GADM_country.country_name.iloc[0], eval(GADM_country.all_levels.iloc[0]), GADM_country.local_crs.iloc[0])
    else:
        return(GADM_file, GADM_country.country_name.iloc[0], GADM_country.level_city.iloc[0], GADM_country.local_crs.iloc[0])


def clean_GADM_city_names(GADM_file, country_name, level_city):
    '''
        Handles cases in GADM when several cities in the same country have
        the same name. First tries to create a new name "city_name (region_name)",
        and if several cities have the same name within a region, then adds
        an index at the end of the name to differenciate them.

        Returns: cleaned gpd.GeoDataFrame
    '''

    # select useful columns
    if country_name in ['cyprus', 'ireland']:
        GADM_file = GADM_file[['NAME_1', 'geometry']]
        GADM_file.insert(0, 'region_name', GADM_file.NAME_1)
    else:
        GADM_file = GADM_file[['NAME_1', f'NAME_{level_city}', 'geometry']]
    GADM_file.columns = ['region_name', 'city_name', 'geometry']
    GADM_file['country_name'] = country_name

    # replace "/" in GADM_file, region names
    if GADM_file.city_name.str.contains("/").any():
        GADM_file['city_name'] = GADM_file.city_name.str.replace("/", "-")

    # replace "/" in GADM_file, city names
    if GADM_file.region_name.str.contains("/").any():
        GADM_file['region_name'] = GADM_file.region_name.str.replace("/", "-")

    # deal with duplicates
    g_d = GADM_file[GADM_file.duplicated(subset=['city_name'])]
    # add region_name to city name where we have duplicates
    g_d['city_name'] = g_d['city_name'] + ' (' + g_d['region_name'] + ')'
    # in cases where we have more than one duplicated; add orignal index as str behind dupl
    if country_name in ['germany', 'spain']:
        g_d_2 = g_d[g_d.duplicated(subset=['city_name'])]
        g_d = g_d.drop(g_d_2.index)
        g_d_2['city_name'] = g_d_2['city_name'] + '_' + g_d_2.index.map(str)
        g_d = g_d.append(g_d_2)
    GADM_file = GADM_file.drop(g_d.index).append(g_d).loc[GADM_file.city_name.apply(type) == str]
    print('-----')
    print('GADM duplicates after cleaning:')
    print(GADM_file.loc[GADM_file.duplicated()])
    print('country name: ', country_name)

    return(GADM_file)


def prepare_GADM(GADM_file, local_crs):
    '''
        Reproject gadm to local crs and creates several transformations of the boundary
        encoded as WKT strings.

        Returns: gpd.GeoDataFrame
    '''
    GADM_file['boundary_GADM_WGS84'] = GADM_file.geometry.apply(lambda x: x.wkt)

    GADM_file_local = GADM_file.to_crs(local_crs)

    GADM_file_local['boundary_GADM'] = GADM_file_local.geometry.apply(lambda x: x.wkt)
    GADM_file_local['boundary_GADM_500m_buffer'] = GADM_file_local.geometry.buffer(500).apply(lambda x: x.wkt)
    GADM_file_local['boundary_GADM_2k_buffer'] = GADM_file_local.geometry.buffer(2000).apply(lambda x: x.wkt)

    GADM_file_local = GADM_file_local.drop(columns=['geometry'])

    print('GADM prepared.')

    return(GADM_file_local)


def city_paths_from_gadm(path_db_folder,country,GADM_file):
    if country in ['cyprus', 'ireland']:
        return [os.path.join(path_db_folder, country, city) for city in GADM_file.city_name]
    else:
        return [os.path.join(path_db_folder, country, region, city) for region, city in zip(
                                                                                    GADM_file.region_name,
                                                                                    GADM_file.city_name)]


def create_folders(list_city_paths):
    for city_path in list_city_paths:
        Path(city_path).mkdir(parents=True, exist_ok=False)


def city_paths_to_txt(city_paths,country,path_db_folder):
    city_names = [el.rsplit('/')[-1] for el in city_paths]
    city_paths_full = [os.path.join(path, city) for path, city in zip(city_paths, city_names)]   
    
    path_file = os.path.join(path_db_folder, country, f"paths_{country}.txt")
    if os.path.isfile(path_file): 
        add_paths_to_file(city_paths_full,path_file,country,path_db_folder)
    else:
        write_to_file(path_file,city_paths_full,'w')


def add_paths_to_file(city_paths_full,path_file,country,path_db_folder):
    city_paths_exist = ufo_helpers.get_all_paths(country, path_root_folder=path_db_folder)
    city_paths_to_add = [path for path in city_paths_full if path not in city_paths_exist]
    if city_paths_to_add:
        write_to_file(path_file,city_paths_to_add,'a')


def write_to_file(path_file,city_paths,mode):
    # mode 'w' overwrites existing content, 'a' appends content
    with open(path_file,mode) as f:
        for element in city_paths:
            f.write(element + "\n")


def create_city_boundary_files(GADM_file,
                               country_name,
                               list_city_paths):
    '''
        Creates for all cities in a country based on GADM a csv with to store city-level
        information about a city together with its geometry

        Returns: None

         !!TODO!!:
        * add support for incomplete OSM countries
    '''
    for city_path in list_city_paths:
        city = GADM_file[GADM_file.city_name == os.path.split(city_path)[-1]]
        city.to_csv(city_path + '_boundary.csv', index=False)

    print('City boundary files created.')


def create_city_bldg_geom_files(gdf_bldg,
                                gadm_file,
                                list_city_paths,
                                only_region,
                                part
                                ):
    '''
        Matches a processed building footprints dataset with GADM city boundaries and saves two csv with geom/id for
        within the boundary of the city, and within a buffer of 500 m around the city.

        If the dataset covers less than a full country, the only_region parameter filters the relevant cities from the
        region.

        Returns:
        * gpd.GeoDataframe with id and city name but without geometry (for later matching of the attributes)
        * number of buildings at beginning and end as integers

         !!TODO!!:
        * add support for importing neighboring regions within a country for getting buildings within buffer.
        * add support for incomplete OSM countries
    '''

    # adjust TODO!

    # counting initial num bldgs and dropping duplicates
    n_bldg_start = len(gdf_bldg)
    gdf_bldg = remove_dupls(gdf_bldg, 'bldg geoms', 'id')
    # remove invalid geoms from gdf
    n_invalid = len(gdf_bldg.loc[~gdf_bldg.is_valid])
    gdf_bldg = gdf_bldg.loc[gdf_bldg.is_valid]
    n_bldg_start2 = len(gdf_bldg)

    # alocate bldgs on boundaries based on max intersecting area
    # prepare GADM file for intersecting with gdf_bldg
    gdf_gadm_file = gadm_file[['city_name', 'boundary_GADM']].set_geometry(
        gadm_file['boundary_GADM'].apply(wkt.loads)).set_crs(CRS_UNI)
    # append to gdf_bldg
    gdf_bldg['city_name'] = get_city_per_bldg(gdf_bldg, gdf_gadm_file)
    # do second sjoin on larger buffer
    gdf_bldg_buff = gpd.sjoin(gdf_bldg, gadm_file[['city_name', 'boundary_GADM_500m_buffer']].set_geometry(
        gadm_file['boundary_GADM_500m_buffer'].apply(wkt.loads)).set_crs(gadm_file.crs)).drop(columns=['index_right'])
    # be aware that at this stage gdf_bldg.city_name_left contais nan values
    # for bldgs that do not intersect with any gadm bound

    print('Num bldgs after sjoin: {}'.format(len(gdf_bldg)))

    if only_region is not None:
        list_city_paths = [path for path in list_city_paths if only_region in path]
        list_city_names = [os.path.split(city_path)[-1] for city_path in list_city_paths]
        discarded_cities = [city_name for city_name in set(gdf_bldg.city_name) if city_name not in list_city_names]
        if discarded_cities != []:
            print(f'Discarded buildings from cities: {discarded_cities}')

    else:
        list_city_names = [os.path.split(city_path)[-1] for city_path in list_city_paths]

    n_bldg_end = 0

    print('Creating building geom city files...')
    # intialise list with cities where we don't have bldgs
    list_saved_cities = []
    list_saved_paths = []
    for city_name, city_path in zip(list_city_names, list_city_paths):

        # take only bldgs of city
        city = gdf_bldg.loc[gdf_bldg.city_name == city_name]

        # if bldgs in city
        if len(city) != 0:
            # take all bldgs that intersect with city buffer
            city_plus_buffer = gdf_bldg_buff.loc[gdf_bldg_buff.city_name_right == city_name]
            # remove all bldgs that are within city bounds
            buffer_area = city_plus_buffer.loc[city_plus_buffer.city_name_left != city_name]

            n_bldg_end += len(city)

            # save bldgs of city and bldgs in buffer
            ufo_helpers.save_csv_wkt(city[['id', 'geometry']], f'{city_path}_geom_{part}.csv')
            ufo_helpers.save_csv_wkt(buffer_area[['id', 'geometry']], f'{city_path}_buffer_{part}.csv')
            list_saved_cities.append(city_name)
            list_saved_paths.append(city_path)

    # remove all bldgs that did not match with any gadm bound (either nan or not in list city names)
    gdf_bldg_out = gdf_bldg.loc[gdf_bldg.city_name.isin(list_city_names)].drop(columns='geometry')

    print('--')
    print('Chunk num bldgs: {}'.format(n_bldg_start))
    print('Num removed invalid geoms: {}'.format(n_invalid))
    print('Chunk num after remove dupls & invalid geoms: {}'.format(n_bldg_start2))
    print('Num bldgs in gdf: {}, Num bldgs outside of gadm: {}, Num bldgs allocated to cities: {}'.format(
        len(gdf_bldg_out), len(gdf_bldg) - len(gdf_bldg_out), n_bldg_end))
    print('Geoms created in {} cities.'.format(len(list_saved_cities)))
    print('--')

    return(gdf_bldg_out, list_saved_cities, list_saved_paths, n_bldg_start2, n_bldg_end)


def create_city_bldg_attrib_files(gdf_bldgs,
                                  bldg_attrib,
                                  file_type,
                                  list_saved_names,
                                  list_saved_paths,
                                  part):
    '''
        Matches a processed building attributes dataset by ids with GADM city names and saves a csv with
        ids and attributes.

        Returns: None

        !!TODO!!:
            * add support for importing neighboring regions within a country for getting buildings within buffer.
            * add support for incomplete OSM countries
    '''
    print(f'Creating {file_type} files per city...')

    # merge on id per chunk
    bldg_attrib = bldg_attrib.merge(gdf_bldgs, on='id', validate='1:1')
    raise_if_inconsistent(gdf_bldgs, bldg_attrib, file_type)

    for city_name, city_path in zip(list_saved_names, list_saved_paths):
        # save in parts
        bldg_attrib_city = bldg_attrib[bldg_attrib.city_name == city_name].drop(columns='city_name')
        bldg_attrib_city.to_csv(f'{city_path}_{file_type}_{part}.csv', index=False)

    print('City attributes created for {} bldgs'.format(len(bldg_attrib)))


def create_source_files(gdf_bldgs,
                        df_sources,
                        list_saved_names,
                        list_saved_paths,
                        part):
    '''
        Matches a processed building attributes sources dataset by ids with GADM city names and saves a csv with
        ids and sources for geom, attribs and extra attribs.

        Returns: None

        !!TODO!!:
            * add support for importing neighboring regions within a country for getting buildings within buffer.
    '''
    print('Creating attribute source files per city...')
    # merge chunk with df_source on id
    df_sources = df_sources.merge(gdf_bldgs, on='id', validate='1:1')
    raise_if_inconsistent(gdf_bldgs, df_sources, 'attrib_source')

    # save per city
    for city_name, city_path in zip(list_saved_names, list_saved_paths):
        df_sources_city = df_sources[df_sources.city_name == city_name].drop(columns='city_name')
        df_sources_city.to_csv(f'{city_path}_attrib_source_{part}.csv', index=False)

    print('City attribute sources created for {} bldgs'.format(len(df_sources)))


def raise_if_inconsistent(gdf_1, gdf_2, file_type):
    if len(gdf_1) != len(gdf_2):
        raise ValueError(f'Num of bldgs in geometry file is not equivalent to num bldgs in correspdoning {file_type} files')


def get_stats(country_name, dataset_name, n_bldg_start, n_bldg_end, end, list_0_cities, num_stats):
    '''
    Reports stats on the run.

    Returns: pd.DataFrame
    '''

    # get time
    h_div = divmod(end, 3600)
    m_div = divmod(h_div[1], 60)

    stats = {}
    stats['country'] = country_name
    stats['dataset_name'] = dataset_name
    stats['n_bldg_start'] = n_bldg_start
    stats['n_bldg_end'] = n_bldg_end
    stats['duration'] = '{} h {} m {} s'.format(h_div[0], m_div[0], round(m_div[1], 0))
    stats['date'] = date.today().strftime("%d/%m/%Y")
    stats['saved_cities'] = [list_0_cities]
    stats['n_attrib_pre_dupl'] = num_stats['num_attrib0']
    stats['n_attrib_post_dupl'] = num_stats['num_attrib1']
    stats['n_x_attrib_pre_dupl'] = num_stats['num_x_attrib0']
    stats['n_x_attrib_post_dupl'] = num_stats['num_x_attrib1']

    return(pd.DataFrame(stats, index=['0']))


def merge_parts(path):
    '''
        Merges files created in chunks for one city and saved as part1, part2 etc.
        Inspects the number of parts, if 1 then renames, if several, concatenate
        and renames.

        Returns: None
    '''

    path_glob = path + '*'
    path_parts = glob.glob(path_glob)

    # count num files before merge and delete
    num_files_pre = len([p1 for p1 in path_parts if '_geom' in p1])

    # initialise
    bool_merge = False
    # go through all possible endings; Note: extra_attrib has to come before! _attrib to avoid mistakes here!
    for ending in ['_geom_', '_buffer_', '_attrib_', '_extra_attrib_', '_attrib_source_']:
        paths_ending = [p for p in path_parts if ending in p]

        if ending == '_attrib_':
            paths_ending = [k for k in paths_ending if 'extra' not in k]
            paths_ending = [k for k in paths_ending if 'source' not in k]

        if len(paths_ending) == 0:
            pass

        if len(paths_ending) == 1:
            os.rename(paths_ending[0], path + ending[:-1] + '.csv')
            bool_merge = True

        if len(paths_ending) > 1:
            df = pd.DataFrame()
            for p in paths_ending:
                df = df.append(pd.read_csv(p))
            # save appended files
            df.to_csv(path + ending[:-1] + '.csv', index=False)
            # remove all part files
            for pr in paths_ending:
                os.remove(pr)
            bool_merge = True

    path_parts_post = glob.glob(path_glob)
    num_files_post = len([p2 for p2 in path_parts_post if '_geom' in p2])
    str_city_name = path.rsplit('/', 1)[1]

    # check that we didn't do something stupid an increase file size
    if num_files_pre < num_files_post:
        raise ValueError(
            '{}: Num parts before: {} > Num parts after: {}'.format(
                str_city_name, num_files_pre, num_files_post))

    return bool_merge


def merge(list_saved_paths, country, path_db_folder='/p/projects/eubucco/data/2-database-city-level'):
    """
    Based on list saved paths OR the country name, this functions calls "merge_parts"
    either for all saved paths or for the whole country
    """
    counter = 0

    if list_saved_paths:
        all_paths = list_saved_paths
    else:
        all_paths = ufo_helpers.get_all_paths(country, path_root_folder=path_db_folder)

    for path in all_paths:
        bool_merge = False
        # print(path)
        bool_merge = merge_parts(path)
        if bool_merge:
            counter += 1
    print('Successfully merged {} paths! Closing run.'.format(counter))


# HIGH


def db_set_up(country,
            dataset_name,
            path_db_folder,
            chunksize=int(5E5),
            only_region=None,
            overwrite=False,
            boundaries=True,
            bldgs=True,
            path_stats='/p/projects/eubucco/stats/2-db-set-up',
            path_inputs_parsing='/p/projects/eubucco/git-eubucco/database/preprocessing/1-parsing/inputs-parsing.csv',
            path_int_fol='/p/projects/eubucco/data/1-intermediary-outputs',
            ):
    '''

        Sets up the EUBUCCO database structure from (i) GADM city boundaries, (ii) parsed country/region/city-level
        building geometry, attributes and extra attributes.

        Creates a folder structure country/region/city with the following files: <city>_boundary.csv, <city>_geoms.csv,
        <city>_buffer.csv, <city>_attribs.csv and if relevant <city>_extra_attribs.csv

        Folder and boundary files creation can be deactivated via the relevant function arguments.

        update 02.02.2022, A: Felix:
        a) dataset_name ='': open inputs_parsing_csv for countryname and loops through all files related to that country

        b) 'sepa mode': depreciated

        Returns: None
    '''
    start = time.time()

    # get file gadm, import gadm, get proper crs, get country info
    GADM_file, country_name, level_city, _ = fetch_GADM_info_country(country)
    GADM_file = clean_GADM_city_names(GADM_file, country_name, level_city)
    GADM_file = prepare_GADM(GADM_file, CRS_UNI)

    # only take gadm bounds and cities from dataset_name
    gadm_bounds_dataset = mask_gadm(GADM_file, dataset_name, country_name, path_inputs_parsing)
    city_paths_dataset = city_paths_from_gadm(path_db_folder,country_name,gadm_bounds_dataset)

    if not overwrite:
        if os.path.isdir(city_paths_dataset[0]):
            raise Exception(f'Folders for {dataset_name} already exist. Aborting...')
        create_folders(city_paths_dataset)
        city_paths_to_txt(city_paths_dataset,country,path_db_folder)

    if boundaries:
        create_city_boundary_files(gadm_bounds_dataset, country_name, city_paths_dataset)

    if bldgs:
        n_bldg_start_sum = 0
        n_bldg_end_sum = 0
        list_saved_names_sum = []
        list_saved_paths_sum = []

        # reading in gov geoms as chunks
        geom_filename = os.path.join(path_int_fol, country_name, dataset_name + '-3035_geoms.csv')
        chunks = pd.read_csv(geom_filename, chunksize=chunksize)

        # reading in attribs and x-attribs files; checking for duplicates
        df_bldg_attrib, df_bldg_x_attrib, dict_num_attribs = get_attribs(
            path_int_fol, country_name, dataset_name)
        try:
            source_filename = os.path.join(path_int_fol, country_name, dataset_name + '_attrib_sources.csv')
            df_sources = pd.read_csv(source_filename)
        except BaseException:
            df_sources = pd.DataFrame()

        if df_sources.empty:
            print('no_source file found - creating new one')
            df_sources = create_new_df_source(df_bldg_attrib, dataset_name)
        else:
            df_sources['dataset_name'] = dataset_name
            print('Checking for duplicates in source files')
            df_sources = remove_dupls(df_sources, 'df_sources', 'id')


        for idx, chunk in enumerate(chunks):
            print('-----')
            print('chunk: ', idx)

            gdf = gpd.GeoDataFrame(chunk, geometry=chunk['geometry'].apply(wkt.loads), crs=CRS_UNI)

            gdf_bldgs, list_saved_names, list_saved_paths, n_bldg_start, n_bldg_end = create_city_bldg_geom_files(
                gdf, gadm_bounds_dataset, city_paths_dataset, only_region, idx)

            create_city_bldg_attrib_files(gdf_bldgs,
                                        df_bldg_attrib,
                                        'attrib',
                                        list_saved_names,
                                        list_saved_paths,
                                        idx)
            if not df_bldg_x_attrib.empty:
                create_city_bldg_attrib_files(gdf_bldgs,
                                            df_bldg_x_attrib,
                                            'extra_attrib',
                                            list_saved_names,
                                            list_saved_paths,
                                            idx)
            create_source_files(gdf_bldgs, df_sources, list_saved_names, list_saved_paths, idx)

            n_bldg_start_sum += n_bldg_start
            n_bldg_end_sum += n_bldg_end

            # collect all city names & paths where we have bldgs
            list_saved_names_sum.extend(list_saved_names)
            list_saved_paths_sum.extend(list_saved_paths)

        # calculate end time
        end = time.time() - start

        # calculate stats
        df_stats = get_stats(country_name,
                            dataset_name,
                            n_bldg_start_sum,
                            n_bldg_end_sum,
                            end,
                            list_saved_names_sum,
                            dict_num_attribs)

        # save stats file
        Path(path_stats).mkdir(parents=True, exist_ok=False)
        df_stats.to_csv(os.path.join(path_stats, dataset_name + '_stat.csv'), index=False)

        print(df_stats.iloc[0])
        print('----------------')
        print('saved all geom files in gadm folders & saved stats files')
        print('################')

        # merge chunks for processed cities where we saved bldgs
        print('merging all paths')
        merge(list_saved_paths_sum, None, path_db_folder)
