import os
import time

import pandas as pd
import numpy as np

from ufo_map.Utils.helpers import import_csv_w_wkt_to_gdf, get_all_paths, write_stats
from ufo_map.Feature_engineering.buildings import features_building_level, features_buildings_distance_based
from ufo_map.Feature_engineering.blocks import features_block_level, features_blocks_distance_based
from ufo_map.Feature_engineering.streets import *
from ufo_map.Feature_engineering.city_level import *

CRS_UNI = 'EPSG:3035'


def create_features(country_name,
                    city_idx,
                    bld=False,
                    blk=False,
                    bld_d=False,
                    blk_d=False,
                    int_=False,
                    str_=False,
                    sbb_=False,
                    city_level=False,
                    left_over=False,
                    ua_mode=False,
                    path_stats='/p/projects/eubucco/stats/5-ft-eng',
                    data_dir='/p/projects/eubucco/data/2-database-city-level-v0_1'
                    ):
    '''
        Creates features and saves them as csv files <city>_<ft-type>_fts.csv

        Returns: None

        TODO:
                * improve the write_stats outputs locations/names
                * add hot start option for bld_d and blk_d
    '''

    start = time.time()

    paths = {}
    for file in ['geom', 'buffer', 'bld_fts', 'bld_d_fts', 'block_fts', 'block_d_fts', 'intersections', 'int_fts',
                 'streets', 'str_fts', 'sbb', 'sbb_fts', 'lu_fts', 'city_level_fts']:

        fts_paths_cities = get_all_paths(country_name, file, data_dir, left_over, ua_mode)
        paths[file] = fts_paths_cities[city_idx]

    city_name = os.path.split(list(paths.values())[0])[-2]
    print(city_name)

    buildings = import_csv_w_wkt_to_gdf(paths['geom'], CRS_UNI)
    buffer_ = import_csv_w_wkt_to_gdf(paths['buffer'], CRS_UNI)
    indexes_ = buildings.index
    buildings = buildings.append(buffer_).reset_index(drop=True)

    city_fts = pd.DataFrame(index=[0])

    if bld:
        building_fts = buildings.merge(features_building_level(buildings), on='id')
        building_fts.loc[indexes_].drop(columns=['geometry']).to_csv(paths['bld_fts'], index=False)
        city_fts = pd.concat((city_fts, features_city_level_buildings(buildings.loc[indexes_])), axis=1)

    if bld_d:
        building_fts = features_buildings_distance_based(buildings, building_fts)
        building_fts.loc[indexes_].to_csv(paths['bld_d_fts'], index=False)

    if blk:
        block_fts = buildings.merge(features_block_level(buildings), on='id')
        block_fts.loc[indexes_].drop(columns=['geometry']).to_csv(paths['block_fts'], index=False)
        city_fts = pd.concat((city_fts, features_city_level_blocks(block_fts.loc[indexes_])), axis=1)

    if blk_d:
        block_fts = features_blocks_distance_based(buildings, block_fts)
        block_fts.loc[indexes_].to_csv(paths['block_d_fts'], index=False)

    buildings = buildings.loc[indexes_]

    if int_:
        intersections = import_csv_w_wkt_to_gdf(paths['intersections'], CRS_UNI)
        fts_int = buildings[['id']]
        fts_int['dist_to_closest_int'] = feature_distance_to_closest_intersection(
            np.array(buildings.geometry), np.array(intersections.geometry), intersections.sindex)
        fts_int = pd.concat((fts_int, feature_intersection_count_within_buffer(
            np.array(buildings.geometry), intersections.sindex)), axis=1)
        fts_int.to_csv(paths['int_fts'], index=False)
        city_fts = pd.concat((city_fts, feature_city_level_intersections(intersections)), axis=1)

    if str_:
        streets = import_csv_w_wkt_to_gdf(paths['streets'], CRS_UNI)
        fts_str = buildings[['id']]
        fts_str = pd.concat((fts_str, features_closest_street(buildings, streets)), axis=1)
        fts_str = pd.concat((fts_str, features_street_distance_based(buildings, streets)), axis=1)
        fts_str.to_csv(paths['str_fts'], index=False)
        city_fts = pd.concat((city_fts, features_city_level_streets(streets)), axis=1)

    if sbb_:
        sbb = import_csv_w_wkt_to_gdf(paths['sbb'], CRS_UNI)
        fts_sbb = buildings[['id']]
        fts_sbb = pd.concat((fts_sbb, features_own_sbb(buildings, sbb)), axis=1)
        fts_sbb = pd.concat((fts_sbb, features_sbb_distance_based(buildings, sbb)), axis=1)
        fts_sbb.to_csv(paths['sbb_fts'], index=False)
        city_fts = pd.concat((city_fts, features_city_level_sbb(sbb)), axis=1)

    if city_level:
        print('Generating city-level features...')
        city_fts.to_csv(paths['city_level_fts'], index=False)

    duration = time.time() - start
    stats = {'city_idx': city_idx}
    filename = f'{city_idx}_stat-parts'
    write_stats(stats, duration, path_stats, filename)
