import os
import time

import pandas as pd
import numpy as np

import ufo_map.Utils.helpers as ufo_helpers
import ufo_map.Feature_engineering.buildings as ufo_buildings
import ufo_map.Feature_engineering.blocks as ufo_blocks
import ufo_map.Feature_engineering.streets as ufo_streets
import ufo_map.Feature_engineering.city_level as ufo_city

CRS_UNI = 'EPSG:3035'


def create_features(city_path,
                    bld=True,
                    blk=True,
                    bld_d=True,
                    blk_d=True,
                    int_=True,
                    str_=True,
                    sbb_=True,
                    city_level=True,
                    path_stats='/p/projects/eubucco/stats/5-ft-eng'
                    ):
    '''
        Creates features and saves them as csv files <city>_<ft-type>_fts.csv

        Returns: None

        TODO:
                * improve the ufo_helpers.write_stats outputs locations/names
                * add hot start option for bld_d and blk_d
    '''

    start = time.time()

    paths = {}
    for file in ['geom', 'buffer', 'bld_fts', 'bld_d_fts', 'block_fts', 'block_d_fts', 'intersections', 'int_fts',
                 'streets', 'str_fts', 'sbb', 'sbb_fts', 'lu_fts', 'city_level_fts']:
        paths[file] = f'{city_path}_{file}.csv'

    city_name = os.path.split(list(paths.values())[0])[-2]
    print(city_name)

    buildings = ufo_helpers.import_csv_w_wkt_to_gdf(paths['geom'], CRS_UNI)
    buffer_ = ufo_helpers.import_csv_w_wkt_to_gdf(paths['buffer'], CRS_UNI)
    indexes_ = buildings.index
    buildings = buildings.append(buffer_).reset_index(drop=True)

    city_fts = pd.DataFrame(index=[0])

    if bld and not os.path.isfile(paths['bld_fts']):
        building_fts = buildings.merge(ufo_buildings.features_building_level(buildings), on='id')
        building_fts.loc[indexes_].drop(columns=['geometry']).to_csv(paths['bld_fts'], index=False)
        city_fts = pd.concat((city_fts, ufo_city.features_city_level_buildings(buildings.loc[indexes_])), axis=1)

    if bld_d and not os.path.isfile(paths['bld_d_fts']):
        building_fts = ufo_buildings.features_buildings_distance_based(buildings, building_fts)
        building_fts.loc[indexes_].to_csv(paths['bld_d_fts'], index=False)

    if blk and not os.path.isfile(paths['block_fts']):
        block_fts = buildings.merge(ufo_blocks.features_block_level(buildings), on='id')
        block_fts.loc[indexes_].drop(columns=['geometry']).to_csv(paths['block_fts'], index=False)
        city_fts = pd.concat((city_fts, ufo_city.features_city_level_blocks(block_fts.loc[indexes_])), axis=1)

    if blk_d and not os.path.isfile(paths['block_d_fts']):
        block_fts = ufo_blocks.features_blocks_distance_based(buildings, block_fts)
        block_fts.loc[indexes_].to_csv(paths['block_d_fts'], index=False)

    buildings = buildings.loc[indexes_]

    if int_ and not os.path.isfile(paths['int_fts']):
        intersections = ufo_helpers.import_csv_w_wkt_to_gdf(paths['intersections'], CRS_UNI)
        fts_int = buildings[['id']]
        fts_int['dist_to_closest_int'] = ufo_streets.feature_distance_to_closest_intersection(
            np.array(buildings.geometry), np.array(intersections.geometry), intersections.sindex)
        fts_int = pd.concat((fts_int, ufo_streets.feature_intersection_count_within_buffer(
            np.array(buildings.geometry), intersections.sindex)), axis=1)
        fts_int.to_csv(paths['int_fts'], index=False)
        city_fts = pd.concat((city_fts, ufo_city.feature_city_level_intersections(intersections)), axis=1)

    if str_ and not os.path.isfile(paths['str_fts']):
        streets = ufo_helpers.import_csv_w_wkt_to_gdf(paths['streets'], CRS_UNI)
        fts_str = buildings[['id']]
        fts_str = pd.concat((fts_str, ufo_streets.features_closest_street(buildings, streets)), axis=1)
        fts_str = pd.concat((fts_str, ufo_streets.features_street_distance_based(buildings, streets)), axis=1)
        fts_str.to_csv(paths['str_fts'], index=False)
        city_fts = pd.concat((city_fts, ufo_city.features_city_level_streets(streets)), axis=1)

    if sbb_ and not os.path.isfile(paths['sbb_fts']):
        sbb = ufo_helpers.import_csv_w_wkt_to_gdf(paths['sbb'], CRS_UNI)
        fts_sbb = buildings[['id']]
        fts_sbb = pd.concat((fts_sbb, ufo_streets.features_own_sbb(buildings, sbb)), axis=1)
        fts_sbb = pd.concat((fts_sbb, ufo_streets.features_sbb_distance_based(buildings, sbb)), axis=1)
        fts_sbb.to_csv(paths['sbb_fts'], index=False)
        city_fts = pd.concat((city_fts, ufo_city.features_city_level_sbb(sbb)), axis=1)

    if city_level and not os.path.isfile(paths['city_level_fts']):
        print('Generating city-level features...')
        city_fts.to_csv(paths['city_level_fts'], index=False)

    duration = time.time() - start
    stats = {'city_path': city_path}
    filename = f'{city_path}_stat-parts'
    ufo_helpers.write_stats(stats, duration, path_stats, filename)
