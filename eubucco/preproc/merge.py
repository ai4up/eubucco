import logging
import os
from pathlib import Path

import pandas as pd
import geopandas as gpd
import numpy as np
from pyogrio.errors import DataSourceError


logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def merge_gov_osm_msft(region_id: str, gov_dir: str, osm_dir: str, msft_dir: str, out_dir: str, db_version: str) -> None:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{region_id}.parquet"

    if out_path.is_file():
        logger.info(f'Gov, OSM, and MSFT data already merged for {region_id}.')
        return

    gov_path = os.path.join(gov_dir, f"{region_id}.parquet")
    osm_path = os.path.join(osm_dir, f"{region_id}.parquet")
    msft_path = os.path.join(msft_dir, f"{region_id}.parquet")

    gov = _read_geodata(gov_path).to_crs(3035)
    osm = _read_geodata(osm_path).to_crs(3035)
    msft = _read_geodata(msft_path).to_crs(3035)

    gov['dataset'] = 'gov'
    osm['dataset'] = 'osm'
    msft['dataset'] = 'msft'

    gov_osm = _merge_building_datasets(gov, osm, fillna=True)
    gov_osm_msft = _merge_building_datasets(gov_osm, msft, fillna=False)
    gov_osm_msft = _generate_unique_id(gov_osm_msft, db_version)

    gov_osm_msft.to_parquet(out_path)


def _merge_building_datasets(gdf1: gpd.GeoDataFrame, gdf2: gpd.GeoDataFrame, fillna: bool) -> gpd.GeoDataFrame:
    if gdf1.empty:
        return gdf2

    if gdf2.empty:
        return gdf1

    matching, non_matching, _ = _match_building_datasets_based_on_overlap(gdf1, gdf2)

    if fillna:
        # use gdf2 to fill missing attributes in gdf1
        gdf1 = _fill_missing_attributes(gdf1, matching)

    return pd.concat([gdf1, non_matching], ignore_index=True)


def _match_building_datasets_based_on_overlap(gdf1: gpd.GeoDataFrame, gdf2: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    # determine intersecting buildings
    int_idx2, int_idx1 = gdf1.sindex.query(gdf2.geometry, predicate='intersects')

    # assess degree of overlap
    gdf1_int = gdf1.iloc[int_idx1]
    gdf2_int = gdf2.iloc[int_idx2]
    geoms1 = gdf1_int.geometry.reset_index()
    geoms2 = gdf2_int.geometry.reset_index()
    ioa = _intersection_to_area_ratio(geoms1, geoms2)
    gdf2_int['ioa'] = ioa.values

    # select matching buildings based on large intersection
    intersecting = gdf2_int[gdf2_int['ioa'] > 0.8]
    intersecting.index = gdf1_int.index[ioa > 0.8]

    # select non-matching buildings (with no intersection)
    non_intersecting = gdf2.drop(gdf2_int.index)

    # select non-matching buildings (with slight intersection)
    gdf2_largest_int = gdf2_int.sort_values('ioa', ascending=False).drop_duplicates(subset=['id'], keep='first')
    intersecting_below_thresh = gdf2_largest_int[gdf2_largest_int['ioa'] < 0.2]

    matching = intersecting
    non_matching = pd.concat([non_intersecting, intersecting_below_thresh])
    unsure = gdf2.drop(matching.index).drop(non_matching.index)

    return matching, non_matching, unsure


def _fill_missing_attributes(gdf: gpd.GeoDataFrame, matching: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    # aggregate attributes for 1:n matches
    matched_type = _most_frequent_category(matching['type'])
    matched_height = matching.groupby(level=0)['height'].mean()
    matched_age = matching.groupby(level=0)['age'].mean()

    type_missings = gdf['type'].isna()
    height_missings = gdf['height'].isna()
    age_missings = gdf['age'].isna()

    gdf['type'] = gdf['type'].fillna(matched_type)
    gdf['height'] = gdf['height'].fillna(matched_height)
    gdf['age'] = gdf['age'].fillna(matched_age)

    gdf['filled_type'] = type_missings & gdf['type'].notna()
    gdf['filled_height'] = height_missings & gdf['height'].notna()
    gdf['filled_age'] = age_missings & gdf['age'].notna()

    return gdf


def _read_geodata(path: str) -> gpd.GeoDataFrame:
    try:
        if str(path).endswith('.parquet'):
            return gpd.read_parquet(path)
        else:
            return gpd.read_file(path)

    except (FileNotFoundError, DataSourceError):
        logger.warning(f"File {path} could not be found.")
        return gpd.GeoDataFrame(geometry=[], crs=3035)


def _most_frequent_category(s: pd.Series) -> pd.Series:
    mode = s.groupby(level=0).apply(pd.Series.mode)
    single_mode = mode.groupby(level=0).first()  # alternatively: get mode of larger building

    return single_mode


def _intersection_to_area_ratio(s1: gpd.GeoSeries, s2: gpd.GeoSeries) -> pd.Series:
    intersection = s1.intersection(s2).area
    area = np.minimum(s1.area, s2.area)

    return intersection / area


def _generate_unique_id(df: gpd.GeoDataFrame, db_version: str) -> gpd.GeoDataFrame:
    df['id_source'] = df['id']
    df['id'] = (
        'v' + str(db_version) + '-' +
        df['LAU_ID'] + '-' +
        df.groupby('LAU_ID').cumcount().astype(str)
    )

    return df
