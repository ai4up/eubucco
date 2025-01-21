import logging
import os
from pathlib import Path

import pandas as pd
import geopandas as gpd
import numpy as np

from preproc.attribs import _encode_missing_in_string_columns

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def merge_gov_osm_msft(region_id: str, gov_dir: str, osm_dir: str, msft_dir: str, out_dir: str) -> None:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{region_id}.parquet"

    if out_path.is_file():
        logger.info(f'Gov, OSM, and MSFT data already merged for {region_id}.')
        return

    gov_path = os.path.join(gov_dir, f"{region_id}.parquet")
    osm_path = os.path.join(osm_dir, f"{region_id}.parquet")
    msft_path = _find_file(msft_dir, f"{region_id}.gpkg")

    gov = _read_geodata(gov_path).to_crs(3035)
    osm = _read_geodata(osm_path).to_crs(3035)
    msft = _read_geodata(msft_path).to_crs(3035)
    msft = _encode_missing_in_string_columns(msft)

    gov['dataset'] = 'gov'
    osm['dataset'] = 'osm'
    msft['dataset'] = 'msft'

    gov_osm = merge_building_datasets(gov, osm, fillna=True)
    gov_osm_msft = merge_building_datasets(gov_osm, msft, fillna=False)

    gov_osm_msft.to_parquet(out_path)


def merge_building_datasets(gdf1: gpd.GeoDataFrame, gdf2: gpd.GeoDataFrame, fillna: bool) -> gpd.GeoDataFrame:
    if gdf1.empty:
        return gdf2

    if gdf2.empty:
        return gdf1

    # determine intersecting buildings
    int_idx2, int_idx1 = gdf1.sindex.query(gdf2.geometry, predicate='intersects')

    # assess degree of overlap
    gdf1_int = gdf1.iloc[int_idx1]
    gdf2_int = gdf2.iloc[int_idx2]
    geoms1 = gdf1_int.geometry.reset_index()
    geoms2 = gdf2_int.geometry.reset_index()
    ioa = _intersection_to_area_ratio(geoms1, geoms2)
    gdf2_int['ioa'] = ioa.values

    # add new non-intersecting buildings
    non_intersecting = gdf2.drop(gdf2_int.index)

    # add new slightly intersecting buildings
    gdf2_largest_int = gdf2_int.sort_values('ioa', ascending=False).drop_duplicates(keep='first')
    intersecting_below_thresh = gdf2_largest_int[gdf2_largest_int['ioa'] < 0.2]

    # use gdf2 to fill missing attributes in gdf1
    if fillna:
        gdf2_overlapping = gdf2_int[gdf2_int['ioa'] > 0.8]
        gdf2_overlapping.index = gdf1_int.index[ioa > 0.8]

        matched_type = _most_frequent_category(gdf2_overlapping['type'])
        matched_height = gdf2_overlapping.groupby(level=0)['height'].mean()
        matched_age = gdf2_overlapping.groupby(level=0)['age'].mean()

        type_missings = gdf1['type'].isna()
        height_missings = gdf1['height'].isna()
        age_missings = gdf1['age'].isna()

        gdf1['type'] = gdf1['type'].fillna(matched_type)
        gdf1['height'] = gdf1['height'].fillna(matched_height)
        gdf1['age'] = gdf1['age'].fillna(matched_age)

        gdf1['filled_type'] = type_missings & gdf1['type'].notna()
        gdf1['filled_height'] = height_missings & gdf1['height'].notna()
        gdf1['filled_age'] = age_missings & gdf1['age'].notna()

    return pd.concat([gdf1, non_intersecting, intersecting_below_thresh])


def _find_file(data_dir: str, pattern: str) -> Path:
    try:
        return next(Path(data_dir).rglob(pattern))
    except StopIteration:
        logger.warning(f"File {pattern} could not be found in {data_dir}.")
        return None


def _read_geodata(path: str) -> gpd.GeoDataFrame:
    try:
        if str(path).endswith('.parquet'):
            return gpd.read_parquet(path)
        else:
            return gpd.read_file(path)

    except FileNotFoundError:
        return gpd.GeoDataFrame(geometry=[], crs=3035)


def _most_frequent_category(s: pd.Series) -> pd.Series:
    mode = s.groupby(level=0).apply(pd.Series.mode)
    single_mode = mode.groupby(level=0).first()  # alternatively: get mode of larger building

    return single_mode


def _intersection_to_area_ratio(s1: gpd.GeoSeries, s2: gpd.GeoSeries) -> pd.Series:
    intersection = s1.intersection(s2).area
    area = np.minimum(s1.area, s2.area)

    return intersection / area
