import logging
import json
from pathlib import Path
from typing import Dict

import pandas as pd
import geopandas as gpd
import numpy as np
from pandas.api.types import CategoricalDtype

from utils.load import all_files

FLOOR_HEIGHT = 3  # meter

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def attrib_cleaning(data_dir: str, out_dir: str, dataset_type: str, type_mapping_path: str = None, source_mapping_path: str = None, file_pattern: str = None) -> None:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    for f in all_files(data_dir, file_pattern):
        try:
            out_path = out_dir / f"{f.stem}.parquet"

            if out_path.is_file():
                logger.info(f'Attributes already cleaned for {f.name}...')
                continue

            logger.info(f'Cleaning attributes for {f.name}...')
            df = _read_geodata(f)
            df = _add_source_dataset_col(df, source_mapping_path, dataset_type)

            if dataset_type == 'msft':
                df = msft_height_cleaning(df)
            else:
                df = type_cleaning(df)
                df = type_mapping(df, type_mapping_path)
                df = age_cleaning(df)
                df = height_cleaning(df)
                df = floors_cleaning(df)

            df = _remove_duplicates(df)
            df = _remove_non_building_structures(df)
            df = _encode_missing_in_string_columns(df)
            df.to_parquet(out_path)

        except Exception:
            logger.exception(
                f'Exception occurred while cleaning attributes for file {f.name}. Skipping {f.name} and continuing...')


def _remove_duplicates(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    '''Drop duplicates, keeping the row with the least NaN values'''
    df['attr_nan_count'] = df[['height', 'type', 'age']].isna().sum(axis=1)
    df = df.sort_values(by='attr_nan_count', ascending=True)

    df = df.drop_duplicates(subset=['id'], keep='first')
    df = df.drop_duplicates(subset=['geometry'], keep='first')

    df = df.drop(columns=['attr_nan_count'])

    return df


def _add_source_dataset_col(df: gpd.GeoDataFrame, source_mapping_path: str, dataset_type: str) -> gpd.GeoDataFrame:
    if dataset_type in ['osm', 'msft']:
        df['source_dataset'] = dataset_type
    else:
        with open(source_mapping_path, 'r') as f:
            region_mapping = json.load(f)
            source_file_mapping = {v: k for k, vs in region_mapping.items() for v in vs}

        df['source_dataset'] = 'gov-' + df['source_file'].map(source_file_mapping)

    return df


def _remove_non_building_structures(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    '''Remove non-building structures based on type_source column'''
    non_bldg_types = [
        'Tiefgarage',
        'Gebäude zur Versorgung;Tiefgarage',
        '31001_2465',
    ]

    len1 = len(df)
    df = df[~df['type_source'].isin(non_bldg_types)]
    df = df[~df['type_source'].str.startswith('5300', na=False)]  # German ALKIS Code for traffic areas
    len2 = len(df)
    logger.info(f'Removed {len1-len2} non-building structures based on type_source column.')

    return df


def msft_height_cleaning(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    df['height'] = _to_numeric(df['height'].replace(-1, np.nan))
    df['height'] = df['height'].replace(0, np.nan)

    return df


def height_cleaning(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    df['height'] = _to_numeric(df['height'])
    df['height'] = df['height'].replace(0, np.nan)
    df = _estimate_height_from_floors(df)

    return df


def floors_cleaning(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    df['floors'] = _to_numeric(df['floors'])
    df['floors'] = df['floors'].replace(0, np.nan)

    return df


def age_cleaning(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    df['age'] = df['age'].dropna().astype(str).apply(_extract_year)
    df['age'] = df['age'].replace(0, np.nan)

    return df


def type_cleaning(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    df['type_source'] = df['type_source'].astype('string')

    return df


def type_mapping(df: gpd.GeoDataFrame, type_mapping_path: str) -> gpd.GeoDataFrame:
    bldg_types = pd.read_csv(type_mapping_path)
    bldg_types['type_source'] = bldg_types['type_source'].astype('string')
    regional_types = bldg_types[bldg_types['source_datasets'].apply(lambda x: bool(set(x.split(',')) & set(df['source_dataset'].unique())))]

    type_mapping = regional_types.set_index('type_source')['type'].to_dict()
    res_type_mapping = regional_types.set_index('type_source')['residential_type'].to_dict()

    df['type'] = _harmonize_type(df['type_source'], type_mapping)
    df['residential_type'] = _harmonize_type(df['type_source'], res_type_mapping)

    return df


def _read_geodata(path: Path) -> gpd.GeoDataFrame:
    if 'parquet' in path.suffix:
        return gpd.read_parquet(path)
    elif 'gpkg' in path.suffix:
        return gpd.read_file(path)
    else:
        raise ValueError(f'Unsupported file format: {path.suffix}')


def _estimate_height_from_floors(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    df['height_source'] = df['height']
    df['height_source'] = df['height_source'].fillna('floors')
    df['height'] = df['height'].fillna(df['floors'] * FLOOR_HEIGHT)

    return df


def _harmonize_type(source_type: pd.Series, type_mapping: Dict[str, str]) -> pd.Series:
    '''Maps buildings types from the source dataset to harmonized types for each building in a city.'''
    types = set(type_mapping.values())
    types.discard(np.nan)
    harm_type = source_type.map(type_mapping).astype(CategoricalDtype(categories=types))

    return harm_type


def _to_numeric(s: pd.Series) -> pd.Series:
    nan_count_before = s.isna().sum()
    s = pd.to_numeric(s, errors='coerce')
    nan_count_after = s.isna().sum()
    failure_count = nan_count_before - nan_count_after

    if failure_count > 0:
        logger.warning(f'Coercing {s.name} to numeric failed for {failure_count} rows.')

    return s


def _extract_year(s: str) -> float:
    try:
        s = float(s[:4])  # extract year from YYYY-MM-DD encoded string
        if s < 1000:
            return np.nan

        return s

    except Exception:
        return np.nan


def _encode_missing_in_string_columns(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    string_cols = ['id', 'block_id', 'LAU_ID', 'h3_index', 'type_source', 'height_source', 'source_file']
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].replace(np.nan, None).astype('string')

    return df