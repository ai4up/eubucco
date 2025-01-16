import logging
from pathlib import Path
import re
from typing import Dict, List

import pandas as pd
import geopandas as gpd
import numpy as np
from pandas.api.types import CategoricalDtype

FLOOR_HEIGHT = 3  # meter

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


def attrib_cleaning(data_dir: str, out_dir: str, type_mapping_path: str, db_version: str, file_pattern: str = None) -> None:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    for f in _all_files(data_dir, file_pattern):
        try:
            out_path = out_dir / f"{f.stem}.parquet"

            if out_path.is_file():
                logger.info(f'Attributes already cleaned for {f.name}...')
                continue

            logger.info(f'Cleaning attributes for {f.name}...')
            if 'parquet' in f.suffix:
                df = gpd.read_parquet(f)
            elif 'gpkg' in f.suffix:
                df = gpd.read_file(f)
            else:
                raise ValueError(f'Unsupported file format: {f.suffix}')

            df = unique_ids(df, db_version)
            df = type_mapping(df, type_mapping_path)
            df = age_cleaning(df)
            df = height_cleaning(df)
            df = floors_cleaning(df)
            df = _encode_missing_in_string_columns(df)

            df.to_parquet(out_path)

        except Exception:
            logger.exception(
                f'Exception occurred while cleaning attributes for file {f.name}. Skipping {f.name} and continuing...')


def height_cleaning(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    df = _estimate_height_from_floors(df)
    df['height'] = _to_numeric(df['height'])

    return df


def floors_cleaning(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    df['floors'] = _to_numeric(df['floors'])

    return df


def age_cleaning(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    df['age'] = df['age'].dropna().astype(str).apply(_extract_year)

    return df


def type_mapping(df: gpd.GeoDataFrame, type_mapping_path: str) -> gpd.GeoDataFrame:
    bldg_types = pd.read_csv(type_mapping_path)
    type_mapping = bldg_types.set_index('type_source')['type'].to_dict()
    res_type_mapping = bldg_types.set_index('type_source')['residential_type'].to_dict()

    df['type'] = _harmonize_type(df['type_source'], type_mapping)
    df['residential_type'] = _harmonize_type(df['type_source'], res_type_mapping)

    return df


def unique_ids(df: gpd.GeoDataFrame, db_version: str) -> gpd.GeoDataFrame:
    df['id_source'] = df['id']
    df['id'] = 'v' + str(db_version) + '-' + df['LAU_ID'] + '-' + pd.Series(range(len(df)), dtype=str)

    return df


def _all_files(data_dir: str, pattern: str = None) -> List[Path]:
    paths = Path(data_dir).rglob('*')
    files = [p for p in paths if p.is_file()]

    if pattern:
        pattern = re.compile(pattern)
        files = [f for f in files if pattern.match(f.name)]

    return files


def _estimate_height_from_floors(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    df['height_source'] = df['height']
    df['height_source'] = df['height_source'].fillna('floors')
    df['height'] = df['height'].fillna(df['floors'] * FLOOR_HEIGHT)

    return df


def _harmonize_type(source_type: pd.Series, type_mapping: Dict[str, str]) -> pd.Series:
    '''
        Maps buildings types from the source dataset to harmonized types for each building in a city.
    '''

    types = set(type_mapping.values())
    types.remove(np.nan)
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
    string_cols = ['id', 'id_source', 'block_id', 'LAU_ID', 'h3_index', 'type_source', 'height_source', 'source_file']
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].replace(np.nan, None).astype('string')

    return df