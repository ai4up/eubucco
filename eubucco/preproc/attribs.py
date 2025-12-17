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
            df = _add_source_dataset_col(df, dataset_type, source_mapping_path)

            if dataset_type == 'msft':
                df = msft_height_cleaning(df)
            else:
                df = type_cleaning(df)
                df = type_mapping(df, type_mapping_path)
                df = age_cleaning(df)
                df = height_cleaning(df)
                df = floors_cleaning(df)

            df = _remove_duplicates(df)
            df = _remove_non_building_structures(df, type_mapping_path)
            df = _encode_missing_in_string_columns(df)
            df.to_parquet(out_path)

        except Exception:
            logger.exception(
                f'Exception occurred while cleaning attributes for file {f.name}. Skipping {f.name} and continuing...')


def attrib_cleaning_post_conflation(data_dir: str, out_dir: str, type_mapping_path: str = None, source_mapping_path: str = None, file_pattern: str = None) -> None:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    for f in all_files(data_dir, file_pattern):
        try:
            out_path = out_dir / f"{f.stem}.parquet"
            osm_path = Path("/p/projects/eubucco/data/3-attrib-cleaning-v1-osm") / f"{f.stem}.parquet"

            if out_path.is_file():
                logger.info(f'Attributes already cleaned for {f.name}...')
                continue

            logger.info(f'Cleaning attributes for {f.name}...')
            df = _read_geodata(f)
            df = _add_source_dataset_col(df, source_mapping_path)
            df = type_cleaning(df)
            df = type_mapping(df, type_mapping_path)
            df = floors_cleaning(df)
            df = _remove_non_building_structures(df, type_mapping_path)

            if "gov" in df["dataset"].unique() or "osm" in df["dataset"].unique():
                df = _reverse_height_estimation_from_floors(df)

            if "gov" in df["dataset"].unique() and osm_path.is_file():
                osm = _read_geodata(osm_path)
                osm = type_mapping(osm, type_mapping_path, "osm")
                osm = _reverse_height_estimation_from_floors(osm)
                osm = _remove_non_building_structures(osm, type_mapping_path)

                df = merge_weighted_source_values(df, osm, attr='floors', source_id='osm_height_source_ids') # must run before height
                df = merge_weighted_source_values(df, osm, attr='height', source_id='osm_height_source_ids')
                df = merge_weighted_source_values(df, osm, attr='age', source_id='osm_age_source_ids')
                df = merge_source_type(df, osm, attr='type', agg_binary=True)
                df = merge_source_type(df, osm, attr='residential_type')

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


def _add_source_dataset_col(df: gpd.GeoDataFrame, source_mapping_path: str = None) -> gpd.GeoDataFrame:
    df['source_dataset'] = df['dataset']
    with open(source_mapping_path, 'r') as f:
        region_mapping = json.load(f)
        source_file_mapping = {v: k for k, vs in region_mapping.items() for v in vs}

    mask = df['dataset'] == 'gov'
    df.loc[mask, 'source_dataset'] = 'gov-' + df[mask]['source_file'].map(source_file_mapping)

    return df


def _remove_non_building_structures(df: gpd.GeoDataFrame, type_mapping_path: str) -> gpd.GeoDataFrame:
    '''Remove non-building structures based on type_source column'''
    bldg_types = pd.read_csv(type_mapping_path)
    regional_types = bldg_types[bldg_types['source_datasets'].apply(lambda x: bool(set(x.split(',')) & set(df['source_dataset'].unique())))]
    types_to_be_removed = regional_types[regional_types['remove']]['type_source'].unique().tolist()

    len1 = len(df)
    df = df[~df['type_source'].isin(types_to_be_removed)]
    df = df[~df['type_source'].str.startswith('5300', na=False)]  # German ALKIS Code for traffic areas
    len2 = len(df)
    logger.info(f'Removed {len1-len2} non-building structures based on type_source column.')

    return df


def msft_height_cleaning(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    df['height'] = _to_numeric(df['height'].replace(-1, np.nan))
    df['height'] = df['height'].clip(lower=0)
    df['height'] = df['height'].replace(0, np.nan)
    df['height_source'] = df['height']

    return df


def height_cleaning(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    df['height'] = _to_numeric(df['height'])
    df['height'] = df['height'].clip(lower=0)
    df['height'] = df['height'].replace(0, np.nan)
    df['height_source'] = df['height']
    df = _estimate_height_from_floors(df)

    return df


def floors_cleaning(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    df['floors'] = _to_numeric(df['floors'])
    df['floors'] = df['floors'].clip(lower=0)
    df['floors'] = df['floors'].replace(0, np.nan)

    return df


def age_cleaning(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    df['age'] = df['age'].dropna().astype(str).apply(_extract_year)
    df['age'] = df['age'].clip(lower=0)
    df['age'] = df['age'].replace(0, np.nan)

    return df


def type_cleaning(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    df['type_source'] = df['type_source'].astype('string')

    return df


def type_mapping(df: gpd.GeoDataFrame, type_mapping_path: str, source_dataset: str = None) -> gpd.GeoDataFrame:
    bldg_types = pd.read_csv(type_mapping_path)
    bldg_types['type_source'] = bldg_types['type_source'].astype('string')

    type_categories = set(bldg_types['type'].unique())
    res_type_categories = set(bldg_types['residential_type'].unique())
    if "source_dataset" not in df.columns:
        df['source_dataset'] = source_dataset

    regional_types = bldg_types[bldg_types['source_datasets'].apply(lambda x: bool(set(x.split(',')) & set(df['source_dataset'].unique())))]
    type_mapping = regional_types.set_index('type_source')['type'].to_dict()
    res_type_mapping = regional_types.set_index('type_source')['residential_type'].to_dict()

    df['type'] = _harmonize_type(df['type_source'], type_mapping, type_categories)
    df['residential_type'] = _harmonize_type(df['type_source'], res_type_mapping, res_type_categories)

    return df


def _read_geodata(path: Path) -> gpd.GeoDataFrame:
    if 'parquet' in path.suffix:
        return gpd.read_parquet(path)
    elif 'gpkg' in path.suffix:
        return gpd.read_file(path)
    else:
        raise ValueError(f'Unsupported file format: {path.suffix}')


def _estimate_height_from_floors(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    df.loc[df['floors'].notna(), 'height_source'] = df['height_source'].fillna('floors')
    df['height'] = df['height'].fillna(df['floors'] * FLOOR_HEIGHT)

    return df


def _reverse_height_estimation_from_floors(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    mask = df['height_source'] == 'floors'
    df.loc[mask, 'height_source'] = np.nan
    df.loc[mask, 'height'] = np.nan

    return df


def _harmonize_type(source_type: pd.Series, type_mapping: Dict[str, str], type_categories: set) -> pd.Series:
    '''Maps buildings types from the source dataset to harmonized types for each building in a city.'''
    type_categories.discard(np.nan)
    harm_type = source_type.map(type_mapping).astype(CategoricalDtype(categories=type_categories))

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


def merge_source_values(gdf: gpd.GeoDataFrame, osm: gpd.GeoDataFrame, attr: str):
    source_id = f"osm_{attr}_source_ids"
    df_exp = (
        gdf[["id", source_id]]
        .dropna()
        .explode(source_id, ignore_index=True)
        .merge(osm[["id", attr]].dropna().rename(columns={"id": source_id}), on=source_id, how="inner")
    )
    df_heights = (
        df_exp.groupby("id", sort=False, as_index=False)
        .agg({attr: list, source_id: list})
        .rename(columns={attr: f"osm_{attr}_source_values"})
    )
    gdf = gdf.drop(columns=[source_id, f"osm_{attr}_source_values"], errors="ignore").merge(df_heights[["id", source_id, f"osm_{attr}_source_values"]], on="id", how="left")

    return gdf


def merge_weighted_source_values(gdf: gpd.GeoDataFrame, osm: gpd.GeoDataFrame, attr: str, source_id: str):
    gdf1_small = gdf[["id", source_id, "geometry"]].dropna(subset=[source_id])
    gdf2_small = osm[["id", attr, "geometry"]].dropna(subset=[attr]).rename(columns={"id": source_id})

    gdf1_exp = gdf1_small.explode(source_id, ignore_index=True)

    merged = gpd.GeoDataFrame(gdf1_exp.merge(gdf2_small, left_on=source_id, right_on=source_id, how="inner"))
    merged["area_int"] = merged.geometry_x.intersection(merged.geometry_y).area

    merged["int_weight"] = merged["area_int"] / merged.groupby("id")["area_int"].transform("sum")
    merged["int_weight"] = merged["int_weight"].fillna(merged.groupby("id")["id"].transform("size"))
    merged["attr_weight"] = merged[attr] * merged["int_weight"]

    merged["overlap_ratio"] = merged["area_int"] / merged.geometry_x.area

    weighted_attr = (
        merged.groupby("id", as_index=False)[["attr_weight", "overlap_ratio"]]
        .sum(min_count=1)
        .rename(columns={"attr_weight": f"osm_{attr}_merged", "overlap_ratio": f"osm_{attr}_confidence"})
    )
    gdf = gdf.drop(columns=[f"osm_{attr}_merged", f"osm_{attr}_confidence"], errors="ignore").merge(weighted_attr, on="id", how="left")

    source_lists = (
        merged.groupby("id", as_index=False)
        .agg({attr: list, source_id: list})
        .rename(columns={attr: f"osm_{attr}_source_values", source_id: f"osm_{attr}_source_ids"})
    )
    gdf = gdf.drop(columns=[f"osm_{attr}_source_ids", f"osm_{attr}_source_values"], errors="ignore").merge(source_lists, on="id", how="left")

    return gdf


def merge_source_type(gdf: gpd.GeoDataFrame, osm: gpd.GeoDataFrame, attr: str, agg_binary=False):
    source_id = f"osm_{attr}_source_ids"
    if attr == "residential_type":
        gdf1_small = gdf[gdf["osm_type_merged"] == "residential"][["id", source_id, "geometry"]].dropna(subset=[source_id])
    else:
        gdf1_small = gdf[["id", source_id, "geometry"]].dropna(subset=[source_id])
    gdf2_small = osm[["id", attr, "geometry"]].rename(columns={"id": source_id})

    gdf1_exp = gdf1_small.explode(source_id, ignore_index=True)

    merged = gdf1_exp.merge(gdf2_small, left_on=source_id, right_on=source_id, how="left")
    merged = gpd.GeoDataFrame(gdf1_exp.merge(gdf2_small, left_on=source_id, right_on=source_id, how="left"))

    merged["area_target"] = merged.geometry_x.area
    merged["area_int"] = merged.geometry_x.intersection(merged.geometry_y).area

    area_by_type = (
        merged.groupby(["id", attr], observed=True)
        .agg({"area_int": "sum", "area_target": "first", source_id: list, attr: list})
        .rename(columns={attr: f"osm_{attr}_source_values"})
        .reset_index()
    )
    area_by_type["overlap_ratio"] = area_by_type["area_int"] / area_by_type["area_target"]
    dominant_type = area_by_type.sort_values(by="overlap_ratio", ascending=False).drop_duplicates(subset=["id"], keep="first")
    dominant_type[f"osm_{attr}_confidence"] = dominant_type["overlap_ratio"].clip(0, 1)

    gdf = (
        gdf.drop(columns=[source_id, f"osm_{attr}_merged", f"osm_{attr}_source_values", f"osm_{attr}_confidence"], errors="ignore")
        .merge(
            dominant_type[["id", source_id, attr, f"osm_{attr}_source_values", f"osm_{attr}_confidence"]]
            .rename(columns={attr: f"osm_{attr}_merged"}
                    ), on="id", how="left"
        )
    )

    if agg_binary:
        area_by_type["osm_binary_type_merged"] = np.where(area_by_type[attr] == "residential", "residential", "non-residential")
        area_by_binary_type = area_by_type.groupby(["id", "osm_binary_type_merged"], observed=True, as_index=False)["overlap_ratio"].sum()
        dominant_binary_type = area_by_binary_type.sort_values(by="overlap_ratio", ascending=False).drop_duplicates(subset=["id"], keep="first")
        dominant_binary_type["osm_binary_type_confidence"] = dominant_binary_type["overlap_ratio"].clip(0, 1)

        gdf = gdf.merge(
            dominant_binary_type[["id", "osm_binary_type_merged", "osm_binary_type_confidence"]],
            on="id",
            how="left",
        )

    return gdf
