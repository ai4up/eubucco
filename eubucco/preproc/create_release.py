import glob
import json
from pathlib import Path
from typing import List, Tuple

import pandas as pd
import numpy as np
import geopandas as gpd
import pandas as pd


def create_release(regions: list, data_dir: str, prediction_data_dir: str, out_dir: str, source_mapping_path: str) -> None:
    """
    Create a release dataset for each region by loading conflated datasets and predictions,
    and converting to the release schema.
    """
    for region in regions:
        print(f"Creating release dataset for region {region}...")
        conflated = _load_conflated_datasets(region, data_dir)
        conflated = _preprocess_merging_uncertainties(conflated)
        predictions = _load_predictions(region, prediction_data_dir)

        combined = pd.concat([conflated, predictions], axis=1)
        release_dataset = _convert_to_release_schema(combined, source_mapping_path)

        (
            release_dataset
            .sort_values(by=["geometry_source", "city_id"])
            .to_parquet(
                path=Path(out_dir) / f"{region}.parquet",
                index=False,
                compression="gzip",
                write_covering_bbox=True,
                row_group_size=10_000,
                schema_version="1.1.0"
            )
        )


def _load_conflated_datasets(region: str, data_dir: str) -> gpd.GeoDataFrame:
    return _read_geoparquets(f"{data_dir}/{region}*.parquet").set_index("id")


def _load_predictions(region: str, data_dir: str) -> pd.DataFrame:
    return _read_parquets(f"{data_dir}/{region}*.parquet")


def _preprocess_merging_uncertainties(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    for attr in ["floors", "height", "construction_year"]:
        source_col = f"osm_{attr}_source_values"
        upper_col = f"osm_{attr}_confidence_upper"
        lower_col = f"osm_{attr}_confidence_lower"
        if source_col in gdf.columns:
            gdf[lower_col] = gdf[source_col].apply(np.min)
            gdf[upper_col] = gdf[source_col].apply(np.max)

    return gdf


def _convert_to_release_schema(df: pd.DataFrame, source_mapping_path: str) -> gpd.GeoDataFrame:
    """Build harmonized dataframe using flexible precedence with per-row sources."""
    df = _discard_unrealistic_floors_n_heights(df)
    df = _age_cleaning(df)
    df = _floors_cleaning(df)

    # --- Height ---
    height_precedence = [
        (df["source_dataset"], "height", None, None, None),
        ("osm", "osm_height_merged", "osm_height_confidence_lower", "osm_height_confidence_upper", "osm_height_source_ids"),
        ("estimated", "height_pred", "height_confidence_lower", "height_confidence_upper", None),
    ]
    height_specials = [
        {
            "when": df["height_source"] == "msft", 
            "then_value": df["height_pred"], 
            "then_source": "estimated", 
            "then_conf_lo": df["height_confidence_lower"], 
            "then_conf_hi": df["height_confidence_upper"]
        },
    ]
    # --- Floors ---
    floors_precedence = [
        (df["source_dataset"], "floors", None, None, None),
        ("osm", "osm_floors_merged", "osm_floors_confidence_lower", "osm_floors_confidence_upper", "osm_floors_source_ids"),
        ("estimated", "floors_pred", "floors_confidence_lower", "floors_confidence_upper", None),
    ]
    # --- Age ---
    age_precedence = [
        (df["source_dataset"], "age", None, None, None),
        ("osm", "osm_age_merged", "osm_construction_year_confidence_lower", "osm_construction_year_confidence_upper", "osm_age_source_ids"),
    ]
    # --- Type ---
    type_precedence = [
        (df["source_dataset"], "type", None, None, None),
        ("osm", "osm_type_merged", "osm_binary_type_confidence", None, "osm_type_source_ids"),
        ("estimated", "type_pred", "binary_type_confidence", None, None),
    ]
    # --- Subtype ---
    subtype_precedence = [
        (df["source_dataset"], "type", None, None, None),
        ("osm", "osm_type_merged", "osm_type_confidence", None, "osm_type_source_ids"),
        ("estimated", "type_pred", "type_confidence", None, None),
    ]
    res_type_precedence = [
        (df["source_dataset"], "residential_type", None, None, None),
        ("osm", "osm_residential_type_merged", "osm_residential_type_confidence", None, "osm_residential_type_source_ids"),
        ("estimated", "residential_type_pred", "residential_type_confidence", None, None),
    ]

    height_val, height_src, height_lo, height_hi, height_ids = map_with_precedence(df, height_precedence, height_specials)
    floors_val, floors_src, floors_lo, floors_hi, floors_ids = map_with_precedence(df, floors_precedence)
    age_val, age_src, age_lo, age_hi, age_ids = map_with_precedence(df, age_precedence)
    type_val, type_src, type_conf, _, type_ids = map_with_precedence(df, type_precedence)
    subtype_val, subtype_src, subtype_conf, _, subtype_ids = map_with_precedence(df, subtype_precedence)
    res_type_val, res_type_src, res_type_conf, _, res_type_ids = map_with_precedence(df, res_type_precedence)

    # merge residential type into subtype
    res_mask = subtype_val == "residential"
    subtype_val = subtype_val.where(~res_mask, res_type_val)
    subtype_src = subtype_src.where(~res_mask, res_type_src)
    subtype_conf = subtype_conf.where(~res_mask, subtype_conf.fillna(1) * res_type_conf)
    subtype_ids = subtype_ids.where(~res_mask, res_type_ids)

    # aggregate use types
    cats_res = ["semi-detached duplex house", "detached single-family house", "terraced house", "apartment block"]
    cats_nonres = ["industrial", "commercial", "public", "agricultural", "others"]
    type_val = np.where(type_val.isin(cats_nonres), "non-residential", "residential")
    type_val = pd.Categorical(type_val, categories=["residential", "non-residential"])
    subtype_val = pd.Categorical(subtype_val, categories=cats_nonres + cats_res).rename_categories({
        "semi-detached duplex house": "semi-detached",
        "detached single-family house": "detached",
        "terraced house": "terraced",
        "apartment block": "apartment"
    })

    # --- Source datasets ---
    with open(source_mapping_path, "r") as f:
        gov_datasets = json.load(f).keys()

    source_datasets = ["osm", "msft", "estimated"] + ["gov-" + s for s in gov_datasets]

    # --- Microsoft height ---
    microsoft_heights = np.where(df["source_dataset"] == "msft", df["height"], df["msft_height_merged"])

    # --- Combine ---
    df_converted = pd.DataFrame({
        "type": type_val,
        "type_source": pd.Categorical(type_src, categories=source_datasets),
        "type_confidence": type_conf.astype(float).round(2),
        "type_source_ids": type_ids,

        "subtype": subtype_val,
        "subtype_source": pd.Categorical(subtype_src, categories=source_datasets),
        "subtype_confidence": subtype_conf.astype(float).round(2),
        "subtype_source_ids": subtype_ids,
        "subtype_raw": df["type_source"],

        "height": height_val.astype(float).round(1),
        "height_source": pd.Categorical(height_src, categories=source_datasets),
        "height_confidence_lower": height_lo.astype(float).round(1),
        "height_confidence_upper": height_hi.astype(float).round(1),
        "height_source_ids": height_ids,

        "floors": floors_val.astype(float).round(1),
        "floors_source": pd.Categorical(floors_src, categories=source_datasets),
        "floors_confidence_lower": floors_lo.astype(float).round(1),
        "floors_confidence_upper": floors_hi.astype(float).round(1),
        "floors_source_ids": floors_ids,

        "construction_year": age_val.astype(float).round(0),
        "construction_year_source": pd.Categorical(age_src, categories=source_datasets),
        "construction_year_confidence_lower": age_lo.astype(float).round(0),
        "construction_year_confidence_upper": age_hi.astype(float).round(0),
        "construction_year_source_ids": age_ids,

        "id": df["block_id"] + "-" + df.groupby("block_id").cumcount().astype(str),
        "block_id": df["block_id"],
        "city_id": df["LAU_ID"],
        "region_id": df["region"],
        "last_changed": "v0.2",
        "microsoft_height": microsoft_heights.astype(float).round(1),

        "geometry": df["geometry"],
        "geometry_source": pd.Categorical(df["source_dataset"], categories=source_datasets),
        "geometry_source_id": df["id_source"],

    }).reset_index(drop=True)

    return gpd.GeoDataFrame(df_converted[
        [
            "id", "region_id", "city_id",
            "type", "subtype", "height", "floors", "construction_year",
            "type_confidence", "subtype_confidence", "height_confidence_lower", "height_confidence_upper", "floors_confidence_lower", "floors_confidence_upper", "construction_year_confidence_lower", "construction_year_confidence_upper",
            "geometry_source", "type_source", "subtype_source", "height_source", "floors_source", "construction_year_source",
            "geometry_source_id", "type_source_ids", "subtype_source_ids", "height_source_ids", "floors_source_ids", "construction_year_source_ids",
            "microsoft_height",
            "subtype_raw",
            "last_changed",
            "geometry",
        ]
    ])


def _read_parquets(path_pattern: str) -> pd.DataFrame:
    files = glob.glob(path_pattern)
    return pd.concat(
        [pd.read_parquet(f) for f in files]
    )


def _read_geoparquets(path_pattern: str) -> gpd.GeoDataFrame:
    files = glob.glob(path_pattern)
    return gpd.GeoDataFrame(pd.concat(
        [gpd.read_parquet(f) for f in files]
    ))


def map_with_precedence(
    df: pd.DataFrame, 
    precedence: List[tuple], 
    special_cases: List[dict] = None
) -> Tuple[pd.Series, pd.Series, pd.Series, pd.Series, pd.Series]:
    """
    Sequentially resolves attribute values, sources, and dual confidence bounds.

    Example:
        precedence = [
            ("osm", "osm_height", "h_conf_lo", "h_conf_hi", "osm_ids"),
            ("pred", "h_pred", "conf", "conf", None)
        ]
        specials = [{"when": df["type"] == "shed", "then_value": 2.5, "then_conf_lo": 1.0}]
        
        val, src, lo, hi, ids = map_with_precedence(df, precedence, specials)

    Args:
        df: Input DataFrame.
        precedence: List of (source, value_col, conf_lo_col, conf_hi_col, ids_col).
        special_cases: List of dicts for conditional overrides.

    Returns:
        Tuple of (values, sources, confidence_low, confidence_high, source_ids).
    """
    val = pd.Series(np.nan, index=df.index, dtype=object)
    src = pd.Series(np.nan, index=df.index, dtype=object)
    conf_lo = pd.Series(np.nan, index=df.index, dtype=float)
    conf_hi = pd.Series(np.nan, index=df.index, dtype=float)
    src_ids = pd.Series(np.nan, index=df.index, dtype=object)

    for source_name, value_col, conf_col_lo, conf_col_hi, ids_col in precedence:
        if value_col not in df.columns:
            continue

        mask = val.isna() & df[value_col].notna()
        if not mask.any():
            continue

        # Resolve source name
        if isinstance(source_name, str):
            src.loc[mask] = source_name
        elif isinstance(source_name, pd.Series):
            src.loc[mask] = source_name.loc[mask]
        else:
            src.loc[mask] = source_name(df).loc[mask]

        val.loc[mask] = df.loc[mask, value_col]
        
        if conf_col_lo and conf_col_lo in df.columns:
            conf_lo.loc[mask] = df.loc[mask, conf_col_lo]
        if conf_col_hi and conf_col_hi in df.columns:
            conf_hi.loc[mask] = df.loc[mask, conf_col_hi]
        if ids_col and ids_col in df.columns:
            src_ids.loc[mask] = df.loc[mask, ids_col]

    # Special-case overrides
    if special_cases:
        for case in special_cases:
            m = case["when"]
            if "then_value" in case:   val.loc[m] = case["then_value"]
            if "then_source" in case:  src.loc[m] = case["then_source"]
            if "then_conf_lo" in case: conf_lo.loc[m] = case["then_conf_lo"]
            if "then_conf_hi" in case: conf_hi.loc[m] = case["then_conf_hi"]
            if "then_ids" in case:     src_ids.loc[m] = case["then_ids"]

    return val, src, conf_lo, conf_hi, src_ids


def _discard_unrealistic_floors_n_heights(df: pd.DataFrame) -> pd.DataFrame:
    unreal_heights = (df["height"] <= 0) | (df["height"] > 350)
    unreal_floors = (df["floors"] <= 0) | (df["floors"] > 100)
    unreal_floor_height_ratio = (df['floors'] > df['height'] / 1.5) # assuming min floor height of 1.5m

    df.loc[unreal_floor_height_ratio | unreal_heights, "height"] = np.nan
    df.loc[unreal_floor_height_ratio | unreal_floors, "floors"] = np.nan

    return df


def _age_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    df['age'] = df['age'].dropna().astype(str).apply(_extract_year).astype(float)
    df['age'] = df['age'].clip(lower=0)
    df['age'] = df['age'].replace(0, np.nan)
    return df


def _floors_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    df['floors'] = df['floors'].clip(lower=1)
    return df


def _extract_year(s: str) -> float:
    try:
        s = float(s[:4])  # extract year from YYYY-MM-DD encoded string
        if s < 1000:
            return np.nan

        return s

    except Exception:
        return np.nan
