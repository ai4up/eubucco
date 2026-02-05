from pathlib import Path

import numpy as np
import pandas as pd
import geopandas as gpd
from sklearn import metrics

from utils.load import all_files


def create_eval_metrics_file(pred_dir: str, aux_dir: str, out_dir: str, nuts_geometry_path: str, regions: list):
    stats = []
    for r in regions:
        stats.append(_calculate_regional_eval_metrics(Path(pred_dir), Path(aux_dir), r))

    df_stats = pd.concat(stats, ignore_index=True).set_index('region_id')
    gdf_stats = _add_nuts_geometry(df_stats, nuts_geometry_path)

    out_path = Path(out_dir) / 'prediction-eval-metrics-NUTS2.parquet'
    gdf_stats.to_parquet(out_path, index=False)


def create_city_overview_file(data_dir: str, out_dir: str, lau_geometry_path: str, nuts_geometry_path: str, pattern: str = r'.*\.parquet$'):
    stats = []
    for f in all_files(data_dir, pattern):
        stats.append(_calculate_city_stats(f))

    df_stats = pd.concat(stats, ignore_index=True).set_index('city_id')

    out_path = Path(out_dir) / 'city-stats.parquet'
    gdf_stats = _add_lau_geometry(df_stats, lau_geometry_path)
    gdf_stats.to_parquet(out_path, index=False)

    out_path = Path(out_dir) / 'region-stats.parquet'
    df_nuts_stats = _aggregate_to_nuts(df_stats)
    gdf_nuts_stats = _add_nuts_geometry(df_nuts_stats, nuts_geometry_path)

    gdf_nuts_stats.to_parquet(out_path, index=False)


def _calculate_regional_eval_metrics(pred_dir: Path, aux_dir: Path, region: str) -> pd.DataFrame:
    pred = _read_parquets(pred_dir, region).reset_index()
    aux = _read_parquets(aux_dir, region, columns=['id', 'bldg_msft_height', 'validation'])
    gdf = pred.merge(aux, on='id')

    gdf['binary_type_true'] = np.where(gdf["type_true"] == "residential", "residential", "non-residential")
    gdf['binary_type_true'] = pd.Categorical(gdf['binary_type_true'], categories=["residential", "non-residential"])
    gdf['binary_type_pred'] = np.where(gdf["type_pred"] == "residential", "residential", "non-residential")
    gdf['binary_type_pred'] = pd.Categorical(gdf['binary_type_pred'], categories=["residential", "non-residential"])

    stats = {
        'region_id': region,
        'country': region[:2],
        'n': len(gdf),
        'n_gt_binary_type': (gdf['binary_type_true'].notna()).sum(),
        'n_gt_type': (gdf['type_true'].notna()).sum(),
        'n_gt_residential_type': (gdf['residential_type_true'].notna()).sum(),
        'n_gt_height': (gdf['height_true'].notna()).sum(),
        'n_gt_floors': (gdf['floors_true'].notna()).sum(),
    }

    for var, bins in [('height', [0, 5, 10, 20, np.inf]), ('msft_height', [0, 5, 10, 20, np.inf]), ('floors', [0, 3, 6, np.inf])]:
        if var == 'msft_height':
            val = gdf.dropna(subset=['height_true', 'bldg_msft_height'])
            true = val['height_true']
            pred = val['bldg_msft_height']
        else:
            val = gdf.dropna(subset=[f'{var}_true'])
            # val = gdf[gdf['validation']].dropna(subset=[f'{var}_true']) # needed when evaluating local predictions to avoid train-test leakage
            true = val[f'{var}_true']
            pred = val[f'{var}_pred']

        if len(val) / len(gdf) < 0.01:
            continue

        stats[f'{var}_mae'] = metrics.mean_absolute_error(true, pred)
        stats[f'{var}_rmse'] = np.sqrt(metrics.mean_squared_error(true, pred))
        stats[f'{var}_r2'] = metrics.r2_score(true, pred)

        for i in range(len(bins) - 1):
            bin_mask = (true > bins[i]) & (true <= bins[i + 1])
            bin_true = true[bin_mask]
            bin_pred = pred[bin_mask]
            if len(bin_true) == 0:
                continue

            stats[f'{var}_mae_{bins[i]}_{bins[i + 1]}'] = metrics.mean_absolute_error(bin_true, bin_pred)
            stats[f'{var}_rmse_{bins[i]}_{bins[i + 1]}'] = np.sqrt(
                metrics.mean_squared_error(bin_true, bin_pred)
            )

    for var in ['binary_type', 'type', 'residential_type']:
        val = gdf.dropna(subset=[f'{var}_true'])

        if len(val) / len(gdf) < 0.01:
            continue

        # ensure classification metrics are calculated correctly even if only subset of classes is present
        true = val[f'{var}_true']
        pred = val[f'{var}_pred']
        stats[f'{var}_f1_macro'] = metrics.f1_score(true, pred, average='macro')
        stats[f'{var}_f1_micro'] = metrics.f1_score(true, pred, average='micro')
        stats[f'{var}_kappa'] = metrics.cohen_kappa_score(true, pred)

        if true.dtype != 'category':
            print(f"Region {region} - {var} - Classes: {true.unique()}")
            continue

        if var == 'binary_type':
            continue  # skip class-specific metrics for binary classifications

        categories = true.cat.categories
        f1_scores = metrics.f1_score(true, pred, average=None, labels=categories)

        rename = {
            "semi-detached duplex house": "semi-detached",
            "detached single-family house": "detached",
            "terraced house": "terraced",
            "apartment block": "apartment"
        }
        for class_label, f1 in zip(categories, f1_scores):
            stats[f'{var}_f1_{rename.get(class_label, class_label)}'] = f1

    return pd.DataFrame([stats])


def _calculate_city_stats(f: Path):
    gdf = gpd.read_parquet(f)

    # --- 1. BASE CALCULATIONS ---
    gdf['area'] = gdf.geometry.area
    gdf['floor_area'] = gdf['area'] * gdf['floors'].astype(float).fillna(1)

    # --- 2. PRE-CALCULATE ALL BOOLEAN FLAGS (Counts) ---
    # Sources
    gdf['is_gov'] = gdf['geometry_source'].str.contains('gov', na=False)
    gdf['is_osm'] = gdf['geometry_source'] == 'osm'
    gdf['is_msft'] = gdf['geometry_source'] == 'msft'

    # Ground Truth
    gdf['is_gt_type'] = gdf['type_source'] == gdf['geometry_source']
    gdf['is_gt_subtype'] = gdf['subtype_source'] == gdf['geometry_source']
    gdf['is_gt_height'] = gdf['height_source'] == gdf['geometry_source']
    gdf['is_gt_floors'] = gdf['floors_source'] == gdf['geometry_source']
    gdf['is_gt_construction_year'] = gdf['construction_year_source'] == gdf['geometry_source']

    # Estimations
    gdf['is_est_type'] = gdf['type_source'] == 'estimated'
    gdf['is_est_subtype'] = gdf['subtype_source'] == 'estimated'
    gdf['is_est_height'] = gdf['height_source'] == 'estimated'
    gdf['is_est_floors'] = gdf['floors_source'] == 'estimated'

    # Merged Values
    for col in ['type', 'subtype', 'height', 'floors', 'construction_year']:
        src = f'{col}_source'
        gdf[f'is_merged_{col}'] = (gdf[src] != gdf['geometry_source']) & (gdf[src] != 'estimated')

    # Bins
    gdf['h_0_5'] = (gdf['height'] > 0) & (gdf['height'] <= 5)
    gdf['h_5_10'] = (gdf['height'] > 5) & (gdf['height'] <= 10)
    gdf['h_10_20'] = (gdf['height'] > 10) & (gdf['height'] <= 20)
    gdf['h_20_inf'] = (gdf['height'] > 20)

    gdf['f_0_3'] = (gdf['floors'] > 0) & (gdf['floors'] <= 3)
    gdf['f_4_6'] = (gdf['floors'] > 3) & (gdf['floors'] <= 6)
    gdf['f_7_inf'] = (gdf['floors'] > 6)

    gdf['yr_0_1900'] = gdf['construction_year'] <= 1900
    gdf['yr_1901_1970'] = (gdf['construction_year'] > 1900) & (gdf['construction_year'] <= 1970)
    gdf['yr_1971_2000'] = (gdf['construction_year'] > 1970) & (gdf['construction_year'] <= 2000)
    gdf['yr_2001_inf'] = gdf['construction_year'] > 2000

    gdf['a_0_25'] = gdf['area'] <= 25
    gdf['a_25_100'] = (gdf['area'] > 25) & (gdf['area'] <= 100)
    gdf['a_100_500'] = (gdf['area'] > 100) & (gdf['area'] <= 500)
    gdf['a_500_inf'] = gdf['area'] > 500

    # Type/Subtype category counts
    types = ['residential', 'non-residential']
    subtypes = ['commercial', 'industrial', 'agricultural', 'public', 'others',
                'detached', 'semi-detached', 'terraced', 'apartment']

    for t in types:
        gdf[f'is_t_{t.replace("-", "_")}'] = gdf['type'] == t
    for st in subtypes:
        gdf[f'is_st_{st.replace("-", "_")}'] = gdf['subtype'] == st

    # --- 3. PRE-CALCULATE MASKED AREA COLUMNS ---
    for s in ['gov', 'osm', 'msft']:
        gdf[f'area_{s}'] = gdf['area'] * gdf[f'is_{s}']
        gdf[f'fa_{s}'] = gdf['floor_area'] * gdf[f'is_{s}']

    for t in types:
        t_cl = t.replace("-", "_")
        gdf[f'area_t_{t_cl}'] = gdf['area'] * gdf[f'is_t_{t_cl}']
        gdf[f'fa_t_{t_cl}'] = gdf['floor_area'] * gdf[f'is_t_{t_cl}']

    for st in subtypes:
        st_cl = st.replace("-", "_")
        gdf[f'area_st_{st_cl}'] = gdf['area'] * gdf[f'is_st_{st_cl}']
        gdf[f'fa_st_{st_cl}'] = gdf['floor_area'] * gdf[f'is_st_{st_cl}']

    # --- 4. AGGREGATION ---
    gdf['country'] = gdf['region_id'].str[:2]
    region_stats = gdf.groupby(['city_id', 'region_id', 'country']).agg(
        # Source counts
        n=('area', 'count'),
        n_gov=('is_gov', 'sum'),
        n_osm=('is_osm', 'sum'),
        n_msft=('is_msft', 'sum'),
        n_gt_type=('is_gt_type', 'sum'),
        n_gt_subtype=('is_gt_subtype', 'sum'),
        n_gt_height=('is_gt_height', 'sum'),
        n_gt_floors=('is_gt_floors', 'sum'),
        n_gt_construction_year=('is_gt_construction_year', 'sum'),
        n_merged_type=('is_merged_type', 'sum'),
        n_merged_subtype=('is_merged_subtype', 'sum'),
        n_merged_height=('is_merged_height', 'sum'),
        n_merged_floors=('is_merged_floors', 'sum'),
        n_merged_construction_year=('is_merged_construction_year', 'sum'),
        n_estimated_type=('is_est_type', 'sum'),
        n_estimated_subtype=('is_est_subtype', 'sum'),
        n_estimated_height=('is_est_height', 'sum'),
        n_estimated_floors=('is_est_floors', 'sum'),
        # Attribute-specific counts
        n_type_residential=('is_t_residential', 'sum'),
        n_type_non_residential=('is_t_non_residential', 'sum'),
        n_subtype_commercial=('is_st_commercial', 'sum'),
        n_subtype_industrial=('is_st_industrial', 'sum'),
        n_subtype_agricultural=('is_st_agricultural', 'sum'),
        n_subtype_public=('is_st_public', 'sum'),
        n_subtype_others=('is_st_others', 'sum'),
        n_subtype_detached=('is_st_detached', 'sum'),
        n_subtype_semi_detached=('is_st_semi_detached', 'sum'),
        n_subtype_terraced=('is_st_terraced', 'sum'),
        n_subtype_apartment=('is_st_apartment', 'sum'),
        n_height_0_5=('h_0_5', 'sum'),
        n_height_5_10=('h_5_10', 'sum'),
        n_height_10_20=('h_10_20', 'sum'),
        n_height_20_inf=('h_20_inf', 'sum'),
        n_floors_0_3=('f_0_3', 'sum'),
        n_floors_4_6=('f_4_6', 'sum'),
        n_floors_7_inf=('f_7_inf', 'sum'),
        n_construction_year_0_1900=('yr_0_1900', 'sum'),
        n_construction_year_1901_1970=('yr_1901_1970', 'sum'),
        n_construction_year_1971_2000=('yr_1971_2000', 'sum'),
        n_construction_year_2001_inf=('yr_2001_inf', 'sum'),
        n_area_0_25=('a_0_25', 'sum'),
        n_area_25_100=('a_25_100', 'sum'),
        n_area_100_500=('a_100_500', 'sum'),
        n_area_500_inf=('a_500_inf', 'sum'),
        # Area sums
        area=('area', 'sum'),
        area_gov=('area_gov', 'sum'),
        area_osm=('area_osm', 'sum'),
        area_msft=('area_msft', 'sum'),
        area_type_residential=('area_t_residential', 'sum'),
        area_type_non_residential=('area_t_non_residential', 'sum'),
        area_subtype_commercial=('area_st_commercial', 'sum'),
        area_subtype_industrial=('area_st_industrial', 'sum'),
        area_subtype_agricultural=('area_st_agricultural', 'sum'),
        area_subtype_public=('area_st_public', 'sum'),
        area_subtype_others=('area_st_others', 'sum'),
        area_subtype_detached=('area_st_detached', 'sum'),
        area_subtype_semi_detached=('area_st_semi_detached', 'sum'),
        area_subtype_terraced=('area_st_terraced', 'sum'),
        area_subtype_apartment=('area_st_apartment', 'sum'),
        # Floor area sums
        floor_area=('floor_area', 'sum'),
        floor_area_gov=('fa_gov', 'sum'),
        floor_area_osm=('fa_osm', 'sum'),
        floor_area_msft=('fa_msft', 'sum'),
        floor_area_type_residential=('fa_t_residential', 'sum'),
        floor_area_type_non_residential=('fa_t_non_residential', 'sum'),
        floor_area_subtype_commercial=('fa_st_commercial', 'sum'),
        floor_area_subtype_industrial=('fa_st_industrial', 'sum'),
        floor_area_subtype_agricultural=('fa_st_agricultural', 'sum'),
        floor_area_subtype_public=('fa_st_public', 'sum'),
        floor_area_subtype_others=('fa_st_others', 'sum'),
        floor_area_subtype_detached=('fa_st_detached', 'sum'),
        floor_area_subtype_semi_detached=('fa_st_semi_detached', 'sum'),
        floor_area_subtype_terraced=('fa_st_terraced', 'sum'),
        floor_area_subtype_apartment=('fa_st_apartment', 'sum'),
    ).astype(int).reset_index()

    return region_stats


def _add_lau_geometry(metrics: pd.DataFrame, geometry_path: str) -> gpd.GeoDataFrame:
    gdf_lau = gpd.read_parquet(geometry_path, columns=['city_id', 'geometry']).set_index('city_id').to_crs(3035)

    return gpd.GeoDataFrame(metrics.join(gdf_lau, how='left').reset_index(names='city_id'))


def _add_nuts_geometry(metrics: pd.DataFrame, geometry_path: str) -> gpd.GeoDataFrame:
    gdf_nuts = gpd.read_parquet(geometry_path, columns=['region_id', 'region_name', 'geometry']).set_index('region_id').to_crs(3035)

    return gpd.GeoDataFrame(metrics.join(gdf_nuts, how='left').reset_index(names='region_id'))


def _aggregate_to_nuts(metrics_lau: pd.DataFrame) -> pd.DataFrame:
    metrics_nuts = metrics_lau.groupby(['region_id', 'country']).sum().reset_index().set_index('region_id')

    return metrics_nuts


def _read_parquets(data_dir: Path, region: str, columns=None) -> pd.DataFrame:
    files = data_dir.glob(f"{region}*.parquet")
    return pd.concat(
        [pd.read_parquet(f, columns=columns) for f in files]
    )
