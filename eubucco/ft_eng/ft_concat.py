import os
import glob
import logging
import random
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def generate_dataset(file_path, **kwargs):
    df = load_dataset(**kwargs)
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_pickle(file_path)


def load_dataset(data_dir, country=None, countries=[], cities=None, n_cities=None, fts_types=None, fts_selection=None, dropna_for_col=None, join_type='inner', seed=1):
    fts_types = fts_types or ['attrib', 'bld_fts', 'bld_d_fts', 'block_fts', 'block_d_fts', 'int_fts',
                              'str_fts', 'sbb_fts', 'wsf-evo_age', 'lu_fts', 'city_level_fts']
    if country:
        countries = [country]

    files = []
    for type in fts_types:
        for country in countries:
            files.extend(glob.glob(os.path.join(data_dir, country, '**', f'*_{type}.csv'), recursive=True))

    if not cities:
        cities = {file.rsplit('/')[-2] for file in files}

    if n_cities:
        random.seed(seed)
        cities = random.sample(cities, n_cities)

    dfs = []
    for city in cities:
        try:
            city_fts_df = _concatenate_fts_by_city(files, city, join_type, dropna_for_col)
            city_fts_df = _add_city_level_fts(city_fts_df, files, city)
            city_fts_df = _feature_selection(city_fts_df, fts_selection)
            dfs.append(city_fts_df)
        except Exception:
            logger.exception(
                f'Exception occurred while concatenating features for city {city}. Skipping {city} and continuing...')

    df = pd.concat(dfs, axis=0, ignore_index=True)
    df = df.drop_duplicates(subset=['id'])
    return df


def _concatenate_fts_by_city(files, city, join_type, dropna_for_col):
    dfs = []
    max_len = 0
    for file in files:
        if file.rsplit('/')[-2] == city and 'city_level_fts' not in file:
            df = pd.read_csv(file)

            max_len = max(len(df), max_len)

            df = df.drop_duplicates(subset=['id'])
            df = df.set_index('id')

            if dropna_for_col in df.columns:
                df = df.dropna(subset=[dropna_for_col])

            dfs.append(df)

    city_fts_df = pd.concat(dfs, axis=1, join=join_type)
    city_fts_df = city_fts_df.reset_index()
    city_fts_df = city_fts_df.drop_duplicates(subset=['id'])

    if dup_cols := set(city_fts_df.columns[city_fts_df.columns.duplicated()]):
        logger.debug(f'Removing duplicate columns {dup_cols}')
        city_fts_df = city_fts_df.loc[:,~city_fts_df.columns.duplicated()]

    if dropped_blds := max_len - len(city_fts_df):
        logger.warning(f'Dropped {dropped_blds} ({(dropped_blds) / max_len * 100:.2f}%) buildings for city {city}')

    return city_fts_df


def _add_city_level_fts(df, files, city):
    try:
        city_fts_file = next(file for file in files if file.rsplit('/')[-2] == city and 'city_level_fts' in file)
        city_fts_df = pd.read_csv(city_fts_file)
        df = df.assign(**city_fts_df.iloc[0].to_dict())
    except StopIteration:
        logger.warning(f'No city-level feature file found for {city}.')

    df['city'] = city
    return df


def _feature_selection(df, fts_selection):
    if fts_selection:
        df = df.drop(columns=set(df.columns) - fts_selection)
    return df
