import os
import pandas as pd
import socket
print(socket.gethostname())


def get_mapping_dict(dataset_name,
                     path_type_matches_folder='/p/projects/eubucco/data/0-raw-data/type-matches'
                     ) -> dict:
    '''
        Creates dict with {type_source:type_db} key/value pairs
        from a csv with such information available as columns.
    '''

    if dataset_name in ['hamburg-gov', 'brandenburg-gov', 'sachsen-gov', 'sachsen-anhalt-gov',
                        'thuerigen-gov', 'nordrhein-westfalen-gov', 'niedersachsen-gov']:
        dataset_name = 'germany-alkis'

    elif dataset_name in ['calabria-gov', 'lazio-gov', 'piemonte-gov', 'veneto-gov']:
        dataset_name = 'italy-edifc-uso'

    elif 'osm' in dataset_name:
        dataset_name = 'osm'

    df_types = pd.read_csv(os.path.join(path_type_matches_folder, dataset_name + '_type_matches.csv'))
    df_types['type_db'] = [x if isinstance(x, str) else '' for x in df_types.type_db]

    return {x: y for x, y in zip(df_types.type_source, df_types.type_db)}


def type_mapping(df_bldgs, mapping) -> pd.DataFrame:
    '''
        Maps buildings types from the source dataset to residential, non-residential
        or unknown for each building in a city.

        Parameters:
        * mapping: dict with {type_source:type_db} key/value pairs
        * df_types: pd.DataFrame with the columns 'code' and 'type_db'

        Returns: pd.DataFrame with new types in the 'type' column
    '''

    print(f'Could not match {len([x for x in df_bldgs.type_source if x not in mapping.keys()])} source types')
    print(f'Could not match: {set([x for x in df_bldgs.type_source if x not in mapping.keys()])}')

    df_bldgs['type'] = [mapping[x] if x in mapping.keys() else '' for x in df_bldgs.type_source]

    return df_bldgs


def add_floor_as_height(df,floor_height):
    return [x if str(x) != 'nan' else y * floor_height for x, y in zip(df.height, df.floors)]
