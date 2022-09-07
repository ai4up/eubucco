import os
import pandas as pd
import socket
print(socket.gethostname())

from preproc.create_overview import get_paths_dataset

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


def floor_checks(df, df_merge,len_df,id_source_df,height_df,n_nan):
    """
    Function to check for several errors that could occur while remdoing the floor mapping!
    """
    # check that we didnt add additional rows to df
    if len(df)!=len_df: raise ValueError('Error! Merge created additional rows!')
    # check that all ids stayed constant  
    if len(df.loc[~df.id_source.isin(id_source_df)])>0: raise ValueError('Error! Merge created additional ids that were not there before!')        
    # check that we replace the right amount of values 
    if len(df.height.compare(height_df)) != len(df_merge): raise ValueError('Error! Adding too many values!')
    # check that no additional nans were created
    if len(df.loc[df.height.isna()])>n_nan: raise ValueError('Error! we have more Nans than before in height col!')
    # check that right column type 
    if 'height' not in df.select_dtypes(include=[float]).columns: raise ValueError('Error! dtype of height col is not float anymore')
    return True


def fix_floor(i, 
            p, 
            floor_height,
            test=False,
            path_input_parsing = '/p/projects/eubucco/git-eubucco/database/preprocessing/1-parsing/inputs-parsing.csv',
            path_int_fol= '/p/projects/eubucco/data/1-intermediary-outputs',
            path_db_set_up = '/p/projects/eubucco/data/2-database-city-level-v0_1'):
    """
    Function to fix the floor mapping by opening the parsed dataset and replacing values where floor mapping is applicable
    with new floor mapping vals
    """

    # get df_attrib large file
    print('loading large attrib file...')
    df_pars = pd.read_csv(os.path.join(path_int_fol,
                                    p['country'],
                                    p['dataset_name'] +
                                    '_attrib.csv'))

    # take only cols that have nan in height col and a floor value
    df_pars = df_pars[(~df_pars.floors.isna())&(df_pars.height.isna())]
    print('{} bldgs where height is nan and floor value given'.format(len(df_pars)))

    if len(df_pars)>0:
        # take only id, floor and height val
        df_pars = df_pars[['id','height','floors']]
        # do floor mapping on attrib file
        len_pre = len(df_pars[df_pars.height.isna()])                
        df_pars['height'] = add_floor_as_height(df_pars,floor_height)
        len_post = len(df_pars[df_pars.height.isna()])
        print('Filled in {} vals'.format(len_pre - len_post))
        print('----')
        
        # prepare for merge
        df_pars = df_pars.rename(columns={'id':'id_old'})
        df_pars = df_pars.drop(columns='floors')
        # get the files that should be re-mapped
        paths_cities, dataset_name = get_paths_dataset(i-1,            
                                                    'attrib',
                                                    path_input_parsing,
                                                    path_db_set_up)
        # intialise
        failed = []
        lst_vals = []
        for path_city in paths_cities:
            print('-------')
            print(path_city.split('/')[-1])
            try:
                # read in files
                df = pd.read_csv(path_city)
                len_df = len(df)
                n_nan = len(df.loc[df.height.isna()])
                id_source_df = df.id_source.copy()
                bool_merge = True
                height_df = df.height.copy()
            except BaseException:
                bool_merge = False
                failed.append(path_city)
            
            if bool_merge:
                # merge on id
                df_merge = pd.merge(df,df_pars,left_on='id_source',right_on='id_old').reset_index(drop=True)
                if len(df_merge)>0: 
                    print(df_merge)
                    # assign height values from floos calculation
                    df.loc[df.id_source.isin(df_merge.id_source),'height'] = df_merge.height_y
                    print('Filling in {} height vals'.format(len(df_merge)))
                    print(df)
                    lst_vals.append(len(df_merge))
                    # do checks before saving 
                    if floor_checks(df, df_merge,len_df, id_source_df,height_df,n_nan): 
                        print('Saving to disk...')
                        if test: path_city = '/p/projects/eubucco/data/test_data/floor_fixing'+path_city.rsplit('/')[-1]
                        print(path_city)
                        #df.to_csv(path_city, index=False)

        print('------')
        print('------')
        print('Filled in {} height values from floors'.format(sum(lst_vals)))
        print('Done')
        print(f'Could not load:{failed}')