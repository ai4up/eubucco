import pandas as pd
import os


def average_flanders_dupls(df):
    """
    Function that removes duplicates and calculates the average of duplicate entries, and adjusts the source file information
    """
    # average all values for int/float cols
    # drop all id duplicates in original file
    df_dropped = df.drop_duplicates(subset='id').reset_index()
    # get average of cols (only in cols where average can be calculated)
    df_grouped = df.groupby('id').mean().reset_index()
    # replace old value with mean val
    df_dropped['height'] = df_dropped['id'].map(df_grouped.set_index('id')['height']).fillna(df_dropped['height'])
    df_dropped['age'] = df_dropped['id'].map(df_grouped.set_index('id')['age']).fillna(df_dropped['age'])
    df_dropped = df_dropped.drop(columns='index')

    # adjust source files
    df_sources = df.groupby('id')['source_file'].apply(sum)
    df_dropped = pd.merge(
        df_dropped,
        df_sources,
        on='id').drop(
        columns='source_file_x').rename(
            columns={
                'source_file_y': 'source_file'})
    return df_dropped


def match_netherlands():
    """
    Function to match netherlands attrib with parsed geoms and adjust duplicated IDs
    Author: Felix
    Date: 22.03.22
    """
    # define paths
    path_root = '/p/projects/eubucco/data/1-intermediary-outputs/netherlands/NL3035'
    # path attrib
    path_attrib = '/p/projects/eubucco/data/1-intermediary-outputs/netherlands/netherlands-gov_attrib.csv'
    #
    path_int_fol = '/p/projects/eubucco/data/1-intermediary-outputs'
    country_name = 'netherlands'
    dataset_name = 'netherlands-gov'

    def get_geom_file(path_root, idx):
        path_geom = path_root + '/netherlands-gov_geom_' + str(idx) + '.csv'
        df = pd.read_csv(path_geom)
        return df

    print('reading in attrib files')
    # get attrib file
    df_attrib = pd.read_csv(path_attrib)

    print('taking care of duplicates in attrib file')
    # take care of duplicate ids in attrib file
    s_attribs = df_attrib.id.astype("string")
    s = s_attribs[s_attribs.duplicated(keep=False)]
    s_w_suffix = (s_attribs.astype(str) + '_' + s.groupby(s).cumcount().astype(str)).fillna(s_attribs)
    df_attrib['id'] = s_w_suffix

    # loop over geoms and split attrib
    len_df_old = 0
    for idx in range(20):
        print('----')
        print('looping through geom files {} / 20'.format(idx))
        # get path
        df = get_geom_file(path_root, idx)
        # get lentghs
        start = len_df_old
        end = len_df_old + len(df)
        # choose attrib files according to length
        df_attrib_part = df_attrib.iloc[start:end]
        # update id in geom file
        df['id'] = df_attrib_part.id.reset_index(drop=True)
        # save all changes
        print('saving files')
        df.to_csv(
            os.path.join(
                path_int_fol,
                country_name,
                'osm',
                dataset_name +
                '_' +
                str(idx) +
                '-3035_geoms.csv'),
            index=False)
        df_attrib_part.to_csv(
            os.path.join(
                path_int_fol,
                country_name,
                'osm',
                dataset_name +
                '_' +
                str(idx) +
                '_attrib.csv'),
            index=False)
        # update counter
        len_df_old += len(df)

    print('closing run. all files saved')


def poland_concat():
    path_root = '/p/projects/eubucco/data/1-intermediary-outputs/poland'

    for ending in ['gov-3035_geoms', 'gov_attrib', 'gov_extra_attrib']:

        # read in both files
        df0 = pd.read_csv(os.path.join(path_root, 'poland-' + ending + '_powiats' + '.csv'))
        df1 = pd.read_csv(os.path.join(path_root, 'poland-' + ending + '.csv'))

        # concate
        df_out = pd.concat([df0, df1])

        # save
        df_out.to_csv(os.path.join(path_root, 'poland-' + ending + '_combined.csv'), index=False)


def abruzzo_concat():
    path_root = '/p/projects/eubucco/data/1-intermediary-outputs/italy'

    for ending in ['gov-3035_geoms', 'gov_attrib']:

        # read in both files
        df0 = pd.read_csv(os.path.join(path_root, 'abruzzo-' + ending + '_p1' + '.csv'))
        df1 = pd.read_csv(os.path.join(path_root, 'abruzzo-' + ending + '_p2' + '.csv'))

        # concate
        df_out = pd.concat([df0, df1])

        if ending == 'gov_attrib':
            df_out = df_out[['id', 'height', 'type_source', 'type', 'age', 'floors', 'source_file']]
        if ending == 'gov-3035_geoms':
            df_out = df_out[['id', 'geometry']]

        # save
        df_out.to_csv(os.path.join(path_root, 'abruzzo-' + ending + '.csv'), index=False)
