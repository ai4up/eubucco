import pandas as pd
import os
import sys

def validate_ids(city_path):
    '''
        Test function that ensures that ids between geom and attrib
        files are well aligned and do not contain duplicates.

        Takes as input a city path for structure that should be:

        ../<city_name>
            |_ <city_name>_geom.csv
            |_ <city_name>_attrib.csv

    '''
    print(f'Test run for {city_path}')
    print('===============\n')

    failed = False

    #city_name = os.path.normpath(city_path).split(os.path.sep)[-1]

    df_geom = pd.read_csv(os.path.join(city_path+'_geom.csv'))
    df_attrib = pd.read_csv(os.path.join(city_path+'_attrib.csv'))

    # test duplicates before merge
    n_duplicates_id_geom = df_geom.duplicated(subset=['id']).sum()
    n_duplicates_id_attrib = df_attrib.duplicated(subset=['id']).sum()
    print(f'Nb duplicates id geom {n_duplicates_id_geom}')
    print(f'Nb duplicates id attrib {n_duplicates_id_attrib}')

    if any((n_duplicates_id_attrib,n_duplicates_id_geom)) > 0:
        failed = True
        d_geom = df_geom[df_geom.duplicated(subset='id',keep=False)]['id'].to_list()
        d_attrib = df_attrib[df_attrib.duplicated(subset='id',keep=False)]['id'].to_list()
        print('Failure: there are issues with ids pre-merge')
        print(f'Duplicated id geom: {d_geom}')
        print(f'Duplicated id attrib: {d_attrib}')

    # merge
    df_merge = pd.merge(df_geom, df_attrib, on='id', how='outer')

    # tests after merge
    n_duplicates_merge = df_merge.duplicated(subset=['id']).sum()
    n_id_source_diff = (df_merge.id_source_x != df_merge.id_source_y).sum()
    print(f'Nb duplicates id after merge {n_duplicates_id_attrib}')
    print(f'Nb disagreements id_source {n_id_source_diff}')

    if any((n_duplicates_merge,n_id_source_diff)) > 0:
        failed = True
        d_merge = df_merge[df_merge.duplicated(subset='id',keep=False)]['id'].to_list()
        id_source_diff = df_merge[df_merge.id_source_x != df_merge.id_source_y]
        id_source_diff = [(x,y) for x,y in (id_source_diff.id_source_x,id_source_diff.id_source_y)] 
        print('Failure: there are duplicated ids post-merge')
        print(f'Duplicated id post merge: {d_merge}')
        print(f'Disagreements id_source: {id_source_diff}')

    print('===============\n')

    if failed: print('FAILURE')
    else: print('SUCCESS')










# geom.csv -> id, id_source
# attrib.csv -> id, id_source
# ==> outer merge on id, check that id_source_y and id_source_x work, check that there are no duplicates / no null, or len before/after

# if success: 
# if failed: 

# path
# failed
# nb_fail_x
# id_duplicate
# source_id_duplicates
