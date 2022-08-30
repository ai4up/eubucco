import pandas as pd
import geopandas as gpd
import sys
from zipfile import ZipFile
sys.path.append('../..')
from utils.concate import validity_merge_db


def read_db(path):
    '''
        Load a database data chunk with both attributes and geometries.
    '''
    df_geom = gpd.read_file(path+'!geom.gpkg')
    df_attrib = pd.read_csv(ZipFile(path).open('attrib.csv'))
    
    if validity_merge_db(df_geom,df_attrib): 
        
        return pd.merge(df_geom,df_attrib,on='id') 


def match_gadm_info(df_temp,df_overview):
    """ function to match country, region and city info from overview table with building level data
        df_temp (dataframe):=   building level dataframe
        df_overview:=           overview table
    """
    # remove numbering at end of id str 
    df_temp['id_temp'] = df_temp['id'].str.rsplit('-',1).apply(lambda x: x[0])
    # merge with overview file
    df_out = df_temp.merge(df_overview, left_on='id_temp',right_on='id')
    # keep only relevant columns
    df_out = df_out[['id_x','id_source','country','region','city','height','age','type','type_source','geometry']]
    # rename back to 'id' and return
    return df_out.rename(columns={'idx_x':'id'})        