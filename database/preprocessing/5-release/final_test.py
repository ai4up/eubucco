import pandas as pd
import geopandas as gpd
from zipfile import ZipFile, Path
import sys,os
sys.path.append('/home/nmd/Projects/mlup/eubucco/eubucco/')
from utils.load import read_db,match_gadm_info
from utils.concate import compare_overviews

PATH = "XXX"            # path to folder with all database content, chunk zip individually
LIST_CHUNK = ['XXX']
VERSION = "v0.1"
CODE_COUNTRY_MAP = {}


print('Final test chunking...')

# import meta info
df_meta = pd.read_csv(ZipFile(os.path.join(PATH,'v0_1-ADDITIONAL-FILES.zip'))\
    .open('v0_1-ADDITIONAL-FILES/admin-codes-matches-v0.1.csv'))

# loop through files in folder
for chunk in LIST_CHUNK:

    print(f"============\n{chunk}\n============")

    # import df and match
    df = read_db(os.path.join(PATH,VERSION+'_'+chunk+'.zip'))
    df = match_gadm_info(df,df_meta)

    # import overview
    df_overview = pd.read_csv(ZipFile(os.path.join(PATH,'city-level-overview-tables-v0.1.zip'))\
        .open(f'{CODE_COUNTRY_MAP[chunk[0:2]]}_overview.csv'))
    
    # compute new stats
    d = {'bldgs_n_tot': df.groupby('city')['city'].count().rename('right_col'),
        'height_n':    df.height.groupby(df.city).count().rename('right_col') - df.height.isna().groupby(df.city).sum().rename('right_col'),
        'age_n':       df.city.groupby(df.city).count().rename('right_col') - df.age.isna().groupby(df.city).sum().rename('right_col'),
        'type_n':      df.city.groupby(df.city).count().rename('right_col') - df['type'].isna().groupby(df.city).sum().rename('right_col')
        }
    
    # compare
    for key in d.keys(): compare_overviews(key,d[key],df_overview)