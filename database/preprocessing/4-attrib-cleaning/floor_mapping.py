import os
import sys
import pandas as pd

PROJECT_SRC_PATH = os.path.realpath(os.path.join(__file__, '..', '..', '..', '..', 'eubucco'))
sys.path.append(PROJECT_SRC_PATH)

from preproc.create_overview import get_paths_dataset
from preproc.attribs import *
from ufo_map.Utils.helpers import arg_parser

# get the relevant files that should be mapped
path_cities,dataset_name = get_paths_dataset(arg_parser(['i']).i-1,'attrib')

print('...')

# intialise
failed = []
lst_vals = []
# map
for path_city in path_cities:

    print('-------')
    print(path_city)

    try:
        df = pd.read_csv(path_city)
        len_pre = len(df[df.height.isna()])
        df['height'] = add_floor_as_height(df)
        len_post = len(df[df.height.isna()])
        print('Filled in {} vals'.format(len_pre-len_post))
        lst_vals.append(len_pre-len_post)
        # path_city = path_city.split('.')[0]+'_test.csv'
        df.to_csv(path_city,index=False)
    except:
        failed.append(path_city)

print('Filled in {} height values from floors'.format(sum(lst_vals)))
print('Done')
print(f'Failed:{failed}')
