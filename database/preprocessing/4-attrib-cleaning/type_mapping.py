import os
import sys
import pandas as pd

PROJECT_SRC_PATH = os.path.realpath(os.path.join(__file__, '..', '..', '..', '..', 'eubucco'))
sys.path.append(PROJECT_SRC_PATH)

from preproc.create_overview import get_paths_dataset
from preproc.attribs import * 
from ufo_map.Utils.helpers import arg_parser

# get the relevant files that should be mapped
path_cities,dataset_name = get_paths_dataset(arg_parser(['i']).i-1,
                                'attrib')
                                # path_inputs_csv = '/home/nmd/Projects/mlup/git-eubucco/database/preprocessing/1-parsing/inputs-parsing.csv',
                                # path_database = '/home/nmd/Data/eubucco'
                                # )

print(dataset_name)

# get the mapping dict
mapping = get_mapping_dict(dataset_name)
                        #   path_type_matches_folder = '/home/nmd/Data/eubucco')

failed = []

# map
for path_city in path_cities:

    print(path_city)

    try:
        df = pd.read_csv(path_city)
        # path_city = path_city.split('.')[0]+'_test.csv'
        type_mapping(df,mapping).to_csv(path_city,index=False)
    except:
        failed.append(path_city)

print('Done')
print(f'Failed:{failed}')
