import pandas as pd
import numpy as np
import os

path_database = '/p/projects/eubucco/data/2-database-city-level'
country_name = 'france'
ending = 'attrib'

with open(os.path.join(path_database, country_name, 'paths_' + country_name + '.txt')) as f:
    paths = [line.rstrip() + '_' + ending + '.csv' for line in f]

# print(paths)

for path_city in paths:
    print(path_city)
    df_bldgs = pd.read_csv(path_city)
    df_bldgs['age'] = [int(x[:4]) if isinstance(x, str) else np.nan for x in df_bldgs.age]
    # ~~~
    # path_city = path_city.split('.')
    # path_city = path_city[0]+'_test.'+path_city[1]
    # ~~~
    df_bldgs.to_csv(path_city, index=False)
