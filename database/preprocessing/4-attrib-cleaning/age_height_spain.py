import pandas as pd
import os
import numpy as np

path_database = '/p/projects/eubucco/data/2-database-city-level'
country_name = 'spain'
ending = 'attrib'

with open(os.path.join(path_database, country_name, 'paths_' + country_name + '.txt')) as f:
    paths = [line.rstrip() + '_' + ending + '.csv' for line in f]

failed = []

# for path_city in paths[571-4:]:
for path_city in paths:

    print(path_city)
    try:
        df_bldgs = pd.read_csv(path_city)

        df_bldgs['age'] = [int(x.split('-')[0]) if x[0] != '-' else np.nan for x in df_bldgs.age]
        df_bldgs['height'] = df_bldgs.floors * 2.5
        # df_bldgs = df_bldgs.drop(columns='heights')

        df_bldgs.to_csv(path_city, index=False)
    except BaseException:
        failed.append(path_city)


print('Done')
print(f'Failed:{failed}')
