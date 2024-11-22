import pandas as pd
from countrygroups import EUROPEAN_UNION
import urllib.request
import os

# url = 'https://minedbuildings.blob.core.windows.net/global-buildings/dataset-links.csv'
url = 'https://minedbuildings.z5.web.core.windows.net/global-buildings/dataset-links.csv'

path_dl = '/p/projects/eubucco/data/0-raw-data/microsoft-242'

## Functions

def check_all_countries_captured():
    if len(countries_eubucco)==len(set(df.Location)):
        print('All countries are here.')
    else:
        print(f'{[x for x in countries_eubucco if x not in set(df.Location)]} missing')

def check_all_parts_downloaded():
    file_names = []
    for dir_, _, files in os.walk(root_dir): 
        for file_name in files:
            file_names.append(int(str.split(file_name,'.')[0]))
    not_dl = [x for x in df.QuadKey if x not in file_names]
    if not_dl == []: print('All parts downloaded.')
    else: print(not_dl)

def download_msft():
    ''' download and store in folders (./GlobalMLBuildingFootprints/country/part)'''

    for location in set(df.Location):
        print(location)
        path_location = os.path.join(path_dl,location)
        os.mkdir(path_location)
        for _,part in df[df.Location==location].iterrows():
            print(part.QuadKey)
            urllib.request.urlretrieve(part.Url, os.path.join(path_location,str(part.QuadKey)+'.csv.gz'))

## Main 

# get the list of url to retrieve
df = pd.read_csv(url)
# df = pd.read_csv('../../Downloads/dataset-links.csv')
countries_eubucco = EUROPEAN_UNION.names + ['Switzerland','Norway','UnitedKingdom']
df = df[df.Location.isin(countries_eubucco)]

check_all_countries_captured()

# test
# df = df.sort_values(by='Size',ascending=True)[0:3]
# print(df)

# download and store in folders (./GlobalMLBuildingFootprints/country/part)
for location in set(df.Location):
    print(location)

    path_location = os.path.join(path_dl,location)
    os.mkdir(path_location)
    for _,part in df[df.Location==location].iterrows():
        print(part.QuadKey)
        urllib.request.urlretrieve(part.Url, os.path.join(path_location,str(part.QuadKey)+'.csv.gz'))

print('Done')
check_all_parts_downloaded()
