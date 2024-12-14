from pyrosm import get_data, OSM
from pyrosm.data import sources
import os


EU_27 = ['austria','belgium', 'bulgaria', 'croatia', 'cyprus', 'czech_republic', 'denmark', 'estonia',
        'finland', 'france', 'germany', 'greece', 'hungary', 'ireland_and_northern_ireland', 'italy',
        'latvia', 'lithuania', 'luxembourg', 'malta',  'netherlands', 'poland', 'portugal', 'romania',
        'slovakia', 'slovenia', 'spain', 'sweden']

countries_to_import =  EU_27  + ['switzerland','norway']

countries_w_sub_regions = ['france', 'germany', 'italy', 'netherlands', 'poland']

# uk - great_britain (with or without subregions didnt work)
# ran the command below directly in ~/uk folder via terminal
# wget https://download.geofabrik.de/europe/united-kingdom-latest.osm.pbf

dict_regions = {}

dict_regions['france'] = sources.subregions.france.available
dict_regions['germany'] = sources.subregions.germany.available # note: remove regbez
dict_regions['germany'] = [reg for reg in dict_regions['germany'] if 'regbez' not in reg] # <- not tested
dict_regions['italy'] = sources.subregions.italy.available
dict_regions['netherlands'] = sources.subregions.netherlands.available
dict_regions['poland'] = sources.subregions.poland.available

path_output = "/p/projects/eubucco/data/0-raw-data/osm-pbf-24"


for country in countries_to_import:

    print(country)

    try:
        os.mkdir(os.path.join(path_output, country))
    except FileExistsError:
        pass

    if country in countries_w_sub_regions:

        for region in dict_regions[country]:

            get_data(region, directory=os.path.join(path_output, country), update=True)

    else:

        get_data(country, directory=os.path.join(path_output, country), update=True)

