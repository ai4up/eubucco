from pyrosm import get_data, OSM
from pyrosm.data import sources
import os


#EU_27 = ['austria','bulgaria','croatia','germany','greece','hungary','ireland_and_northern_ireland','italy','latvia','portugal','romania','sweden']

# 'belgium', 'bulgaria', 'croatia', 'cyprus', 'czech_republic', 'denmark', 'estonia',
#         'finland', 'france', 'germany', 'greece', 'hungary', 'ireland_and_northern_ireland', 'italy',
#         'latvia', 'lithuania', 'luxembourg', 'malta',  'netherlands', 'poland', 'portugal', 'romania',
#         'slovakia', 'slovenia', 'spain', 'sweden']


# EU_27 = [
#     'cyprus',
#     'czech_republic',
#     'denmark',
#     'estonia',
#     'finland',
#     'france',
#     'lithuania',
#     'luxembourg',
#     'malta',
#     'netherlands',
#     'poland',
#     'slovakia',
#     'slovenia',
#     'spain',
#     'switzerland']


# countries_to_import = ['austria','bulgaria','croatia','germany','greece','hungary',
#                        'ireland_and_northern_ireland','italy','latvia','romania','sweden']

countries_to_import = ['spain']

# countries_to_import =  #EU_27  # + ['switzerland']


countries_w_sub_regions = ['france', 'germany', 'italy', 'netherlands', 'poland']

dict_regions = {}

dict_regions['france'] = sources.subregions.france.available
dict_regions['germany'] = sources.subregions.germany.available
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
