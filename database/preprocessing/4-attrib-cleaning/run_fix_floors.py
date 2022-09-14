import os
import sys
import pandas as pd

PROJECT_SRC_PATH = os.path.realpath(os.path.join(__file__, '..', '..', '..', '..', 'eubucco'))
sys.path.append(PROJECT_SRC_PATH)

from preproc.attribs import fix_floor  # noqa: E402
from preproc.parsing import get_params  # noqa: E402
#from preproc.parsing import get_params
from ufo_map.Utils.helpers import arg_parser  # noqa: E402

# define constants
FLOOR_HEIGHT = 3 #m

#--- arg parser---#
# utilize Slurm's concurrent job scheduling by mapping SLURM_ARRAY_TASK_ID to city indices
# args = arg_parser([('i', int), ('c', str)])
# city_idx = args.i
# country = args.c

# print(country)
# print(city_idx)
# print('----------')

path_input_parsing='/p/projects/eubucco/git-eubucco/database/preprocessing/1-parsing/inputs-parsing.csv'
args = arg_parser(['i'])
p = get_params(args.i, path_input_parsing)

print('---')
print(args.i)
print(p['country'])
print(p['dataset_name'])

# run fix floor
fix_floor(args.i,p,FLOOR_HEIGHT, test=True)