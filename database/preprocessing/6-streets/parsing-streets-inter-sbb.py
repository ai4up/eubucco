import os
import sys

PROJECT_SRC_PATH = os.path.realpath(os.path.join(__file__, '..', '..', '..', '..', 'eubucco'))
sys.path.append(PROJECT_SRC_PATH)

from ufo_map.Utils.helpers import arg_parser  # noqa: E402
from preproc.streets import parse_streets  # noqa: E402

# utilize Slurm's concurrent job scheduling by mapping SLURM_ARRAY_TASK_ID to city indices
args = arg_parser([('i', int), ('c', str)])
city_idx = args.i
country = args.c

print(country)
print(city_idx)

parse_streets(country, city_idx)
