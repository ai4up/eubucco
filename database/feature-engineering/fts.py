import os
import sys

PROJECT_SRC_PATH = os.path.realpath(os.path.join(__file__, '..', '..', '..', 'eubucco'))
sys.path.append(PROJECT_SRC_PATH)

from ft_eng.ft_eng import create_features
from ufo_map.Utils.helpers import arg_parser

# utilize Slurm's concurrent job scheduling by mapping SLURM_ARRAY_TASK_ID to city indices
args = arg_parser([('i', int), ('c', str)])
city_idx = args.i
country = args.c

print(country)
print(city_idx)

create_features(country,
                city_idx,
                bld=True,
                blk=True,
                bld_d=True,
                blk_d=True,
                int_=True,
                str_=True,
                sbb_=True,
                city_level=True,
                left_over=False,
                ua_mode=False,
                path_stats='/p/projects/eubucco/stats/5-ft-eng',
                data_dir='/p/projects/eubucco/data/2-database-city-level-v0_1')
