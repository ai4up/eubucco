import os
import sys

PROJECT_SRC_PATH = os.path.realpath(os.path.join(__file__, '..', '..', '..', 'eubucco'))
sys.path.append(PROJECT_SRC_PATH)

from ft_eng.ft_eng import create_features  # noqa: E402
from ufo_map.Utils.helpers import arg_parser  # noqa: E402

# utilize Slurm's concurrent job scheduling by mapping SLURM_ARRAY_TASK_ID to city indices
args = arg_parser([('p', str)])
city_path = args.p

print(city_path)

create_features(city_path,
                bld=True,
                blk=True,
                bld_d=True,
                blk_d=True,
                int_=True,
                str_=True,
                sbb_=True,
                city_level=True,
                path_stats='/p/projects/eubucco/stats/5-ft-eng')
