import os
import sys
import json

PROJECT_SRC_PATH = os.path.realpath(os.path.join(__file__, '..', '..', '..', 'eubucco'))
sys.path.append(PROJECT_SRC_PATH)

from ft_eng.ft_eng import create_features  # noqa: E402

# function parameters are passed by slurm-pipeline via stdin
params = json.load(sys.stdin)
print(params)

create_features(**params,
                bld=True,
                blk=True,
                bld_d=True,
                blk_d=True,
                int_=True,
                str_=True,
                sbb_=True,
                city_level=True,
                path_stats='/p/projects/eubucco/stats/5-ft-eng')
