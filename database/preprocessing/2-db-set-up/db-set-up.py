import sys
import os
import json

PROJECT_SRC_PATH = os.path.realpath(os.path.join(__file__, '..', '..', '..', '..', 'eubucco'))
sys.path.append(PROJECT_SRC_PATH)

import preproc # noqa: E402

# function parameters are passed by slurm-pipeline via stdin
params = json.load(sys.stdin)
print(params)
preproc.db_set_up(**params)
