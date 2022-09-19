import sys
import os
import json

PROJECT_SRC_PATH = os.path.realpath(os.path.join(__file__, '..', '..', '..', '..', 'eubucco'))

sys.path.append(PROJECT_SRC_PATH)

from preproc.db_set_up import db_set_up  # noqa: E402

# function parameters are passed by slurm-pipeline via stdin
params = json.load(sys.stdin)
print(params)

db_set_up(
    **params,
    chunksize=int(5E5),
    folders=True,
    boundaries=True,
    bldgs=True,
    auto_merge=True)
