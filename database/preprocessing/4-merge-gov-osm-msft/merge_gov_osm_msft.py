import os
import sys
import json

PROJECT_SRC_PATH = os.path.realpath(os.path.join(__file__, '..', '..', '..', '..', 'eubucco'))
sys.path.append(PROJECT_SRC_PATH)

from preproc.merge import merge_gov_osm_msft

# function parameters are passed by slurm-pipeline via stdin
params = json.load(sys.stdin)
print(params)
merge_gov_osm_msft(**params)