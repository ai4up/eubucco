import os
import sys
import json

PROJECT_SRC_PATH = os.path.realpath(os.path.join(__file__, '..', '..', '..', '..', 'eubucco'))
sys.path.append(PROJECT_SRC_PATH)

from preproc.streets import download_osm_streets_country  # noqa: E402

# function parameters are passed by slurm-pipeline via stdin
params = json.load(sys.stdin)
print(params)

download_osm_streets_country(**params)
