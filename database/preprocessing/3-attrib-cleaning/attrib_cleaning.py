import os
import sys
import json

PROJECT_SRC_PATH = os.path.realpath(os.path.join(__file__, '..', '..', '..', '..', 'eubucco'))
sys.path.append(PROJECT_SRC_PATH)

from preproc.attribs import attrib_cleaning_post_conflation

# function parameters are passed by slurm-pipeline via stdin
params = json.load(sys.stdin)
print(params)
attrib_cleaning_post_conflation(**params)