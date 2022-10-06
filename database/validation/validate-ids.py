import sys
import os
import json

PROJECT_SRC_PATH = os.path.realpath(os.path.join(__file__, '..', '..', '..', 'eubucco'))
sys.path.append(PROJECT_SRC_PATH)

from validation import validation # noqa: E402

# function parameters are passed by slurm-pipeline via stdin
params = json.load(sys.stdin)
print(params)
validation.validate_ids(**params)
