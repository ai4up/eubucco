import os
import sys
import json

PROJECT_SRC_PATH = os.path.realpath(os.path.join(__file__, '..', '..', '..', 'eubucco'))
sys.path.append(PROJECT_SRC_PATH)

from ft_eng import ft_concat # noqa: E402

# function parameters are passed by slurm-pipeline via stdin
params = json.load(sys.stdin)
print(params)

ft_concat.generate_dataset(**params)
