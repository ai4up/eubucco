import os
import sys

PROJECT_SRC_PATH = os.path.realpath(os.path.join(__file__, '..', '..', '..', '..', 'eubucco'))
sys.path.append(PROJECT_SRC_PATH)

from preproc.parsing import parse  # noqa: E402

parse(path_to_param_file='/p/projects/eubucco/git-eubucco/database/preprocessing/1-parsing/inputs-parsing-v1-cluster-test.csv',
          path_output='/p/projects/eubucco/data/1-intermediary-outputs-v1-gov2',
          path_stats='/p/projects/eubucco/stats/1-parsing-v1/',
          test_run=True,
          arg_as_param=True
          )