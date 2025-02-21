import os
import sys

PROJECT_SRC_PATH = os.path.realpath(os.path.join(__file__, '..', '..', '..', '..', 'eubucco'))
sys.path.append(PROJECT_SRC_PATH)

from preproc.parsing import parse  # noqa: E402

# parse(test_run=False)

parse(path_output='/p/projects/eubucco/data/1-intermediary-outputs-v1-osm',test_run=False)