import os
import sys

PROJECT_SRC_PATH = os.path.realpath(os.path.join(__file__, '..', '..', '..', '..', 'eubucco'))
sys.path.append(PROJECT_SRC_PATH)

from preproc.parsing import parse  # noqa: E402

# parse(test_run=False)

# microsoft
parse(path_output='/p/projects/eubucco/data/1-intermediary-outputs-osm-circeular',test_run=False)
# parse(path_output='/p/projects/eubucco/data/1-intermediary-outputs-microsoft-242',test_run=False)

