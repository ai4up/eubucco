import os
import sys

PROJECT_SRC_PATH = os.path.realpath(os.path.join(__file__, '..', '..', '..', '..', 'eubucco'))
sys.path.append(PROJECT_SRC_PATH)

from preproc.create_unique_ids import fix_id  # noqa: E402
from ufo_map.Utils.helpers import arg_parser  # noqa: E402

countries = ['netherlands', 'france', 'spain']
args = arg_parser(['i'])

fix_id(countries[args.i], path_db_folder='/p/projects/eubucco/data/2-database-city-level-v0_1')