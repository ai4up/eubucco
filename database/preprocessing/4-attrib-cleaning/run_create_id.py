import os
import sys

PROJECT_SRC_PATH = os.path.realpath(os.path.join(__file__, '..', '..', '..', '..', 'eubucco'))
sys.path.append(PROJECT_SRC_PATH)

from preproc.parsing import get_params  # noqa: E402
from preproc.create_unique_ids import create_id  # noqa: E402
from ufo_map.Utils.helpers import arg_parser  # noqa: E402

params_file_path = '/p/projects/eubucco/data/0-raw-data/id_look_up/country-ids'
args = arg_parser(['i'])
country = get_params(args.i, params_file_path)['country']

create_id(country,
          db_version=0.1,
          path_old_db_folder='/p/projects/eubucco/data/2-database-city-level',
          path_new_db_folder='/p/projects/eubucco/data/2-database-city-level-v0_1',
          path_root_id='/p/projects/eubucco/data/0-raw-data/id_look_up/country-ids')
