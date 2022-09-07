import os
import sys
import pandas as pd

PROJECT_SRC_PATH = os.path.realpath(os.path.join(__file__, '..', '..', '..', '..', 'eubucco'))
sys.path.append(PROJECT_SRC_PATH)

from preproc.create_unique_ids import fix_id  # noqa: E402
from ufo_map.Utils.helpers import arg_parser  # noqa: E402

params_file_path = '/p/projects/eubucco/git-eubucco/database/preprocessing/1-parsing/inputs-parsing.csv'
args = arg_parser(['i'])
country = pd.read_csv(params_file_path).country.unique()[args.i]

fix_id(country, path_db_folder='/p/projects/eubucco/data/2-database-city-level-v0_1')