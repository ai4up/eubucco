import os
import sys

PROJECT_SRC_PATH = os.path.realpath(os.path.join(__file__, '..', '..', '..', '..', 'eubucco'))
sys.path.append(PROJECT_SRC_PATH)

from preproc.create_unique_ids import create_id

create_id(db_version = 0.1,
        path_to_param_file = '/p/projects/eubucco/git-eubucco/database/preprocessing/1-parsing/inputs-parsing.csv', 
        path_old_db_folder = '/p/projects/eubucco/data/2-database-city-level',
        path_new_db_folder = '/p/projects/eubucco/data/2-database-city-level-v0_1',
        path_root_id = '/p/projects/eubucco/data/0-raw-data/id_look_up/country-ids')
