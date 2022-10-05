import os
import sys

PROJECT_SRC_PATH = os.path.realpath(os.path.join(__file__, '..', '..', '..', '..', 'eubucco'))
sys.path.append(PROJECT_SRC_PATH)

from preproc.create_submission import concate_release  # noqa: E402

concate_release(
    db_version=0.1,
    file_lim=20,
    path_db_folder='/p/projects/eubucco/data/2-database-city-level-v0_1',
    path_to_param_file='/p/projects/eubucco/git-eubucco/database/preprocessing/1-parsing/inputs-parsing.csv',
    path_root_id='/p/projects/eubucco/data/0-raw-data/id_look_up/country-ids',
    out_folder='/p/projects/eubucco/data/5-v0_2')
