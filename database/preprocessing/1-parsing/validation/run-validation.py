import os
import sys

PROJECT_SRC_PATH = os.path.realpath(os.path.join(__file__, '..', '..', '..', '..', '..', 'eubucco'))
sys.path.append(PROJECT_SRC_PATH)

from utils.validation_funcs import get_num_rows_city  # noqa: E402

get_num_rows_city()
