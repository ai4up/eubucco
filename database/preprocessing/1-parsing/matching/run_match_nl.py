import os
import sys

PROJECT_SRC_PATH = os.path.realpath(os.path.join(__file__, '..', '..', '..', '..', '..', 'eubucco'))
sys.path.append(PROJECT_SRC_PATH)

from utils.extra_cases import match_netherlands  # noqa: E402

match_netherlands()
