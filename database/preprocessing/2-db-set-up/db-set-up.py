import sys
import os

PROJECT_SRC_PATH = os.path.realpath(os.path.join(__file__, '..', '..', '..', '..', 'eubucco'))

sys.path.append(PROJECT_SRC_PATH)

from preproc.db_set_up import db_set_up

# only select sepa mode with separated countries like Germany, Austria, Italy,...
db_set_up(parse_single=True, chunksize=int(5E5), folders=True, boundaries=True, bldgs=True, auto_merge=True, sepa_mode=False)


















