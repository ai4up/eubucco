import os
import sys

PROJECT_SRC_PATH = os.path.realpath(os.path.join(__file__, '..', '..', '..', '..', 'eubucco'))
sys.path.append(PROJECT_SRC_PATH)

from preproc.parsing import parse_osm_split  # noqa: E402


# france		-> 75; array: 0-26
# germany		-> 35; array: 0-32
# italy 		-> 54; array: 0-4
# netherlands 	-> 79; array: 0-14
# poland 		-> 80; array: 0-17

parse_osm_split(35, test_run=False)
