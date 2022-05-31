import os
import sys

PROJECT_SRC_PATH = os.path.realpath(os.path.join(__file__, '..', '..', '..', '..', 'eubucco'))
sys.path.append(PROJECT_SRC_PATH)

from ufo_map.Utils.helpers import arg_parser  # noqa: E402
from preproc.streets import download_osm_streets, download_osm_streets_country  # noqa: E402

# utilize Slurm's concurrent job scheduling by mapping SLURM_ARRAY_TASK_ID to city indices
args = arg_parser([('p', str)])
city_path = args.p
print(city_path)

if city_path:
    download_osm_streets(city_path)
else:
    download_osm_streets_country()
