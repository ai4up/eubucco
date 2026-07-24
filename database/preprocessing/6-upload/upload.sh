#!/bin/bash

# initial sync (no need to check for differences with --no-whole-file, since the target directory is empty)
ls /p/projects/eubucco/data/7-release/ | xargs -n 1 -P 8 -I {} rsync -avP --inplace /p/projects/eubucco/data/7-release/{} flo@194.163.151.34:/home/flo/eubucco-data/{}

# subsequent syncs (only transfer changed files, and only the changed parts of files) (can be parallelized with xargs too)
rsync -avP --inplace --no-whole-file /p/projects/eubucco/data/7-release/ flo@194.163.151.34:/home/flo/eubucco-data/

# additional files
scp /p/projects/eubucco/data/8-additional-files/eubucco_lat_lon.parquet  flo@194.163.151.34:/home/flo/
rsync -avP --inplace --no-whole-file --exclude "eubucco_lat_lon.parquet" /p/projects/eubucco/data/8-additional-files/ flo@194.163.151.34:/home/flo/eubucco-additional-files