#!/bin/bash

#SBATCH --qos=short
#SBATCH --time=05:00:00
#SBATCH --job-name=ger_v2_-%A_%a
#SBATCH --account=eubucco
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --output=ger_v2_out-%A_%a.txt
#SBATCH --error=ger_v2_err-%A_%a.txt
#SBATCH --workdir=/p/tmp/fewagner/osm_split/v2
#SBATCH --array=2


pwd; hostname; date

module load anaconda
module load osmium

source activate /home/fewagner/.conda/envs/urban_form

python -u /p/projects/eubucco/git-eubucco/database/preprocessing/1-parsing/run-parse-osm-split.py -i $SLURM_ARRAY_TASK_ID
