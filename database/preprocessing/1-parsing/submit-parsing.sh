#!/bin/bash

#SBATCH --qos=short
#SBATCH --time=04:00:00
#SBATCH --job-name=parse-%A_%a
#SBATCH --account=eubucco
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --output=parse-out-%A_%a.txt
#SBATCH --error=parse-err-%A_%a.txt
#SBATCH --workdir=/p/tmp/fewagner/parse_shp
#SBATCH --array=18

pwd; hostname; date

module load anaconda
module load osmium

source activate /home/nikolami/.conda/envs/ox112

python -u /p/projects/eubucco/git-eubucco/database/preprocessing/1-parsing/main-parsing.py -i $SLURM_ARRAY_TASK_ID
