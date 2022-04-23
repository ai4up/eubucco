#!/bin/bash

#SBATCH --qos=short
#SBATCH --time=01:00:00
#SBATCH --job-name=db_5e5-%A_%a
#SBATCH --account=eubucco
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=6
#SBATCH --output=db_5e5-out-%A_%a.txt
#SBATCH --error=db_5e5-err-%A_%a.txt
#SBATCH --workdir=/p/tmp/fewagner/db-setup/test
#SBATCH --array=35

pwd; hostname; date

module load anaconda
source activate /home/nikolami/.conda/envs/ox112

python -u /p/projects/eubucco/git-eubucco/database/preprocessing/2-db-set-up/db-set-up.py -i $SLURM_ARRAY_TASK_ID

