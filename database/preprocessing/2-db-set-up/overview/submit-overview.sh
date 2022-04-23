#!/bin/bash

#SBATCH --qos=short
#SBATCH --time=02:00:00
#SBATCH --job-name=ov_v0-%A_%a
#SBATCH --account=eubucco
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --output=ov_v0-out-%A_%a.txt
#SBATCH --error=ov_v0-err-%A_%a.txt
#SBATCH --workdir=/p/tmp/fewagner/db-setup/overview
#SBATCH --array=22

pwd; hostname; date

module load anaconda
source activate /home/nikolami/.conda/envs/ox112

python -u /p/projects/eubucco/git-eubucco/database/preprocessing/2-db-set-up/overview/run-overview.py -i $SLURM_ARRAY_TASK_ID

