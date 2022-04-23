#!/bin/bash

#SBATCH --qos=short
#SBATCH --time=01:00:00
#SBATCH --job-name=id-%A_%a
#SBATCH --account=eubucco
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=6
#SBATCH --output=id-out-%A_%a.txt
#SBATCH --error=id-err-%A_%a.txt
#SBATCH --workdir=/p/tmp/fewagner/db-setup/v0_1
#SBATCH --array=5,10,14,17,56,57,60,68

pwd; hostname; date

module load anaconda
source activate /home/nikolami/.conda/envs/ox112

python -u /p/projects/eubucco/git-eubucco/database/preprocessing/4-attrib-cleaning/run_create_id.py -i $SLURM_ARRAY_TASK_ID
