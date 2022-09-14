#!/bin/bash

#SBATCH --qos=short
#SBATCH --time=00:10:00
#SBATCH --job-name=floor_fix-%A_%a
#SBATCH --account=eubucco
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --output=floor_fix-%A_%a.stdout
#SBATCH --error=floor_fix-%A_%a.stderr
#SBATCH --workdir=/p/tmp/fewagner/v0_1-db/fix_id
#SBATCH --array=66

pwd; hostname; date

module load anaconda
source activate /home/nikolami/.conda/envs/ox112

python -u /p/projects/eubucco/git-eubucco/database/preprocessing/4-attrib-cleaning/run_fix_floors.py -i $SLURM_ARRAY_TASK_ID
