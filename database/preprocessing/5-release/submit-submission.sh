#!/bin/bash

#SBATCH --qos=short
#SBATCH --time=02:00:00
#SBATCH --job-name=sub-%A_%a
#SBATCH --account=eubucco
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=6
#SBATCH --output=sub-out-%A_%a.txt
#SBATCH --error=sub-err-%A_%a.txt
#SBATCH --workdir=/p/tmp/fewagner/submission
#SBATCH --array=1,5,58


pwd; hostname; date

module load anaconda
source activate /home/nikolami/.conda/envs/ox112

python -u /p/projects/eubucco/git-eubucco/database/preprocessing/5-release/run_create_submission.py -i $SLURM_ARRAY_TASK_ID
