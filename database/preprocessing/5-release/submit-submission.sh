#!/bin/bash

#SBATCH --qos=short
#SBATCH --time=00:30:00
#SBATCH --job-name=sub_test-%A_%a
#SBATCH --account=eubucco
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --output=sub_test-out-%A_%a.txt
#SBATCH --error=sub_test-err-%A_%a.txt
#SBATCH --workdir=/p/tmp/fewagner/v0_2-db/submission
#SBATCH --array=10


pwd; hostname; date

module load anaconda
source activate /home/nikolami/.conda/envs/ox112

python -u /p/projects/eubucco/git-eubucco/database/preprocessing/5-release/run_create_submission.py -i $SLURM_ARRAY_TASK_ID
