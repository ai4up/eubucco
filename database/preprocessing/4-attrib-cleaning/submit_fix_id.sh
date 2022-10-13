#!/bin/bash

#SBATCH --qos=short
#SBATCH --time=04:00:00
#SBATCH --job-name=fix_id-%A_%a
#SBATCH --account=eubucco
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --output=fix_id-%a.stdout
#SBATCH --error=fix_id-%a.stderr
#SBATCH --workdir=/p/tmp/fewagner/v0_1-db/fix_id/2022-10-10
#SBATCH --array=6,66,22 

pwd; hostname; date

module load anaconda
source activate /home/nikolami/.conda/envs/ox112

python -u /p/projects/eubucco/git-eubucco-tmp/database/preprocessing/4-attrib-cleaning/run_fix_id.py -i $SLURM_ARRAY_TASK_ID
