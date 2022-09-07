#!/bin/bash

#SBATCH --qos=short
#SBATCH --time=04:00:00
#SBATCH --job-name=id-alignment-%A_%a
#SBATCH --account=eubucco
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --output=id-alignment-%j.stdout
#SBATCH --error=id-alignment-%j.stderr
#SBATCH --workdir=/p/tmp/fewagner/v0_1-db/fix_id
#SBATCH --array=1-8,10-18,20-24,26,27

pwd; hostname; date

module load anaconda
source activate /home/nikolami/.conda/envs/ox112

python -u /p/projects/eubucco/git-eubucco/database/preprocessing/4-attrib-cleaning/run_fix_id.py -i $SLURM_ARRAY_TASK_ID
