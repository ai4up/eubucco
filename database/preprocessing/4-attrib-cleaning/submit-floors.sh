#!/bin/bash

#SBATCH --qos=short
#SBATCH --time=01:00:00
#SBATCH --job-name=types-%A_%a
#SBATCH --account=eubucco
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --output=types-out-%A_%a.txt
#SBATCH --error=types-err-%A_%a.txt
#SBATCH --workdir=/p/tmp/fewagner/floors
#SBATCH --array=10,35,36,37,38,55,62,63,67


pwd; hostname; date

module load anaconda

source activate /home/nikolami/.conda/envs/ox112

python -u /p/projects/eubucco/git-eubucco/database/preprocessing/4-attrib-cleaning/floor_mapping.py -i $SLURM_ARRAY_TASK_ID
