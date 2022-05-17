#!/bin/bash

#SBATCH --qos=short
#SBATCH --time=06:00:00
#SBATCH --job-name=parsing_street-%A_%a
#SBATCH --account=eubucco
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --output=parsing_street_out-%A_%a.txt
#SBATCH --error=parsing_street_err-%A_%a.txt
#SBATCH --array=0-488
#SBATCH --workdir=/p/tmp/nikolami

pwd; hostname; date

module load anaconda

source activate /home/nikolami/.conda/envs/ox112

python -u /p/projects/eubucco/git-eubucco/database/preprocessing/6-streets/parsing-streets-inter-sbb.py -c 'netherlands' -i $SLURM_ARRAY_TASK_ID
