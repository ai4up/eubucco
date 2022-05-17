#!/bin/bash

#SBATCH --qos=short
#SBATCH --time=12:00:00
#SBATCH --job-name=ft-eng-%A_%a
#SBATCH --account=eubucco
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --output=ft_eng_out-%A_%a.txt
#SBATCH --error=ft_eng_err-%A_%a.txt
#SBATCH --array=0-488
#SBATCH --workdir=/p/tmp/nikolami/ft-eng

pwd; hostname; date

module load anaconda

source activate ox112

python -u /p/projects/eubucco/git-eubucco/database/feature-engineering/fts.py -c 'netherlands' -i $SLURM_ARRAY_TASK_ID
