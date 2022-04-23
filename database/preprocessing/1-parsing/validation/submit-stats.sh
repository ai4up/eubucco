#!/bin/bash

#SBATCH --qos=short
#SBATCH --time=00:50:00
#SBATCH --job-name=val_city_num-%A_%a
#SBATCH --account=eubucco
#SBATCH --partition=standard
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --output=val_city_num_out-%A_%a.txt
#SBATCH --error=val_city_num_err-%A_%a.txt
#SBATCH --workdir=/p/tmp/fewagner/stats
#SBATCH --array=1

pwd; hostname; date

module load anaconda
# module load osmium

source activate /home/fewagner/.conda/envs/urban_form_local

python -u /p/projects/eubucco/git-eubucco/database/preprocessing/1-parsing/validation/run-validation.py -i $SLURM_ARRAY_TASK_ID
