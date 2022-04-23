#!/bin/bash

#SBATCH --qos=short
#SBATCH --time=01:00:00
#SBATCH --job-name=conc_ital_-%A_%a
#SBATCH --account=eubucco
#SBATCH --partition=standard
#SBATCH --constraint=broadwell
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --mem=90000
#SBATCH --output=conc_ital_out-%A_%a.txt
#SBATCH --error=conc_ital_err-%A_%a.txt
#SBATCH --workdir=/p/tmp/fewagner/osm_split
#SBATCH --array=54


pwd; hostname; date

module load anaconda
#module load osmium

source activate /home/fewagner/.conda/envs/urban_form

python -u /p/projects/eubucco/git-eubucco/database/preprocessing/1-parsing/run-osm-concat.py -i $SLURM_ARRAY_TASK_ID
