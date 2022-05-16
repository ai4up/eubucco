#!/bin/bash

#SBATCH --job-name=ddl_street-%A_%a
#SBATCH --partition=io 
#SBATCH --qos=io
#SBATCH --account=eubucco
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --output=ddl_street_out-%A_%a.txt
#SBATCH --error=ddl_street_err-%A_%a.txt
#SBATCH --workdir=/p/tmp/fewagner/v0_2-db/streets

pwd; hostname; date

module load anaconda

source activate /home/nikolami/.conda/envs/ox112

python -u /p/projects/eubucco/git-eubucco/database/preprocessing/6-streets/downloading-streets.py 
