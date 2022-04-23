#!/bin/bash

#SBATCH --qos=short
#SBATCH --time=00:10:00
#SBATCH --job-name=match_nl-%A_%a
#SBATCH --account=eubucco
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --output=match_nl_out-%A_%a.txt
#SBATCH --error=match_nl_err-%A_%a.txt
#SBATCH --workdir=/p/tmp/fewagner/match/match_nl

pwd; hostname; date

module load anaconda
# module load osmium

source activate /home/fewagner/.conda/envs/urban_form_local

python -u /p/projects/eubucco/git-eubucco/database/preprocessing/1-parsing/matching/run_match_nl.py
