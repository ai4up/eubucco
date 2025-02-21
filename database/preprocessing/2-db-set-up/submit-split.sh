#!/bin/bash

#SBATCH --qos=short
#SBATCH --job-name=split-%A_%a
#SBATCH --partition=standard
#SBATCH --account=eubucco
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --output=split_out-%A_%a.txt
#SBATCH --error=split_err-%A_%a.txt

pwd; hostname; date

module load anaconda

source activate /home/nikolami/.conda/envs/ox112

python -u split_fr.py
