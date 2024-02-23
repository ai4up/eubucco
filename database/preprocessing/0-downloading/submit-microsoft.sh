#!/bin/bash

#SBATCH --qos=short
#SBATCH --job-name=microsoft-%A_%a
#SBATCH --partition=standard
#SBATCH --account=eubucco
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --output=microsoft_out-%A_%a.txt
#SBATCH --error=microsoft_err-%A_%a.txt
#SBATCH --partition=io
#SBATCH --qos=io

pwd; hostname; date

module load anaconda

source activate /home/nikolami/.conda/envs/urban_form

python -u downloading-microsoft.py
