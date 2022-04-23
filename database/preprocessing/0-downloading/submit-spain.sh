#!/bin/bash

#SBATCH --qos=short
#SBATCH --job-name=spain-ddl-%A_%a
#SBATCH --partition=standard
#SBATCH --account=metab
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --output=downloading_spain_out-%A_%a.txt
#SBATCH --error=downloading_spain_err-%A_%a.txt
#SBATCH --partition=io
#SBATCH --qos=io

pwd; hostname; date

module load anaconda

source activate urban_form

python -u downloading-spain-cadaster.py
