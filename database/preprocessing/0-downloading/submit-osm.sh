#!/bin/bash

#SBATCH --qos=short
#SBATCH --job-name=osm-ddl-%A_%a
#SBATCH --partition=standard
#SBATCH --account=metab
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --output=downloading_osm_out-%A_%a.txt
#SBATCH --error=downloading_osm_err-%A_%a.txt
#SBATCH --partition=io
#SBATCH --qos=io

pwd; hostname; date

module load anaconda

source activate /home/nikolami/.conda/envs/urban_form

python -u downloading-openstreetmap.py
