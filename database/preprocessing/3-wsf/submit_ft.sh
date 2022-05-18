#!/bin/bash

#SBATCH --time=02:00:00
#SBATCH --job-name=wsf-%A_%a
#SBATCH --qos=short
#SBATCH --account=eubucco
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --output=wsf_out-%A_%a.txt
#SBATCH --error=wsf_err-%A_%a.txt
#SBATCH --workdir=/p/tmp/nikolami/wsf
#SBATCH --array=10

pwd; hostname; date

module load anaconda

source activate /home/nikolami/.conda/envs/ox112

python -u /p/projects/eubucco/git-eubucco/database/preprocessing/3-wsf/wsf_tot_ft_area.py -i $SLURM_ARRAY_TASK_ID
