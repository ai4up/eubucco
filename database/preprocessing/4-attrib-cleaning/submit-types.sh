#!/bin/bash

#SBATCH --qos=short
#SBATCH --time=02:00:00
#SBATCH --job-name=types-%A_%a
#SBATCH --account=eubucco
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --output=types-out-%A_%a.txt
#SBATCH --error=types-err-%A_%a.txt
#SBATCH --workdir=/p/tmp/nikolami
#SBATCH --array=45


pwd; hostname; date

module load anaconda

source activate /home/nikolami/.conda/envs/ox112

python -u /p/projects/eubucco/git-eubucco/database/preprocessing/4-attrib-cleaning/type_mapping.py -i $SLURM_ARRAY_TASK_ID
