#$ -cwd 
#$ -V
#$ -q short.qc
#$ -pe shmem 2
#$ -N notebook
#$ -o jupyter_session

PORT=$(shuf -i10000-11999 -n1)
echo "executing jupyter on http://$(hostname):$PORT"

source /users/johnson/$USER/devel/venv/3.8.2-GCCcore-9.3.0/bin/activate

jupyter-notebook --no-browser --port=$PORT --ip=`hostname -i`
