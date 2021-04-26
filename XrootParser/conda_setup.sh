export APP=/dune/app/users/$USER
source $APP/miniconda/etc/profile.d/conda.sh
export DBLUE=/dune/data/users/$USER
export XROOT=$DBLUE/data-mgmt-testing/XrootParser
conda activate my_root_env
export PYTHONPATH=$APP/sam-web-client/python:${PYTHONPATH}
