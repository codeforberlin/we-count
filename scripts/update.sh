#!/bin/bash
HOME=/home/we-count
cd $HOME/we-count

# update local code
git fetch -q
# to make a new release update the tag / commit hash below
git checkout b265b7c0067df310c0cf2d3227832e10063ade56

# update local data and restart
scripts/update_data.sh
../venv_wecount/bin/python src/we_count/backend/bzm_get_data.py
/usr/bin/pkill -HUP -F /run/gunicorn/we-count/pid

# update remote code and data
# TODO the code should go in a different directory to avoid public exposure
rsync -a scripts src radtk.de@ssh.strato.de:wecount/
rsync -a csv/.htaccess csv/LICENSE* csv/READ_ME radtk.de@ssh.strato.de:wecount/csv/
ssh radtk.de@ssh.strato.de wecount/scripts/update_data.sh

# "self" update
cd $HOME/we-count-beta
git pull -q
