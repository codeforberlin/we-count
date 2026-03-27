#!/bin/bash
HOME=/home/we-count
cd $HOME/we-count

# update local code
git fetch -q
# to make a new release update the tag / commit hash below
git checkout 5cd5fe92a0dabefaf87b9e14c6c3773233eaf1ed

# update local data and restart
scripts/update_data.sh
../venv_wecount/bin/python src/we_count/backend/bzm_get_data.py
/usr/bin/pkill -HUP -F /run/gunicorn/we-count/pid

# update remote code and data
rsync -a scripts src radtk.de@ssh.strato.de:we-count-code/
rsync -a csv/.htaccess csv/LICENSE* csv/READ_ME radtk.de@ssh.strato.de:we-count-code/csv/
ssh radtk.de@ssh.strato.de we-count-code/scripts/update_data.sh

# "self" update
cd $HOME/we-count-beta
git pull -q
