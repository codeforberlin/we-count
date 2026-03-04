#!/bin/bash
HOME=/home/we-count
cd $HOME/we-count

# update local code
git fetch -q
# to make a new release update the tag / commit hash below
git checkout 54a28af94dc247363266000d49b78b17cef52ee0

# update local data and restart
scripts/update_data.sh
../venv_wecount/bin/python src/we_count/backend/bzm_get_data.py
/usr/bin/pkill -HUP -F /run/gunicorn/we-count/pid

# update remote code and data
rsync scripts src radtk.de@ssh.strato.de:wecount/
ssh radtk.de@ssh.strato.de wecount/scripts/update.sh

# "self" update
cd $HOME/we-count-beta
git pull -q
