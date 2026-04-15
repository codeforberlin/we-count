#!/bin/bash
HOME=/home/we-count
cd $HOME/we-count

# update local code
git fetch -q
# to make a new release update the tag / commit hash below
git checkout 07f4b0f1e46528c248a316e6263fd37574337aab

# update local data and restart
scripts/update_data.sh /srv/www/
../venv_wecount/bin/python src/we_count/backend/bzm_get_data.py
/usr/bin/pkill -HUP -F /run/gunicorn/we-count/pid

# update remote code and data
rsync -a scripts src radtk.de@ssh.strato.de:we-count-code/
rsync -a csv/.htaccess csv/LICENSE* csv/READ_ME radtk.de@ssh.strato.de:we-count-code/csv/
ssh radtk.de@ssh.strato.de we-count-code/scripts/update_data.sh

# "self" update
cd $HOME/we-count-beta
git pull -q
