#!/bin/bash
HOME=/home/we-count
ssh radtk.de@ssh.strato.de wecount/cgi-bin/update.py
cd $HOME/we-count
git fetch -q
# to make a new release update the tag / commit hash below
git checkout 0b61bbf052b42819a1f96f994fd03669c17637b8
../venv_wecount/bin/python src/we_count/backend/bzm_get_data.py
/usr/bin/pkill -HUP -F /run/gunicorn/we-count/pid

cd $HOME/we-count-beta
git pull -q
cp -a $HOME/we-count/src/assets/*.geojson $HOME/we-count/src/assets/*.csv.gz $HOME/we-count/src/assets/*.parquet src/assets/
rsync -abzL $HOME/we-count-beta/src/ radtk.de@ssh.strato.de:wecount/cgi-bin
