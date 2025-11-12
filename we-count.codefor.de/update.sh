#!/bin/bash
HOME=/home/we-count
ssh radtk.de@ssh.strato.de wecount/cgi-bin/update.py
cd $HOME/we-count
git fetch -q
# to make a new release update the tag / commit hash below
git checkout 3bb4868836b67342d8b1f5dfe9fadc69a03b2f94
../venv_wecount/bin/python src/bzm_get_data.py
/usr/bin/pkill -HUP -F /run/gunicorn/we-count/pid

cd $HOME/we-count-beta
git pull -q
cp -a $HOME/we-count/src/assets/*.geojson $HOME/we-count/src/assets/*.csv.gz $HOME/we-count/src/assets/*.parquet src/assets/
rsync -abzL $HOME/we-count-beta/src/ radtk.de@ssh.strato.de:wecount/cgi-bin
