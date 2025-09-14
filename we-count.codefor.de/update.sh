#!/bin/bash
ssh radtk.de@ssh.strato.de wecount/cgi-bin/update.py
cd /home/we-count/we-count
git fetch -q
# to make a new release update the tag / commit hash below
git checkout 7aca2bc40c8a9d5ceabc2dac34fe3a164237bde1
../venv_wecount/bin/python src/bzm_get_data.py
/usr/bin/pkill -HUP -F /run/gunicorn/we-count/pid

cd /home/we-count/we-count-beta
git pull -q
cp -a /home/we-count/we-count/src/assets/* /home/we-count/we-count-beta/src/assets/
/usr/bin/pkill -HUP -F /run/gunicorn/we-count-beta/pid
