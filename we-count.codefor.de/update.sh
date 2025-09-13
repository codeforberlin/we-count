#!/bin/bash
ssh radtk.de@ssh.strato.de wecount/cgi-bin/update.py
cd /home/we-count/we-count
#git pull -q
../venv_wecount/bin/python src/bzm_get_data.py
/usr/bin/pkill -HUP -F /run/gunicorn/we-count/pid
