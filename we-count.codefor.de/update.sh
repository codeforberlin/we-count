#!/bin/bash
cd /home/we-count/we-count
#git pull -q
../venv_wecount/bin/python src/bzm_get_data.py -o "traffic_df_%s.parquet"
/usr/bin/pkill -HUP -F /run/gunicorn/we-count/pid
