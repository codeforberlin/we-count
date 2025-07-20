#!/bin/bash
systemctl stop we-count

sudo -u we-count \
     /home/we-count/venv_wecount/bin/python \
     /home/we-count/we-count/src/bzm_get_data_stand_alone_v01.py

systemctl start we-count
