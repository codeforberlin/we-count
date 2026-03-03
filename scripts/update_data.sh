#!/bin/bash
# Copyright (c) 2023-2026 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    update_data.sh
# @author  Michael Behrisch
# @date    2023-01-15

# Run all data backup scripts which need to run hourly (or possibly more often).
# This script assumes the following structure:
# A checkout of the repo https://github.com/codeforberlin/we-count, where this script resides in a first level subdir.
# Beside this is a virtual env called venv_wecount with all dependenices from requirements.txt installed.
# Data will be generated into a parquet and a csv dir, the geojson does into csv as well.

cd $(dirname $0)/..
. ../venv_wecount/bin/activate
src/we_count/backend/telraam_backup.py -j csv/bzm_telraam_segments.geojson -p parquet/bzm_telraam_traffic_data.parquet -v --csv csv/bzm_telraam
