#!/bin/bash
# Copyright (c) 2023-2026 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    update_data.sh
# @author  Michael Behrisch
# @date    2023-01-15

# Run all data backup scripts once per day (if the segments have not been touched on the current day)
# and a reduced set on a regular schedule triggered by a cronjob.
# This script assumes the following structure:
# A checkout of the repo https://github.com/codeforberlin/we-count, where this script resides in a first level subdir (scripts).
# A secrets.json directly in the checkout dir.
# Beside this is a virtual env called venv_wecount with all dependencies from requirements_backend.txt installed.
# Data will be generated into a parquet and a csv dir, the geojson goes into csv as well.
# This script is also fine for an initial setup.

TELRAAM_SEGMENTS=csv/bzm_telraam_segments.geojson
cd $(dirname $0)/..
. ../venv_wecount/bin/activate
if [ "$(date -r $TELRAAM_SEGMENTS +%Y-%m-%d)" = "$(date +%Y-%m-%d)" ]; then
    src/we_count/backend/telraam_backup.py -j $TELRAAM_SEGMENTS --single-line-output csv/kibana/bzm_telraam_segments.geojson -p parquet/bzm_telraam_traffic_data.parquet -v --csv csv/bzm_telraam
else
    # first run of the day, complete backup
    src/we_count/backend/telraam_backup.py -j $TELRAAM_SEGMENTS --single-line-output csv/kibana/bzm_telraam_segments.geojson -p parquet/bzm_telraam_traffic_data.parquet --csv csv/bzm_telraam --csv-segments csv/segments/bzm_telraam --csv-start-year 2021 --max-prop-updates 500 -v --limit 10
    src/we_count/backend/telraam_backup.py -j $TELRAAM_SEGMENTS -p parquet/bzm_telraam_traffic_advanced.parquet -v --limit 10 --advanced
    src/we_count/backend/ecocounter_backup.py -j csv/bzm_ecocounter_segments.geojson -p parquet/bzm_ecocounter_traffic_data.parquet --csv csv/bzm_ecocounter --csv-segments csv/segments/bzm_ecocounter --csv-start-year 2015 -v
    src/we_count/backend/ecocounter_backup.py -j csv/bzm_ecocounter_segments.geojson -p parquet/bzm_ecocounter_traffic_advanced.parquet -v --advanced
    src/we_count/backend/teu_backup.py -j csv/bzm_teu_segments.geojson -p parquet/bzm_teu_traffic_data.parquet -v --limit 10
    src/we_count/backend/teu_backup.py -j csv/bzm_teu_segments.geojson -p parquet/bzm_teu_traffic_advanced.parquet -v --limit 10 --advanced
    src/we_count/backend/maut_backup.py -j csv/bzm_maut_segments.geojson -p parquet/bzm_maut_traffic_data.parquet -v --limit 10
    src/we_count/backend/vmk_import.py -j csv/bzm_vmk_2023.json
    src/we_count/backend/vmk_import.py -j csv/bzm_vmk_2019.json --year 2019
fi