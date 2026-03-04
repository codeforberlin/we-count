#!/bin/bash
# Copyright (c) 2023-2026 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    update_daily.sh
# @author  Michael Behrisch
# @date    2023-01-15

# Run the daily data backup scripts. For assumptions on the folder structure see update_data.sh.
# This script is also fine for an initial setup.

cd $(dirname $0)/..
. ../venv_wecount/bin/activate
src/we_count/backend/telraam_backup.py -j csv/bzm_telraam_segments.geojson --single-line-output csv/kibana/bzm_telraam_segments.geojson -p parquet/bzm_telraam_traffic_data.parquet --csv csv/bzm_telraam --csv-segments csv/segments/bzm_telraam --csv-start-year 2021 --max-prop-updates 500 -v --limit 10
src/we_count/backend/telraam_backup.py -j csv/bzm_telraam_segments.geojson -p parquet/bzm_telraam_traffic_advanced.parquet -v --limit 10 --advanced
src/we_count/backend/ecocounter_backup.py -j csv/bzm_ecocounter_segments.geojson -p parquet/bzm_ecocounter_traffic_data.parquet -v
src/we_count/backend/ecocounter_backup.py -j csv/bzm_ecocounter_segments.geojson -p parquet/bzm_ecocounter_traffic_advanced.parquet -v --advanced
src/we_count/backend/teu_backup.py -j csv/bzm_teu_segments.geojson -p parquet/bzm_teu_traffic_data.parquet -v --limit 10
src/we_count/backend/teu_backup.py -j csv/bzm_teu_segments.geojson -p parquet/bzm_teu_traffic_advanced.parquet -v --limit 10 --advanced
src/we_count/backend/maut_backup.py -j csv/bzm_maut_segments.geojson -p parquet/bzm_maut_traffic_data.parquet -v --limit 10
