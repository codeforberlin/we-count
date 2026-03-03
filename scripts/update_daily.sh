#!/bin/bash
# Copyright (c) 2023-2026 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    update_daily.sh
# @author  Michael Behrisch
# @date    2023-01-15

# Run the daily data backup scripts. For assumptions on the folder structure see update_data.sh

cd $(dirname $0)/..
. ../venv_wecount/bin/activate
src/we_count/backend/telraam_backup.py -j csv/bzm_telraam_segments.geojson -p parquet/bzm_telraam_traffic_data.parquet -v --csv csv/bzm_telraam --csv-segments csv/segments/bzm_telraam --csv-start-year 2021 --max-prop-updates 500
src/we_count/backend/ecocounter_backup.py -j csv/bzm_ecocounter_segments.geojson -p parquet/bzm_ecocounter_traffic_data.parquet -v
src/we_count/backend/telraam_backup.py -j csv/bzm_telraam_segments.geojson -p parquet/bzm_telraam_traffic_advanced.parquet -v --limit 10 -a
