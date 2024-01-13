#!/usr/bin/env python
# Copyright (c) 2024 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    import_eccounter.py
# @author  Michael Behrisch
# @date    2024-01-09

import json
import math
import os
import sys
from urllib.request import urlopen, urlretrieve

import openpyxl
import timezonefinder


def make_feature(data, tf):
    lon, lat = data["Längengrad"], data["Breitengrad"]
    feature = { "type":"Feature", "properties":{"segment_id" : None},
                "geometry":{ "type":"Point", "coordinates":[lon, lat]}}
    feature["properties"]["name"] = data["Zählstelle"]
    feature["properties"]["description"] = data["Beschreibung - Fahrtrichtung"]
    feature["properties"]["first_data_package"] = data["Installationsdatum"].isoformat(" ")
    feature["properties"]["timezone"] = tf.timezone_at(lng=lon, lat=lat)
    return feature


if __name__ == "__main__":
    if len(sys.argv) == 1:
        url = 'https://www.berlin.de/sen/uvk/_assets/verkehr/verkehrsplanung/radverkehr/weitere-radinfrastruktur/zaehlstellen-und-fahrradbarometer/gesamtdatei-stundenwerte.xlsx'
        excel = os.path.basename(url)
        if not os.path.exists(excel):
            print("Downloading", excel)
            urlretrieve(url, excel)
        with urlopen('https://www.eco-visio.net/api/aladdin/1.0.0/pbl/publicwebpageplus/4728') as jin:
            ecojson = json.load(jin)
    else:
        excel = sys.argv[1]
        ecojson = None

    container = { "type":"FeatureCollection", "features":[] }
    wb = openpyxl.open(excel)
    tf = timezonefinder.TimezoneFinder()
    for idx, line in enumerate(wb['Standortdaten']):
        content = [cell.value for cell in line]
        if idx == 0:
            header = content
        else:
            f = make_feature(dict(zip(header, content)), tf)
            if ecojson:
                f_lon, f_lat = f["geometry"]["coordinates"]
                min_dist, idx = min([(math.hypot(f_lon - ec["lon"], f_lat - ec["lat"]), i) for i, ec in enumerate(ecojson)])
                if min_dist < 0.003:
                    f["properties"]["segment_id"] = ecojson[idx]['idPdc']
                    f["properties"]["eco-counter"] = ecojson[idx]
            container["features"].append(f)
    with open("ecocounter.geojson", "w", encoding="utf8") as out:
        json.dump(container, out, indent=2)
