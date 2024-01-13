#!/usr/bin/env python
# Copyright (c) 2024 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    import_eccounter.py
# @author  Michael Behrisch
# @date    2024-01-09

import sys
import json

import openpyxl


def make_feature(data):
    feature = { "type":"Feature", "properties":{},
                "geometry":{ "type":"Point",
                             "coordinates":[data["Längengrad"], data["Breitengrad"]]}}
    feature["properties"]["name"] = data["Zählstelle"]
    feature["properties"]["description"] = data["Beschreibung - Fahrtrichtung"]
    feature["properties"]["first_data_package"] = data["Installationsdatum"].isoformat(" ")
    return feature


if __name__ == "__main__":
    container = { "type":"FeatureCollection", "features":[] }
    wb = openpyxl.open(sys.argv[1] if len(sys.argv) > 1 else 'gesamtdatei-stundenwerte.xlsx')
    for idx, line in enumerate(wb['Standortdaten']):
        content = [cell.value for cell in line]
        if idx == 0:
            header = content
        else:
            container["features"].append(make_feature(dict(zip(header, content))))
    with open("ecocounter.geojson", "w", encoding="utf8") as out:
        json.dump(container, out, indent=2)
