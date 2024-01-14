#!/usr/bin/env python
# Copyright (c) 2024 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    import_eccounter.py
# @author  Michael Behrisch
# @date    2024-01-09

import bisect
import datetime
import json
import math
import os
from urllib.request import urlopen, urlretrieve
import zoneinfo

import openpyxl
import timezonefinder

import backup_data
from common import get_options
from datamodel import TrafficCount


def update_db(segments, session, options, workbook, features):
    print("Retrieving data for %s segments" % len(segments))
    for s in segments.values():
        session.add(s)
        tzinfo=zoneinfo.ZoneInfo(s.timezone)
        first_year = s.last_backup_utc.year if s.last_backup_utc else 2012
        final_year = s.last_data_utc.year if s.last_data_utc else datetime.datetime.now().year
        counters = None
        for feature in features:
            if feature["properties"]["segment_id"] == s.id:
                counters = feature["properties"]["counter"]
                break
        if not counters:
            print("Warning: No counters for %s." % s.id)
            continue
        if options.verbose and first_year < final_year:
            print("Retrieving data for segment %s between %s and %s." %
                  (s.id, first_year, final_year))
        for year in range(first_year, final_year):
            sheet_name = "Jahresdatei %s" % year
            if sheet_name not in workbook:
                print("No sheet for %s." % year)
                continue
            column_lft = column_rgt = None
            for line in workbook["Jahresdatei %s" % year]:
                content = [cell.value for cell in line]
                if column_lft is None:
                    for idx, col in enumerate(content):
                        if col:
                            for cnt in counters:
                                if cnt["name"] in col:
                                    if column_lft is None:
                                        column_lft = idx
                                    elif column_rgt is None:
                                        column_rgt = idx
                                    else:
                                        print("Warning: Three matching columns for %s in %s." %
                                              (s.id, year))
                    if column_lft is None:
                        print("Warning: No matching columns for %s in %s." % (s.id, year))
                        break
                else:
                    lft = content[column_lft]
                    if lft == "":
                        lft = None
                    rgt = None if column_rgt is None else content[column_rgt]
                    if rgt == "":
                        rgt = None
                    if content[0] is not None and (lft is not None or rgt is not None):
                        entry = {"date": content[0].replace(tzinfo=tzinfo),
                                 "interval": "hourly", "uptime": 1,
                                 "bike_lft": lft, "bike_rgt": rgt}
                        tc = TrafficCount(entry)
                        idx = bisect.bisect(s.counts, tc.date_utc, key=lambda t: t.date_utc)
                        if not s.counts or s.counts[idx-1].date_utc != tc.date_utc:
                            s.counts.insert(idx, tc)
                        else:
                            s.counts[idx-1] = tc
        s.last_backup_utc = datetime.datetime.now(datetime.timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        session.commit()


def main(args=None):
    options = get_options(args, "ecocounter.geojson")
    if options.excel is None:
        url = 'https://www.berlin.de/sen/uvk/_assets/verkehr/verkehrsplanung/radverkehr/weitere-radinfrastruktur/zaehlstellen-und-fahrradbarometer/gesamtdatei-stundenwerte.xlsx'
        options.excel = os.path.basename(url)
        if not os.path.exists(options.excel):
            print("Downloading", options.excel)
            urlretrieve(url, options.excel)
    with urlopen('https://www.eco-visio.net/api/aladdin/1.0.0/pbl/publicwebpageplus/4728') as jin:
        ecojson = json.load(jin)

    container = { "type":"FeatureCollection", "features":[] }
    wb = openpyxl.open(options.excel)
    tf = timezonefinder.TimezoneFinder()
    header = None
    ecocounters = {}
    for line in wb['Standortdaten']:
        content = [cell.value for cell in line]
        if header is None:
            header = content
        else:
            data = dict(zip(header, content))
            counter = {"lon": data["Längengrad"], "lat": data["Breitengrad"],
                       "name": data["Zählstelle"],
                       "description": data["Beschreibung - Fahrtrichtung"],
                       "installation": data["Installationsdatum"].isoformat(" ")}
            lon, lat = data["Längengrad"], data["Breitengrad"]
            min_dist, idx = min([(math.hypot(lon - ec["lon"], lat - ec["lat"]), i) for i, ec in enumerate(ecojson)])
            if min_dist < 0.003:
                ec = ecojson[idx]
                segment_id = ec['idPdc']
                if segment_id in ecocounters:
                    ecocounters[segment_id]["properties"]["counter"].append(counter)
                else:
                    feature = {"type": "Feature",
                               "properties": {"segment_id": ec['idPdc'], "counter": [counter]},
                               "geometry": {"type": "Point", "coordinates": (ec["lon"], ec["lat"])}}
                    feature["properties"]["timezone"] = tf.timezone_at(lng=ec["lon"], lat=ec["lat"])
                    feature["properties"]["eco-counter"] = ec
                    ecocounters[segment_id] = feature
                    container["features"].append(feature)
    with open(options.json_file, "w", encoding="utf8") as out:
        json.dump(container, out, indent=2)
    session = backup_data.open_session(options)
    segments = backup_data.get_segments(session, options)
    update_db(segments, session, options, wb, container["features"])


if __name__ == "__main__":
    main()
