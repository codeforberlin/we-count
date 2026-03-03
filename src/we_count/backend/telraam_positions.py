#!/usr/bin/env python3
# Copyright (c) 2023-2024 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    telraam_positions.py
# @author  Michael Behrisch
# @date    2023-01-15

import datetime
import json
import sys

import osm
import common


def update_props(bbox_segments, old_data, conns, retry, max_prop_updates):
    now = datetime.datetime.now(datetime.UTC)
    update_count = 0  # do not update too many at once, it is costly
    new_segments = []
    for segment_id in sorted(bbox_segments):
        old_segment = None
        if segment_id in old_data:
            old_segment = old_data[segment_id]
            last_prop_update = common.parse_utc_dict(old_segment["properties"], "last_prop_fetch")
            if update_count >= max_prop_updates or last_prop_update > now - datetime.timedelta(days=1):
                new_segments.append(old_segment)
                continue
        update_count += 1
        segment_data = conns.request("/v1/segments/id/%s" % segment_id, retries=retry, required="features")
        if not segment_data.get('features'):
            continue
        segment = segment_data["features"][0]
        segment["properties"] = {"segment_id": segment_id, "last_prop_fetch": now.isoformat()} | segment["properties"]
        if old_segment:
            for keep in ("osm", "last_data_backup", "last_advanced_backup"):
                if keep in old_segment["properties"]:
                    segment["properties"][keep] = old_segment["properties"][keep]
        new_segments.append(segment)
    invalid = set(old_data.keys()) - bbox_segments
    if invalid:
        print(f"The following segments are not in the database: {invalid}.", file=sys.stderr)
        for s in invalid:
            new_segments.append(old_data[s])
    return new_segments


def main(args=None):
    options = common.get_options(args)
    old_features = common.load_json_if_stale(options.json_file, options.clear, options.verbose)
    if old_features is None:
        return False
    old_data = {s["properties"]["segment_id"]: s for s in old_features}

    conns = common.ConnectionProvider(options.secrets["tokens"], options.url)
    res = conns.request("/v1/segments/area", "POST", str({"area": options.bbox}), retries=options.retry, required="features")
    bbox_segments = set(f["properties"]["segment_id"] for f in res.get("features", []))
    if options.verbose:
        print(f"{len(bbox_segments)} total sensor positions in the bounding box.")

    created_at = datetime.datetime.now(datetime.UTC)
    res = {
        "we_count_version": 2,
        "description": f"Telraam segments and instances for {options.bbox} enhanced with OpenStreetMap data, "
        "format description at https://app.swaggerhub.com/apis-docs/telraam/Telraam-API/1.2.0#/Segments/get_v1_segments_id__segment_id_",
        "created_at": created_at.isoformat(),
        "type": "FeatureCollection",
        "features": update_props(bbox_segments, old_data, conns, options.retry, options.max_prop_updates)
    }
    osm.add_osm(res["features"], {sid: f["properties"] for sid, f in old_data.items()})
    common.save_json(options.json_file, res)
    if options.single_line_output:
        with open(options.single_line_output, "w", encoding="utf8") as out:
            for segment in res["features"]:
                s = {"last_update": created_at.isoformat(" ")[:-16], "geometry": segment["geometry"],
                     **segment["properties"], "segment_id": str(segment["properties"]["segment_id"])}
                if "osm" in s:
                    s["osm"]["osmid"] = str(s["osm"]["osmid"])
                if s.get("instance_ids"):
                    s["cameras"] = [{"instance_id": str(iid),
                                     **{k: str(v) if k[-3:] in ("mac", "_id") else v for k, v in inst.items()}}
                                    for iid, inst in s["instance_ids"].items()]
                    del s["instance_ids"]
                    json.dump(s, out)
                    out.write("\n")
    return True


if __name__ == "__main__":
    main()
