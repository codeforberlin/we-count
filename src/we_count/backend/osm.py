#!/usr/bin/env python3
# Copyright (c) 2024 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    osm.py
# @author  Michael Behrisch
# @date    2024-01-02

import datetime
import json
import os
from collections import Counter

import numpy as np

import common
import osmnx
import plotly.express as px
import requests
import shapely


def add_osm(features, old_data, max_updates=2):
    """Add OSM data to features in-place. old_data maps segment_id -> properties dict.
    Caches results for 30 days; updates at most max_updates features per call."""
    now = datetime.datetime.now(datetime.timezone.utc)
    update_count = 0
    for feature in features:
        segment_id = feature["properties"]["segment_id"]
        if segment_id in old_data and "osm" in old_data[segment_id]:
            osm_edge = old_data[segment_id]["osm"]
            if "name" in osm_edge:
                if common.parse_utc_dict(osm_edge, "last_osm_fetch") > now - datetime.timedelta(days=30):
                    feature["properties"]["osm"] = osm_edge
                    continue
        geometry = feature.get("geometry")
        if not geometry:
            continue
        update_count += 1
        if geometry["type"] == "Point":
            coords = [geometry["coordinates"]]
        elif geometry["type"] == "LineString":
            coords = geometry["coordinates"]
        else:  # MultiLineString
            coords = geometry["coordinates"][0]
        osm_edge = find_edge(coords)
        osm_edge["last_osm_fetch"] = now.isoformat()
        del osm_edge["geometry"]
        feature["properties"]["osm"] = osm_edge
        if update_count >= max_updates:
            break


def ensure_graph(coords, graph=None):
    if graph is not None:
        return graph
    center = coords[len(coords) // 2]
    # Download the street network for the specified location
    return osmnx.graph_from_point((center[1], center[0]), network_type='all')


def find_edge(coords, graph=None):
    graph = ensure_graph(coords, graph)
    geoms = osmnx.graph_to_gdfs(graph, nodes=False)["geometry"]

    # build an r-tree spatial index by position for subsequent iloc
    rtree = shapely.STRtree(geoms)
    bounds = np.add(shapely.bounds(shapely.MultiPoint(coords)), [-0.001, -0.001, 0.001, 0.001])
    pos = rtree.query_nearest(shapely.box(*bounds))

    min_dist = 1e14
    min_edge = None
    for ne in geoms.iloc[pos].index:
        osm_edge = graph.edges[ne]
        if "geometry" not in osm_edge:
            n1, n2 = graph.nodes[ne[0]], graph.nodes[ne[1]]
            osm_edge["geometry"] = shapely.LineString([(n1["x"], n1["y"]), (n2["x"], n2["y"])])
        max_point_dist = 0
        if osm_edge.get("highway") in (None, "cycleway", "footway", "path", "service"):
            max_point_dist = 1  # minor street penalty
        for p in coords:
            max_point_dist = max(max_point_dist, shapely.distance(shapely.Point(p), osm_edge["geometry"]))
        if max_point_dist < min_dist:
            min_edge = osm_edge
            min_dist = max_point_dist
    # Send a request to the Nominatim reverse geocoding API
    response = requests.get("https://nominatim.openstreetmap.org/reverse",
                            params={'lat': coords[0][1], 'lon': coords[0][0], 'format': 'json'},
                            headers={'User-Agent': 'bzm v0.1'})
    if response.status_code == 200:
        min_edge["address"] = response.json().get('address', {})
    return min_edge


def find_nearest(coords, graph=None):
    graph = ensure_graph(coords, graph)
    count = Counter({None:0})
    nearest_edges = osmnx.nearest_edges(graph, *zip(*coords))
    for ne in nearest_edges:
        osm_edge = graph.edges[ne]
        if "geometry" not in osm_edge:
            n1, n2 = graph.nodes[ne[0]], graph.nodes[ne[1]]
            osm_edge["geometry"] = shapely.LineString([(n1["x"], n1["y"]), (n2["x"], n2["y"])])
        count[ne] += 1
        for osm_lon, osm_lat in osm_edge["geometry"].coords:
            for lon, lat in coords:
                if abs(osm_lon - lon) < 1e-6 and abs(osm_lat - lat) < 1e-6:
                    count[ne] += 1
    v1, v2 = count.most_common(2)
    if v1[1] == v2[1]:
        print("undecided", v1, v2)
        return {"osmid": None}
    return graph.edges[v1[0]]


if __name__ == "__main__":
    with open(os.path.join(os.path.dirname(__file__), "assets", "sensor.json")) as geojson:
        segments = json.load(geojson)
    lats = []
    lons = []
    names = []
    for s in segments["features"]:
        coords = s["geometry"]["coordinates"][0]
        edge = find_edge(coords)
        near_edge = find_nearest(coords)
        if edge["osmid"] != near_edge["osmid"]:
            print("dist", edge)
            print("near", near_edge)
            x, y = edge["geometry"].xy
            lats = np.append(lats, y)
            lons = np.append(lons, x)
            names = np.append(names, [edge.get("name", "NN")]*len(y))
            lats = np.append(lats, None)
            lons = np.append(lons, None)
            names = np.append(names, None)

    fig = px.line_mapbox(lat=lats, lon=lons, hover_name=names,
                         mapbox_style='open-street-map')
    fig.show()
