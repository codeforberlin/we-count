#!/usr/bin/env python3
# Copyright (c) 2024 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    osm.py
# @author  Michael Behrisch
# @date    2024-01-02

import json
import os
from collections import Counter

import numpy as np
import osmnx
import plotly.express as px
import shapely


def ensure_graph(coords, graph=None):
    if graph is not None:
        return graph
    center = coords[len(coords) // 2]
    # Download the street network for the specified location
    return osmnx.graph_from_point((center[1], center[0]), network_type='all')


def find_edge(segment, graph=None):
    coords = segment["geometry"]["coordinates"][0]
    graph = ensure_graph(coords, graph)
    geoms = osmnx.utils_graph.graph_to_gdfs(graph, nodes=False)["geometry"]

    # build an r-tree spatial index by position for subsequent iloc
    rtree = shapely.STRtree(geoms)
    bounds = np.add(shapely.bounds(shapely.LineString(coords)), [-0.001, -0.001, 0.001, 0.001])
    pos = rtree.query_nearest(shapely.box(*bounds))

    min_dist = 1e14
    min_edge = None
    for ne in geoms.iloc[pos].index:
        osm_edge = graph.edges[ne]
        if "geometry" not in osm_edge:
            n1, n2 = graph.nodes[ne[0]], graph.nodes[ne[1]]
            osm_edge["geometry"] = shapely.LineString([(n1["x"], n1["y"]), (n2["x"], n2["y"])])
        max_point_dist = 0
        if osm_edge.get("highway") in (None, "footway", "service"):
            max_point_dist = 1  # minor street penalty
        for p in coords:
            max_point_dist = max(max_point_dist, shapely.distance(shapely.Point(p), osm_edge["geometry"]))
        if max_point_dist < min_dist:
            min_edge = osm_edge
            min_dist = max_point_dist
    return min_edge


def find_nearest(segment, graph=None):
    coords = segment["geometry"]["coordinates"][0]
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
        edge = find_edge(s)
        near_edge = find_nearest(s)
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
