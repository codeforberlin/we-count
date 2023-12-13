#!/usr/bin/env python3
# Copyright (c) 2023 Michael Behrisch
# SPDX-License-Identifier: MIT

from dash import Dash, html, dcc, Output, Input
from dash_extensions.javascript import assign
import dash_leaflet as dl

from api import json_api


# css for grayscale is loaded from separate file,
# see https://stackoverflow.com/questions/50844844/python-dash-custom-css
# and https://stackoverflow.com/questions/32684470/openstreetmap-grayscale-tiles

# geojson is loaded from file for performance reasons, could be transfered to pbf someday
# see https://www.dash-leaflet.com/components/vector_layers/geojson
app = Dash(__name__, requests_pathname_prefix="/cgi-bin/map.cgi/" if __name__ != '__main__' else None)

attribution='&nbsp;|&nbsp;'.join(['&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>',
                                  '<a href="https://telraam.net">Telraam</a>'])

# Create drop down options.
dd_options = [dict(value=c, label=c) for c in ["active", "non-active"]]
dd_defaults = [o["value"] for o in dd_options]
# Create javascript function that filters on feature name.
geojson_filter = assign("""function(feature, context){
                        const active = feature.properties.uptime === 0;
                        return (active && context.hideout.includes('active')) || (!active && context.hideout.includes('non-active'));
                        }""")
# geojson_filter = assign("function(feature, context){return context.hideout.includes(feature.properties.name);}")
# Create example app.
app.layout = html.Div([
    dl.Map(children=[
        dl.TileLayer(className='bw', attribution=attribution),
        dl.GeoJSON(url='assets/segments.geojson', filter=geojson_filter, hideout=dd_defaults, id="geojson")
    ], style={'height': '80vh'}, center=(52.45, 13.55), zoom=11),
    dcc.Dropdown(id="dd", value=dd_defaults, options=dd_options, clearable=False, multi=True)
])
# Link drop down to geojson hideout prop (could be done with a normal callback, but clientside is more performant).
app.clientside_callback("function(x){return x;}", Output("geojson", "hideout"), Input("dd", "value"))

app.server.register_blueprint(json_api)

if __name__ == '__main__':
    app.run_server(debug=True)
