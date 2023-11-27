#!/usr/bin/env python3

import os
import dash
from dash import dcc, html
import dash_leaflet as dl
import plotly.express as px
import pandas as pd


app = dash.Dash(__name__)
my_map_style = {'width': '100%', 'height': '800px'}
app.layout = dl.Map(dl.TileLayer(), center=(52.45, 13.55), zoom=11, style=my_map_style)


if __name__ == '__main__':
    app.run_server(debug=True)
