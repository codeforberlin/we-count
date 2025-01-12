#!/usr/bin/env python3
# Copyright (c) 2024-2025 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    bzm_v01.py
# @author  Egbert Klaassen
# @date    2025-01-07

# traffic_df    - dataframe, traffic data file
# geo_df        - geo dataframe, street coordinates for px.map
# json_df       - json dataframe (using the same geojson as the geo_df), to access features such as street names

import os
# from tkinter import filedialog
import pandas as pd
import geopandas as gpd
from pandas import json_normalize
import shapely.geometry
import numpy as np
import plotly.express as px
from dash import Dash, html, dash_table, dcc, callback, Output, Input
import dash_bootstrap_components as dbc

DEPLOYED = __name__ != '__main__'


# Save df files for development/debugging purposes
def save_df(df, file_name):
    # Save data frame for debugging purposes
    path = 'D:/OneDrive/PycharmProjects/we-count/src/assets/'
    print('Saving '+ path + file_name)
    df.to_excel(path + file_name + '.xlsx', index=False)


#### Retreive Data ####

# Read geojson data file to access geometry coordinates - using URL
geojson_url = 'https://berlin-zaehlt.de/csv/bzm_telraam_segments.geojson'
if not DEPLOYED:
    print('Reading geojson data...')
geo_df = gpd.read_file(geojson_url)

# Read geojson data file to access geometry coordinates - using local file
# geo_json_path = e.g. 'D:/.../.../bzm/App/assets/'
# geo_json_filename = e-g- 'bzm_segments_2025.geojson'
# print('Reading geojson data...')
# geo_df = gpd.read_file(geo_json_path + geo_json_filename)
# Save file
# geo_df.to_file('D:/.../.../bzm/App/assets/.. .geojson')

# Read geojson data file to access geometry coordinates - using local file selected through file dialog
# geo_json_path = e.g. 'D:/.../.../bzm/App/assets/'
# geo_json_filename = filedialog.askopenfilename(title='Select geojson data file', filetypes=[('geojson files', '*.geojson')])
# geo_df = gpd.read_file(geo_json_path + geo_json_filename)

# Flatten json data file to access properties such as street names for map representation
# Using url
if not DEPLOYED:
    print('Reading json data...')
json_df = pd.read_json(geojson_url)
# Using local filename
# json_df = pd.read_json(geo_json_path + geo_json_filename)
json_df_features = json_normalize(json_df['features'])

# Set data type for clean representation
json_df_features['properties.segment_id']=json_df_features['properties.segment_id'].astype(str)

# Save file
# json_df_features.to_excel('D:/.../.../bzm/App/assets/.. .xlsx')

# Read traffic data from file
# DEPLOYED Path assumes assets to be a sub folder of src, let's define where to put the data file
if DEPLOYED:
    traffic_data_file = os.path.join(os.path.dirname(__file__), 'assets', 'traffic_df_2024_Q4_2025_YTD.csv.gz')
else:
    #traffic_data_file = os.path.join(os.path.dirname(__file__), '..', 'assets', 'traffic_df_2024_Q4_2025_YTD.csv.gz')
    traffic_data_file = os.path.join(os.path.dirname(__file__), 'assets/', 'traffic_df_2024_Q4_2025_YTD.csv.gz')

if not DEPLOYED:
    print('Reading traffic data...')
traffic_df = pd.read_csv(traffic_data_file, compression='gzip')

# Set data types for clean representation
traffic_df['segment_id']=traffic_df['segment_id'].astype(str)
traffic_df['year']=traffic_df['year'].astype(int)
traffic_df['year']=traffic_df['year'].astype(str)


### Create street map ###
if not DEPLOYED:
    print('Prepare map...')

# Add column with bike/car ratio for street map representation (skip rows where car_total is 0, set to 500 i.e. most favorable bike/car ratio)
if not DEPLOYED:
    print('Add bike/car ratio column...')
traffic_df['bike_car_ratio'] = ""
#for i in range(len(traffic_df)):
#    if traffic_df['car_total'].values[i] != 0:
#        traffic_df['bike_car_ratio'].values[i] = traffic_df['bike_total'].values[i] / traffic_df['car_total'].values[i]
#    else:
#        traffic_df['bike_car_ratio'].values[i] = 500

# Prepare consolidated bike/car ratios by segment_id
traffic_df_agg_by_id = traffic_df.groupby('segment_id', as_index=False).agg(bike_total=('bike_total', 'sum'), car_total=('car_total', 'sum'), bike_car_ratio=('bike_car_ratio', 'sum'))
#traffic_df_agg_by_id_sorted=traffic_df_agg_by_id.sort_values(by=['bike_car_ratio'])
traffic_df_agg_by_id['bike_car_ratio'] = traffic_df_agg_by_id['bike_total']/traffic_df_agg_by_id['car_total']
traffic_df_agg_by_id_sorted=traffic_df_agg_by_id.sort_values(by=['bike_car_ratio'])

# Add discrete colors for street map representation
traffic_df_agg_by_id_sorted['map_line_color'] = ""
for i in range(len(traffic_df_agg_by_id_sorted)):
    if traffic_df_agg_by_id_sorted['bike_car_ratio'].values[i] < 0.1:
        traffic_df_agg_by_id_sorted['map_line_color'].values[i] = 'Over 100x more cars'
    elif traffic_df_agg_by_id_sorted['bike_car_ratio'].values[i] < 0.2:
        traffic_df_agg_by_id_sorted['map_line_color'].values[i] = 'Up to 10x more cars'
    elif traffic_df_agg_by_id_sorted['bike_car_ratio'].values[i] < 0.5:
        traffic_df_agg_by_id_sorted['map_line_color'].values[i] = 'Up to 5x more cars'
    elif traffic_df_agg_by_id_sorted['bike_car_ratio'].values[i] < 1:
        traffic_df_agg_by_id_sorted['map_line_color'].values[i] = 'Up to 2x more cars'
    else:
        traffic_df_agg_by_id_sorted['map_line_color'].values[i] = 'More bikes than cars'


# Create Map figure
lats = []
lons = []
ids = []
names = []
map_colors = []

# Custom colors
ADFC_orange = '#D78432'
ADFC_green = '#1C9873'
ADFC_blue = '#2C4B78'
ADFC_darkgrey = '#737373'
ADFC_lightblue = '#95CBD8'
ADFC_skyblue = '#D7EDF2'
ADFC_crimson = '#B44958'
ADFC_lightgrey = '#DEDEDE'
ADFC_palegrey = '#F2F2F2'
ADFC_pink = '#EB9AAC'

for street, street_line_color in zip(traffic_df_agg_by_id_sorted['segment_id'], traffic_df_agg_by_id_sorted['map_line_color']):

    for feature, id, name in zip(geo_df.geometry, json_df_features['properties.segment_id'], json_df_features['properties.osm.name']):
        if id == street:
            if isinstance(feature, shapely.geometry.linestring.LineString):
                linestrings = [feature]
            elif isinstance(feature, shapely.geometry.multilinestring.MultiLineString):
                linestrings = feature.geoms
            else:
                continue
            for linestring in linestrings:
                x, y = linestring.xy
                lats = np.append(lats, y)
                lons = np.append(lons, x)
                ids = np.append(ids, [id]*len(y))
                names = np.append(names, [name] * len(y))
                map_colors = np.append(map_colors, [street_line_color]*len(y))
                lats = np.append(lats, None)
                lons = np.append(lons, None)
                ids = np.append(ids, None)
                names = np.append(names, None)
                map_colors = np.append(map_colors, None)
        else: continue

fig = px.line_map(lat=lats, lon=lons, color=map_colors, hover_name=names, line_group=ids, color_discrete_map= {
    'More bikes than cars': ADFC_green,
    'Up to 2x more cars': ADFC_blue,
    'Up to 5x more cars': ADFC_orange,
    'Up to 10x more cars': ADFC_crimson,
    'Over 100x more cars': ADFC_pink},
    labels={'color': 'Bike/Car ratio'},
    map_style="open-street-map", center= dict(lat=52.5, lon=13.45), height=600, zoom=10)

fig.update_traces(line_width=5)
fig.update_layout(margin=dict(l=40, r=20, t=40, b=30))
fig.update_layout(legend=dict(
    yanchor="top",
    y=0.99,
    xanchor="right",
    x=0.99
))
#fig.show()


### Run Dash app ###

if not DEPLOYED:
    print('Start dash...')

# Initiate values
init_street = 'Kastanienallee'

dbc_css = "https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-templates/dbc.min.css"
app = Dash(__name__, requests_pathname_prefix="/cgi-bin/bzm.cgi/" if DEPLOYED else None,
           external_stylesheets=[dbc.themes.BOOTSTRAP, dbc_css],
           meta_tags=[{'name': 'viewport', 'content': 'width=device-width, initial-scale=1'}]
           )

app.layout = dbc.Container(
    [
        dbc.Row([
            dbc.Col([
                html.H1('Berlin zählt Mobilität', style={'margin-left': 40, 'margin-top': 20, 'margin-bottom': 00, 'margin-right': 40}, className='bg-#F2F2F2'),
            ], width=12)
        ]),
        dbc.Row([
            # Street map
            dbc.Col([
                dcc.Graph(id='map', figure=fig, className='bg-#F2F2F2'),
            ], width=8),

            # General controls
            dbc.Col([

                # Street drop down
                html.H4('Select street:', style={'margin-top': 50, 'margin-bottom': 10}),
                dcc.Dropdown(id='street_name_dd',
                options=sorted([{'label': i, 'value': i}
                        for i in traffic_df['osm.name'].unique()], key=lambda x: x['label']), value=init_street),
                html.Hr(),
                #html.H4('Selected street:'), #, style={'margin-top': 10, 'margin-bottom': 10}),
                #html.Div(id='where'),

                # Date picker
                html.H4('Pick date range:', style={'margin-top': 20, 'margin-bottom': 10}),
                dcc.DatePickerRange(
                    id="date_filter",
                    start_date=traffic_df["date_local"].min(),
                    end_date=traffic_df["date_local"].max(),
                    min_date_allowed=traffic_df["date_local"].min(),
                    max_date_allowed=traffic_df["date_local"].max(),
                    display_format='DD-MMM-YYYY',
                    end_date_placeholder_text='DD-MMMM-YYYY',
                    #style = {'font-family': 'Arial', 'font-size': 24, 'font-weight': 700}
                ),
                html.Hr(),
                # Pie chart
                dcc.Graph(id='pie_traffic', figure={}),

            ], width=3),
        ]),

        # Absolute traffic
        dbc.Row([
            dbc.Col([
                # Radio time division
                html.H4('Absolute traffic', style={'margin-left': 40, 'margin-right': 40, 'margin-top': 30, 'margin-bottom': 30}),

                # Select a time division
                dcc.RadioItems(
                    id='radio_time_division',
                    options=[
                        {'label': 'Year', 'value': 'year'},
                        {'label': 'Month', 'value': 'year_month'},
                        {'label': 'Week', 'value': 'year_week'},
                        {'label': 'Date', 'value': 'date'}
                    ],
                    value='year_month',
                    inline=True,
                    inputStyle={"margin-right": "5px", "margin-left": "20px"},
                    style={'margin-left': 40, 'margin-bottom': 00},
                ),
            ], width=6
            ),
        ]),
        dbc.Row([
            dbc.Col([
                dcc.Graph(id='line_abs_traffic', figure={}, style={'margin-left': 40, 'margin-right': 40, 'margin-top': 30, 'margin-bottom': 30}),
            ], width=12
            ),
        ]),

        # Average traffic
        dbc.Row([
            dbc.Col([
                # Radio time division
                html.H4('Average traffic', style={'margin-left': 40, 'margin-right': 40, 'margin-top': 30, 'margin-bottom': 30}),

                dcc.RadioItems(
                    id='radio_time_unit',
                    options=[
                        {'label': 'Yearly', 'value': 'year'},
                        {'label': 'Monthly', 'value': 'year_month'},
                        {'label': 'Weekly', 'value': 'weekday'},
                        {'label': 'Daily', 'value': 'day'},
                        {'label': 'Hourly', 'value': 'hour'}
                    ],
                    value='weekday',
                    inline=True,
                    inputStyle={"margin-right": "5px", "margin-left": "20px"},
                    style={'margin-left': 40, 'margin-bottom': 00},
                ),
            ], width=6
            ),
        ]),
        dbc.Row([
            dbc.Col([
                dcc.Graph(id='bar_avg_traffic', figure={}, style={'margin-left': 40, 'margin-right': 40, 'margin-top': 30, 'margin-bottom': 30})
            ], width=12
            ),
        ]),
        html.H4('Percentage car speed', style={'margin-left': 40, 'margin-right': 40, 'margin-top': 30, 'margin-bottom': 00}),
        dbc.Row([
            dbc.Col([
                dcc.Graph(id='bar_perc_speed', figure={}, style={'margin-left': 40, 'margin-right': 40, 'margin-top': 30, 'margin-bottom': 30})
            ], width=12
            ),
        ]),

        # Experiment with x- and y-axis scatter
        dbc.Row([
            dbc.Col([
                html.H4('Experiment', style={'margin-left': 40, 'margin-right': 40, 'margin-top': 30, 'margin-bottom': 00}),
            ], width=12
            ),
        ]),
        dbc.Row([
            dbc.Col([
                 # Select x-axis
                html.H6('X-Axis:', style={'margin-left': 40, 'margin-right': 40, 'margin-top': 30, 'margin-bottom': 10}),
                dcc.RadioItems(
                    id='radio_x_axis',
                    #options=['ped_total', 'bike_total', 'car_total', 'heavy_total', 'osm.length', 'osm.maxspeed'],
                    options=[
                        {'label': 'Pedestrians', 'value': 'ped_total'},
                        {'label': 'Bikes', 'value': 'bike_total'},
                        {'label': 'Cars', 'value': 'car_total'},
                        {'label': 'Heavy', 'value': 'heavy_total'},
                        {'label': 'Street Length', 'value': 'osm.length'},
                        {'label': 'Max Speed', 'value': 'osm.maxspeed'}
                    ],
                    value='car_total',
                    inline=True,
                    inputStyle={"margin-right": "5px", "margin-left": "20px"},
                    style={'margin-left': 40, 'margin-bottom': 00},
                ),
            ], width=6
            ),
            dbc.Col([
                # Select y-axis
                html.H6('Y-Axis:', style={'margin-left': 40, 'margin-right': 40, 'margin-top': 30, 'margin-bottom': 10}),
                dcc.RadioItems(
                    id='radio_y_axis',
                    options=[
                        {'label': 'Pedestrians', 'value': 'ped_total'},
                        {'label': 'Bikes', 'value': 'bike_total'},
                        {'label': 'Cars', 'value': 'car_total'},
                        {'label': 'Heavy', 'value': 'heavy_total'},
                        {'label': 'Street Length', 'value': 'osm.length'},
                        {'label': 'Max Speed', 'value': 'osm.maxspeed'}
                    ],
                    value='ped_total',
                    inline=True,
                    inputStyle={"margin-right": "5px", "margin-left": "20px"},
                    style={'margin-left': 00, 'margin-bottom': 00, 'margin-right': 40},
                ),
            ], width=6
            ),
        ]),
        dbc.Row([
            dbc.Col([
                dcc.Graph(id='sc_experiment', figure={}, style={'margin-left': 40, 'margin-right': 40, 'margin-top': 30, 'margin-bottom': 30})
            ], width=12
            ),
        ]),

    html.Br(),
    ],
    fluid = True,
    className = 'dbc'
)

### Map callback ###
@callback(
    #Output(component_id='where', component_property='children'),
    Output(component_id='street_name_dd',component_property='value'),
    Input(component_id='map', component_property='clickData'),
    prevent_initial_call=True
)

def get_street_name(clickData):
    global street
    if clickData:
        street = clickData['points'][0]['hovertext']
    return street


### General traffic callback ###
@callback(
    Output(component_id='pie_traffic', component_property='figure'),
    Output(component_id='line_abs_traffic', component_property='figure'),
    Output(component_id='bar_avg_traffic', component_property='figure'),
    Output(component_id='bar_perc_speed', component_property='figure'),
    Input(component_id='radio_time_division', component_property='value'),
    Input(component_id='radio_time_unit', component_property='value'),
    Input(component_id='street_name_dd', component_property='value'),
    Input(component_id="date_filter", component_property="start_date"),
    Input(component_id="date_filter", component_property="end_date")
)

def update_graph(radio_time_division, radio_time_unit, street_name_dd, start_date, end_date):

    # Add column with selected street and "others"
    #save_df(traffic_df,'Debug traffic_df')
    traffic_df_str_id = traffic_df
    traffic_df_str_id['street_selection'] = traffic_df_str_id.loc[:, 'osm.name']
    selection = street_name_dd
    traffic_df_str_id.loc[traffic_df_str_id['street_selection'] != selection, 'street_selection'] = "Other"

    # Filter time period
    traffic_df_str_id_time = traffic_df_str_id.loc[traffic_df_str_id['date_local'].between(start_date, end_date)]

    # Aggregate
    traffic_df_str_id_time_agg = traffic_df_str_id_time.groupby(by=[radio_time_division,'street_selection'], as_index=False).agg({'ped_total': 'sum', 'bike_total': 'sum', 'car_total': 'sum', 'heavy_total': 'sum'})

    # Create abs line chart
    line_abs_traffic = px.line(traffic_df_str_id_time_agg,
        x=radio_time_division, y=['ped_total', 'bike_total', 'car_total', 'heavy_total'],
        markers=True,
        facet_col='street_selection',
        category_orders={'street_selection': [street_name_dd, 'Other'], 'weekday': ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']},
        labels={'year': 'Year', 'year_month': 'Month', 'year_week': 'Week', 'date': 'Date'},
        color_discrete_map={'ped_total': ADFC_lightblue, 'bike_total': ADFC_green, 'car_total': ADFC_orange, 'heavy_total': ADFC_crimson},
        #template='plotly_dark',
        facet_col_spacing=0.04,
        #height=chart_height, width=chart_width,
        title=f'Absolute traffic count by {radio_time_division}')

    line_abs_traffic.update_yaxes(matches=None)
    line_abs_traffic.for_each_annotation(lambda a: a.update(text=a.text.split("=")[1]))
    line_abs_traffic.update_layout(legend_title_text='Traffic Type')
    line_abs_traffic.update_layout({'plot_bgcolor': ADFC_palegrey,'paper_bgcolor': ADFC_palegrey})
    line_abs_traffic.update_layout(yaxis_title= f'Absolute traffic count')
    #line_abs_traffic.update_xaxes(dtick='M1', tickformat= tick_format)
    line_abs_traffic.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))
    for annotation in line_abs_traffic.layout.annotations: annotation['font'] = {'size': 14}

    # Test: line_abs_traffic.update_layout(legend=dict(orientation="h",))

    # Prepare pie chart data
    pie_df = traffic_df_str_id_time[traffic_df_str_id_time['street_selection']==street_name_dd]
    pie_df_traffic = pie_df[['ped_total', 'bike_total', 'car_total', 'heavy_total']]
    pie_df_traffic_ren = pie_df_traffic.rename(columns={'ped_total': 'Pedestrians', 'bike_total': 'Bikes', 'car_total': 'Cars', 'heavy_total': 'Heavy'})
    pie_df_traffic_sum = pie_df_traffic_ren.aggregate(['sum'])
    pie_df_traffic_sum_T = pie_df_traffic_sum.transpose().reset_index()

    # Create pie chart
    pie_traffic = px.pie(pie_df_traffic_sum_T, names='index', values='sum', color='index', height=300,
                         color_discrete_map={'Pedestrians': ADFC_lightblue, 'Bikes': ADFC_green, 'Cars': ADFC_orange, 'Heavy': ADFC_crimson})

    pie_traffic.update_layout(margin=dict(l=00, r=00, t=00, b=00))
    pie_traffic.update_layout(showlegend=False)
    pie_traffic.update_traces(textposition='inside', textinfo='percent+label')
    #pie_traffic.update_traces(texttemplate='%{percent:.0%}')


    # Create avg bar chart
    traffic_df_str_id_time_grpby = traffic_df_str_id_time.groupby(by=[radio_time_unit, 'street_selection'], as_index=False).agg({'ped_total': 'mean', 'bike_total': 'mean', 'car_total': 'mean', 'heavy_total': 'mean'})

    bar_avg_traffic = px.bar(traffic_df_str_id_time_grpby,
        x=radio_time_unit, y=['ped_total', 'bike_total', 'car_total', 'heavy_total'],
        barmode='stack',
        facet_col='street_selection',
        category_orders={'street_selection': [street_name_dd, 'Other'],
                         'weekday': ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'],
                         'month': ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']},
        labels={'year': 'Yearly', 'year_month': 'Monthly', 'weekday': 'Weekly', 'day': 'Daily', 'hour': 'Hourly'},
        color_discrete_map={'ped_total': ADFC_lightblue, 'bike_total': ADFC_green, 'car_total': ADFC_orange, 'heavy_total': ADFC_crimson},
        facet_col_spacing=0.04,
        #height=chart_height, width=chart_width,
        title=f'Average traffic count by {radio_time_unit}')

    bar_avg_traffic.for_each_annotation(lambda a: a.update(text=a.text.split("=")[1]))
    bar_avg_traffic.update_layout(legend_title_text='Traffic Type')
    bar_avg_traffic.update_layout({'plot_bgcolor': ADFC_palegrey,'paper_bgcolor': ADFC_palegrey})
    bar_avg_traffic.update_layout(yaxis_title= f'Average traffic count')
    bar_avg_traffic.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))
    for annotation in bar_avg_traffic.layout.annotations: annotation['font'] = {'size': 14}

    # Create perc speed bar chart
    cols = ['car_speed0', 'car_speed10', 'car_speed20', 'car_speed30', 'car_speed40', 'car_speed50', 'car_speed60',
            'car_speed70']

    # Add column with all car speed %
    traffic_df_str_id['sum_speed_perc'] = traffic_df_str_id[cols].sum(axis=1)
    # Drop empty rows
    nan_rows = traffic_df_str_id[traffic_df_str_id['sum_speed_perc']==0]
    traffic_df_str_id = traffic_df_str_id.drop(nan_rows.index)

    # Filter time period
    traffic_df_str_id_time = traffic_df_str_id.loc[traffic_df_str_id['date_local'].between(start_date, end_date)]

    traffic_df_str_id_time_spd = traffic_df_str_id_time.groupby(by=[radio_time_unit, 'street_selection'], as_index=False).agg({'car_speed0': 'mean', 'car_speed10': 'mean', 'car_speed20': 'mean', 'car_speed30': 'mean', 'car_speed40': 'mean', 'car_speed50': 'mean', 'car_speed60': 'mean', 'car_speed70': 'mean'})
    bar_perc_speed = px.bar(traffic_df_str_id_time_spd,
         x=radio_time_unit, y=cols,
         barmode='stack',
         facet_col='street_selection',
         category_orders={'street_selection': [street_name_dd, 'Other'],
                          'weekday': ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'],
                          'month': ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep',
                                    'Oct', 'Nov', 'Dec']},
         labels={'year': 'Yearly', 'year_month': 'Monthly', 'weekday': 'Weekly', 'day': 'Daily', 'hour': 'Hourly'},
         color_discrete_map={'car_speed0': ADFC_lightblue, 'car_speed10': ADFC_lightblue,'car_speed20': ADFC_lightblue, 'car_speed30': ADFC_green, 'car_speed40': ADFC_green, 'car_speed50': ADFC_orange, 'car_speed60': ADFC_crimson, 'car_speed70': ADFC_pink},
         facet_col_spacing=0.04,
         # height=chart_height, width=chart_width,
         title=f'Percentage speed by {radio_time_unit}')

    bar_perc_speed.for_each_annotation(lambda a: a.update(text=a.text.split("=")[1]))
    bar_perc_speed.update_layout(legend_title_text='Traffic Type')
    bar_perc_speed.update_layout({'plot_bgcolor': ADFC_palegrey, 'paper_bgcolor': ADFC_palegrey})
    bar_perc_speed.update_layout(yaxis_title=f'Percentage car speed')
    bar_perc_speed.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))
    for annotation in bar_perc_speed.layout.annotations: annotation['font'] = {'size': 14}

    return pie_traffic, line_abs_traffic, bar_avg_traffic, bar_perc_speed

### General traffic callback ###
@callback(
    Output(component_id='sc_experiment', component_property='figure'),
    Input(component_id='radio_x_axis', component_property='value'),
    Input(component_id='radio_y_axis', component_property='value'),
    Input(component_id='street_name_dd', component_property='value'),
    Input(component_id="date_filter", component_property="start_date"),
    Input(component_id="date_filter", component_property="end_date"),
)

def update_graph(radio_x_axis, radio_y_axis, street_name_dd, start_date, end_date):

    # Add column with selected street and "others"
    ex_df = traffic_df
    ex_df['street_selection'] = ex_df.loc[:, 'osm.name']
    selection = street_name_dd
    ex_df.loc[ex_df['street_selection'] != selection, 'street_selection'] = "Other"

    # Filter time period
    ex_df_time = ex_df.loc[ex_df['date_local'].between(start_date, end_date)]

    # Aggregate
    #ex_df_time_agg = ex_df_time.groupby(by=['street_selection'], as_index=False).agg({'ped_total': 'sum', 'bike_total': 'sum', 'car_total': 'sum', 'heavy_total': 'sum'})

    # Create abs line chart
    sc_experiment = px.scatter(ex_df_time,
        x=radio_x_axis, y=radio_y_axis,
        facet_col='street_selection',
        category_orders={'street_selection': [street_name_dd, 'Other']},
        facet_col_spacing=0.04,
        color=radio_y_axis,
        color_continuous_scale='temps',
        labels={'ped_total': 'Pedestrians', 'bike_total': 'Bikes', 'car_total': 'Cars', 'heavy_total': 'Heavy', 'osm.length': 'Street Length', 'osm.maxspeed': 'Max Speed'},
        title=f'Absolute traffic by {radio_x_axis}')

    sc_experiment.update_yaxes(matches=None)
    sc_experiment.for_each_annotation(lambda a: a.update(text=a.text.split("=")[1]))
    sc_experiment.update_layout(legend_title_text='Traffic Type')
    sc_experiment.update_layout({'plot_bgcolor': ADFC_palegrey,'paper_bgcolor': ADFC_palegrey})
    sc_experiment.update_layout(yaxis_title= f'{radio_y_axis}')
    sc_experiment.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))
    for annotation in sc_experiment.layout.annotations: annotation['font'] = {'size': 14}

    return sc_experiment

if __name__ == "__main__":
    app.run_server(debug=False)
