#!/usr/bin/env python3
# Copyright (c) 2024-2025 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    bzm_v01.py
# @author  Egbert Klaassen
# @date    2025-01-27

# traffic_df        - dataframe, traffic data file
# geo_df            - geo dataframe, street coordinates for px.map
# json_df           - json dataframe (using the same geojson as the geo_df), to access features such as street names

import pandas as pd
import geopandas as gpd
import shapely.geometry
import numpy as np
import plotly.express as px
from dash import Dash, html, dcc, callback, Output, Input
import dash_bootstrap_components as dbc

import bzm_get_data
import common
from src.bzm_get_data import save_df

DEPLOYED = __name__ != '__main__'


#### Retrieve Data ####

# Read geojson data file to access geometry coordinates - using URL
geojson_url = 'https://berlin-zaehlt.de/csv/bzm_telraam_segments.geojson'
if not DEPLOYED:
    print('Reading geojson data...')
geo_df = gpd.read_file(geojson_url)

if not DEPLOYED:
    print('Reading json data...')
json_df_features = bzm_get_data.get_locations(geojson_url)

# Read traffic data from file
if not DEPLOYED:
    print('Reading traffic data...')

with common.Benchmarker(not DEPLOYED, "Load traffic data"):
    traffic_df = bzm_get_data.merge_data(json_df_features)

"""" Can move to bzm_get_data? - Start """

# Set data types for clean representation
json_df_features['segment_id']=json_df_features['segment_id'].astype(str)
traffic_df['segment_id']=traffic_df['segment_id'].astype(str)
traffic_df['year']=traffic_df['year'].astype(str)

# Drop uptime when empty
# nan_rows = traffic_df[traffic_df['uptime'].isnull()]
# traffic_df = traffic_df.drop(nan_rows.index)

# Replace nan values
# traffic_df['car_total'] = traffic_df['car_total'].fillna(0)
# traffic_df = traffic_df.fillna(0)

# Add street column for facet graphs
traffic_df['street_selection'] = traffic_df.loc[:, 'osm.name']
traffic_df.loc[traffic_df['street_selection'] != 'does not exist', 'street_selection'] = 'All'

"""" Can move to bzm_get_data? - End """

def update_selected_street(df, segment_id, street_name, start_date, end_date, hour_range):
    # Generate "selected street only" df and populate street_selection
    traffic_df_str = df[df['segment_id'] == segment_id]
    traffic_df_str.loc[traffic_df_str['street_selection'] == 'All', 'street_selection'] = street_name

    # Filter min max street dates based on "selected street only"
    min_str_date = traffic_df_str['date_local'].min()
    max_str_date = traffic_df_str['date_local'].max()

    # Filter min max street hours based on "selected street only"
    if start_date < min_str_date:
        start_date = min_str_date
    if end_date > max_str_date:
        end_date = max_str_date

    min_str_hour = traffic_df_str["hour"].min()
    max_str_hour = traffic_df_str["hour"].max()

    # Filter min max street hours based on "selected street only"
    if hour_range[0] < min_str_hour:
        hour_range[0] = min_str_hour
    if hour_range[1] > max_str_hour:
        hour_range[1] = max_str_hour

    # Add selected street to all streets
    traffic_df_all_str = df._append(traffic_df_str, ignore_index=True)

    # Filter min max period
    traffic_df_sel_str_dates = traffic_df_all_str.loc[traffic_df_all_str['date_local'].between(start_date, end_date)]
    traffic_df_sel_str_hours = traffic_df_sel_str_dates.loc[traffic_df_sel_str_dates['hour'].between(hour_range[0], hour_range[1])]
    out_traffic_df_sel_str = traffic_df_sel_str_hours.sort_values(by=['street_selection', 'date_local'])

    return out_traffic_df_sel_str, start_date, end_date, hour_range

# Initialize constants and variables
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

min_date = traffic_df["date_local"].min()
max_date = traffic_df["date_local"].max()
start_date = min_date
end_date = max_date

data_min_hour = traffic_df["hour"].min()
data_max_hour = traffic_df["hour"].max()

hour_range = [data_min_hour, data_max_hour]

street_name = 'Kastanienallee' #'Köpenicker Straße'
init_segment_id = '9000004995'

# Initiate basic traffic_df with first street selected
traffic_df_sel_str, start_date, end_date, hour_range = update_selected_street(traffic_df, init_segment_id, street_name, start_date, end_date, hour_range)

### Create street map ###
if not DEPLOYED:
    print('Prepare map...')

# Add column with bike/car ratio for street map representation (skip rows where car_total is 0, set to 500 i.e. most favorable bike/car ratio)
if not DEPLOYED:
    print('Add bike/car ratio column...')

# Prepare consolidated bike/car ratios by segment_id
traffic_df_id_bc = traffic_df.groupby(by=['segment_id'], as_index=False).agg(bike_total=('bike_total', 'sum'), car_total=('car_total', 'sum'))
traffic_df_id_bc['bike_car_ratio'] = traffic_df_id_bc['bike_total']/traffic_df_id_bc['car_total']

bins = [0, 0.1, 0.2, 0.5, 1, 500]
labels = ['Over 10x more cars', 'Over 5x more cars', 'Over 2x more cars','More cars than bikes','More bikes than cars']
traffic_df_id_bc['map_line_color'] = pd.cut(traffic_df_id_bc['bike_car_ratio'], bins=bins, labels=labels)

traffic_df_id_bc_out = traffic_df_id_bc

# Create Map figure
lats = []
lons = []
ids = []
names = []
map_colors = []

# Prepare street geo-data and names
for street, street_line_color in zip(traffic_df_id_bc['segment_id'], traffic_df_id_bc['map_line_color']):

    for feature, id, name in zip(geo_df.geometry, json_df_features['segment_id'], json_df_features['osm.name']):
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
        else:
            continue


### Run Dash app ###

if not DEPLOYED:
    print('Start dash...')

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
                dcc.Graph(id='street_map', figure={}, className='bg-#F2F2F2'),
            ], width=8),

            # General controls
            dbc.Col([

                # Street drop down
                html.H4('Select street:', style={'margin-top': 50, 'margin-bottom': 10}),
                dcc.Dropdown(id='street_name_dd',
                options=sorted([{'label': i, 'value': i}
                        for i in traffic_df['osm.name'].unique()], key=lambda x: x['label']), value=street_name),
                html.Hr(),
                html.H4('Traffic type (selected street)', style={'margin-top': 20, 'margin-bottom': 30}),
                # html.Div(id='where'),
                # Pie chart
                dcc.Graph(id='pie_traffic', figure={}),
                html.Hr(),

            ], width=3),
        ]),

        # Date/Time selection
        dbc.Row([
            dbc.Col([
                html.H6('Set hour range:', style={'margin-left': 40, 'margin-right': 40, 'margin-top': 00, 'margin-bottom': 30}),
                # Hour slice
                dcc.RangeSlider(
                    id='range_slider',
                    min=data_min_hour,
                    max=data_max_hour,
                    step=1,
                    value=hour_range,
                    tooltip={'always_visible': True, 'template': "{value} hour"}),
            ], width=6),
            dbc.Col([
                html.H6('Pick date range:', style={'margin-left': 00, 'margin-right': 40, 'margin-top': 00, 'margin-bottom': 30}),
                # Date picker
                dcc.DatePickerRange(
                    id="date_filter",
                    start_date=start_date,
                    end_date=end_date,
                    min_date_allowed=min_date, # traffic_df["date_local"].min(),
                    max_date_allowed=max_date, #traffic_df["date_local"].max(),
                    display_format='DD-MMM-YYYY',
                    end_date_placeholder_text='DD-MMMM-YYYY',
                    minimum_nights=1
                ),
            ], width=6),
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
        dbc.Row([
            dbc.Col([
                html.H4('Percentage car speed - by time unit',style={'margin-left': 40, 'margin-right': 40, 'margin-top': 30, 'margin-bottom': 00}),
                dcc.Graph(id='bar_perc_speed', figure={}, style={'margin-left': 40, 'margin-right': 40, 'margin-top': 30, 'margin-bottom': 30})
            ], width=12
            ),
        ]),
        dbc.Row([
            dbc.Col([
                html.H4('Percentage car speed - average',style={'margin-left': 40, 'margin-right': 40, 'margin-top': 30, 'margin-bottom': 00}),
                dcc.Graph(id='bar_avg_speed', figure={},
                          style={'margin-left': 40, 'margin-right': 40, 'margin-top': 30, 'margin-bottom': 30})
            ], width=12
            ),
        ]),
        # Explore with x- and y-axis scatter
        dbc.Row([
            dbc.Col([
                html.H4('Explore', style={'margin-left': 40, 'margin-right': 40, 'margin-top': 30, 'margin-bottom': 00}),
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
                dcc.Graph(id='sc_explore', figure={}, style={'margin-left': 40, 'margin-right': 40, 'margin-top': 30, 'margin-bottom': 30})
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
    Output(component_id='street_name_dd',component_property='value'),
#     Output(component_id='street_map', component_property='figure'),
    Input(component_id='street_map', component_property='clickData'),
    prevent_initial_call=True
)

def get_street_name(clickData):

    if clickData:
        street_name = clickData['points'][0]['hovertext']

    return street_name

@callback(
    Output(component_id='street_map', component_property='figure'),
    Input(component_id='street_name_dd', component_property='value'),
)

def update_graph(street_name): #, start_date, end_date, hour_range):

    street_map = px.line_map(lat=lats, lon=lons, color=map_colors, hover_name=names, line_group=ids, color_discrete_map= {
        'More bikes than cars': ADFC_green,
        'More cars than bikes': ADFC_blue,
        'Over 2x more cars': ADFC_orange,
        'Over 5x more cars': ADFC_crimson,
        'Over 10x more cars': ADFC_pink},
        labels={'color': 'Bike/Car ratio'},
        map_style="streets", center= dict(lat=52.5, lon=13.45), height=600, zoom=11)

    street_map.update_traces(line_width=5)
    street_map.update_layout(margin=dict(l=40, r=20, t=40, b=30))
    street_map.update_layout(legend=dict(yanchor="top", y=0.99, xanchor="right", x=0.99))

    return street_map

### General traffic callback ###
@callback(
    Output(component_id='pie_traffic', component_property='figure'),
    Output(component_id='line_abs_traffic', component_property='figure'),
    Output(component_id='bar_avg_traffic', component_property='figure'),
    Output(component_id='bar_perc_speed', component_property='figure'),
    Output(component_id='bar_avg_speed', component_property='figure'),
    Output(component_id="date_filter", component_property="start_date"),
    Output(component_id="date_filter", component_property="end_date"),
    Output(component_id="range_slider", component_property="value"),
    Input(component_id='radio_time_division', component_property='value'),
    Input(component_id='radio_time_unit', component_property='value'),
    Input(component_id='street_name_dd', component_property='value'),
    Input(component_id="date_filter", component_property="start_date"),
    Input(component_id="date_filter", component_property="end_date"),
    Input(component_id='range_slider', component_property='value'),
)

def update_graph(radio_time_division, radio_time_unit, street_name, start_date, end_date, hour_range):
    # Get segment_id
    segment_id_index = traffic_df.loc[traffic_df['osm.name'] == street_name]
    segment_id = segment_id_index['segment_id'].values[0]

    # Update street selection
    traffic_df_sel_str, start_date, end_date, hour_range = update_selected_street(traffic_df, segment_id, street_name, start_date, end_date, hour_range)

    # Aggregate
    traffic_df_sel_str_agg = traffic_df_sel_str.groupby(by=['street_selection', radio_time_division],as_index=False).agg({'ped_total': 'sum', 'bike_total': 'sum', 'car_total': 'sum', 'heavy_total': 'sum'})
    save_df(traffic_df_sel_str_agg, 'traffic_df_sel_str_agg.xlsx')

    # Create abs line chart
    line_abs_traffic = px.line(traffic_df_sel_str_agg,
        x=radio_time_division, y=['ped_total', 'bike_total', 'car_total', 'heavy_total'],
        markers=True,
        facet_col='street_selection',
        category_orders={'street_selection': [street_name, 'All'], 'weekday': ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']},
        labels={'year': 'Year', 'year_month': 'Month', 'year_week': 'Week', 'date': 'Date'},
        color_discrete_map={'ped_total': ADFC_lightblue, 'bike_total': ADFC_green, 'car_total': ADFC_orange, 'heavy_total': ADFC_crimson},
        facet_col_spacing=0.04,
        title=f'Absolute traffic count by {radio_time_division}')

    line_abs_traffic.update_yaxes(matches=None)
    line_abs_traffic.for_each_annotation(lambda a: a.update(text=a.text.split("=")[1]))
    line_abs_traffic.update_layout(legend_title_text='Traffic Type')
    line_abs_traffic.update_layout({'plot_bgcolor': ADFC_palegrey,'paper_bgcolor': ADFC_palegrey})
    line_abs_traffic.update_layout(yaxis_title= 'Absolute traffic count')
    line_abs_traffic.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))
    for annotation in line_abs_traffic.layout.annotations:
        annotation['font'] = {'size': 14}
    line_abs_traffic.for_each_annotation(lambda a: a.update(text=a.text.replace(street_name, street_name + ' (segment no:' + segment_id + ')')))


    # Prepare pie chart data
    pie_df = traffic_df_sel_str[traffic_df_sel_str['street_selection'] == street_name]
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


    # Average traffic bar chart
    #traffic_df_str_id_time_grpby = traffic_df_all_time_sorted.groupby(by=[radio_time_unit, 'street_selection'], as_index=False).agg({'ped_total': 'mean', 'bike_total': 'mean', 'car_total': 'mean', 'heavy_total': 'mean'})
    traffic_df_sel_str_groupby = traffic_df_sel_str.groupby(by=[radio_time_unit, 'street_selection'], as_index=False).agg({'ped_total': 'mean', 'bike_total': 'mean', 'car_total': 'mean', 'heavy_total': 'mean'})

    bar_avg_traffic = px.bar(traffic_df_sel_str_groupby,
        x=radio_time_unit, y=['ped_total', 'bike_total', 'car_total', 'heavy_total'],
        barmode='stack',
        facet_col='street_selection',
        category_orders={'street_selection': [street_name, 'All'],
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
    bar_avg_traffic.update_layout(yaxis_title='Average traffic count')
    bar_avg_traffic.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))
    bar_avg_traffic.for_each_annotation(lambda a: a.update(text=a.text.replace(street_name, street_name + ' (segment no:' + segment_id + ')')))
    for annotation in bar_avg_traffic.layout.annotations:
        annotation['font'] = {'size': 14}

    # Percentage speed bar chart
    cols = ['car_speed0', 'car_speed10', 'car_speed20', 'car_speed30', 'car_speed40', 'car_speed50', 'car_speed60',
            'car_speed70']

    # Add column with all car speed %
    traffic_df_sel_str['sum_speed_perc'] = traffic_df_sel_str[cols].sum(axis=1)
    # Drop empty rows
    nan_rows = traffic_df_sel_str[traffic_df_sel_str['sum_speed_perc']==0]
    traffic_df_sel_str_dropped = traffic_df_sel_str.drop(nan_rows.index)

    traffic_df_sel_str_speed = traffic_df_sel_str_dropped.groupby(by=[radio_time_unit, 'street_selection'], as_index=False).agg({'car_speed0': 'mean', 'car_speed10': 'mean', 'car_speed20': 'mean', 'car_speed30': 'mean', 'car_speed40': 'mean', 'car_speed50': 'mean', 'car_speed60': 'mean', 'car_speed70': 'mean'})
    bar_perc_speed = px.bar(traffic_df_sel_str_speed,
         x=radio_time_unit, y=cols,
         barmode='stack',
         facet_col='street_selection',
         category_orders={'street_selection': [street_name, 'All'],
                          'weekday': ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'],
                          'month': ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep',
                                    'Oct', 'Nov', 'Dec']},
         labels={'year': 'Yearly', 'year_month': 'Monthly', 'weekday': 'Weekly', 'day': 'Daily', 'hour': 'Hourly'},
         color_discrete_map={'car_speed0': ADFC_lightblue, 'car_speed10': ADFC_lightblue,'car_speed20': ADFC_lightblue, 'car_speed30': ADFC_green, 'car_speed40': ADFC_green, 'car_speed50': ADFC_orange, 'car_speed60': ADFC_crimson, 'car_speed70': ADFC_pink},
         facet_col_spacing=0.04,
         title=f'Percentage speed by {radio_time_unit}')

    bar_perc_speed.for_each_annotation(lambda a: a.update(text=a.text.split("=")[1]))
    bar_perc_speed.for_each_annotation(lambda a: a.update(text=a.text.replace(street_name, street_name + ' (segment no:' + segment_id + ')')))
    bar_perc_speed.update_layout(legend_title_text='Traffic Type')
    bar_perc_speed.update_layout({'plot_bgcolor': ADFC_palegrey, 'paper_bgcolor': ADFC_palegrey})
    bar_perc_speed.update_layout(yaxis_title='Percentage car speed')
    bar_perc_speed.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))
    for annotation in bar_perc_speed.layout.annotations:
        annotation['font'] = {'size': 14}

    # Create percentage speed average bar chart
    traffic_df_sel_str_avg_speed = traffic_df_sel_str_dropped.groupby(by='street_selection', as_index=False).agg({'car_speed0': 'mean', 'car_speed10': 'mean', 'car_speed20': 'mean', 'car_speed30': 'mean', 'car_speed40': 'mean', 'car_speed50': 'mean', 'car_speed60': 'mean', 'car_speed70': 'mean'})

    bar_avg_speed = px.bar(traffic_df_sel_str_avg_speed,
        x='street_selection', y=cols,
        #barmode='group',
        category_orders={'street_selection': [street_name, 'All'],
                         'weekday': ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'],
                         'month': ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep',
                                   'Oct', 'Nov', 'Dec']},
        labels={'year': 'Yearly', 'year_month': 'Monthly', 'weekday': 'Weekly', 'day': 'Daily',
                'hour': 'Hourly', 'street_selection': 'Street selection'},
        color_discrete_map={'car_speed0': ADFC_lightblue, 'car_speed10': ADFC_lightblue,
                            'car_speed20': ADFC_lightblue, 'car_speed30': ADFC_green,
                            'car_speed40': ADFC_green, 'car_speed50': ADFC_orange,
                            'car_speed60': ADFC_crimson, 'car_speed70': ADFC_pink},
        title='Percentage speed')

    bar_avg_speed.for_each_annotation(lambda a: a.update(text=a.text.split("=")[1]))
    bar_avg_speed.update_layout(xaxis_title = street_name + ' (segment no:' + segment_id + ')')
    bar_avg_speed.update_layout(legend_title_text='Car speed')
    bar_avg_speed.update_layout({'plot_bgcolor': ADFC_palegrey, 'paper_bgcolor': ADFC_palegrey})
    bar_avg_speed.update_layout(yaxis_title='Average percentage car speed')
    bar_avg_speed.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))
    bar_avg_speed.update_layout(barmode='group', bargap=0.5, bargroupgap=0.2)
    for annotation in bar_avg_speed.layout.annotations:
        annotation['font'] = {'size': 14}

    return pie_traffic, line_abs_traffic, bar_avg_traffic, bar_perc_speed, bar_avg_speed, start_date, end_date, hour_range

### Explore traffic callback ###
@callback(
    Output(component_id='sc_explore', component_property='figure'),
    Input(component_id='radio_x_axis', component_property='value'),
    Input(component_id='radio_y_axis', component_property='value'),
    Input(component_id='street_name_dd', component_property='value'),
    Input(component_id="date_filter", component_property="start_date"),
    Input(component_id="date_filter", component_property="end_date"),
    Input(component_id='range_slider', component_property='value'),
)

def update_explore_graph(radio_x_axis, radio_y_axis, street_name, start_date, end_date, hour_range):

    # Get segment_id
    segment_id_index = traffic_df.loc[traffic_df['osm.name'] == street_name]
    segment_id = segment_id_index['segment_id'].values[0]

    #traffic_df_all = update_selected_street(traffic_df, segment_id, street_name)
    traffic_df_sel_str, start_date, end_date, hour_range = update_selected_street(traffic_df, segment_id, street_name, start_date, end_date, hour_range)

    # Create scatter  chart
    sc_explore = px.scatter(traffic_df_sel_str,
        x=radio_x_axis, y=radio_y_axis,
        facet_col='street_selection',
        category_orders={'street_selection': [street_name, 'All']},
        facet_col_spacing=0.04,
        color=radio_y_axis,
        color_continuous_scale='temps',
        labels={'ped_total': 'Pedestrians', 'bike_total': 'Bikes', 'car_total': 'Cars', 'heavy_total': 'Heavy', 'osm.length': 'Street Length', 'osm.maxspeed': 'Max Speed'},
        title=f'Absolute traffic by {radio_x_axis}')

    sc_explore.update_yaxes(matches=None)
    sc_explore.for_each_annotation(lambda a: a.update(text=a.text.split("=")[1]))
    sc_explore.for_each_annotation(lambda a: a.update(text=a.text.replace(street_name, street_name + ' (segment no:' + segment_id + ')')))
    sc_explore.update_layout(legend_title_text='Traffic Type')
    sc_explore.update_layout({'plot_bgcolor': ADFC_palegrey,'paper_bgcolor': ADFC_palegrey})
    sc_explore.update_layout(yaxis_title= f'{radio_y_axis}')
    sc_explore.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))
    for annotation in sc_explore.layout.annotations:
        annotation['font'] = {'size': 14}

    return sc_explore

if __name__ == "__main__":
    app.run_server(debug=False)
