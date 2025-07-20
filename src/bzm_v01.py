#!/usr/bin/env python3
# Copyright (c) 2024-2025 Berlin zählt Mobilität
# SPDX-License-Identifier: MIT

# @file    bzm_performance.py
# @author  Egbert Klaassen
# @date    2025-06-04

""""
# traffic_df        - dataframe with measured traffic data file
# geo_df            - geopandas dataframe, street coordinates for px.line_map
# json_df           - json dataframe based on the same geojson as geo_df, providing features such as street names
"""

import os
import gettext
import datetime
import pandas as pd
import requests
import geopandas as gpd
import dash_bootstrap_components as dbc
from dash import Dash, html, dcc, Output, Input, callback, ctx, no_update
from dash.exceptions import PreventUpdate
import plotly.express as px

DEPLOYED = __name__ != '__main__'

def output_excel(df, file_name):
    path = os.path.join(ASSET_DIR, file_name + '.xlsx')
    df.to_excel(path, index=False)

def output_csv(df, file_name):
    path = os.path.join(ASSET_DIR, file_name + '.csv')
    df.to_csv(path, index=False)

def get_file_size(url):
    try:
        response = requests.head(url, allow_redirects=True)
        if 'Content-Length' in response.headers:
            size_in_bytes = int(response.headers['Content-Length'])
            return size_in_bytes
        else:
            return "File size not available in headers."
    except requests.RequestException as e:
        return f"An error occurred: {e}"

def retrieve_data():
    # Read geojson data file to access geometry coordinates
    if not DEPLOYED:
        print('Reading geojson data...')

    geojson_url = 'https://berlin-zaehlt.de/csv/bzm_telraam_segments.geojson'
    #TODO: manage if offline, geojson_path = os.path.join(ASSET_DIR, 'bzm_telraam_segments.geojson')
    geojson_file_size = get_file_size(geojson_url)
    geo_cols = ['segment_id', 'osm', 'cameras', 'geometry']
    if geojson_file_size > 500:
        geo_df = gpd.read_file(geojson_url, columns=geo_cols)
    else:
        print('Suspected error, geojson_file_size: ' + str(geojson_file_size))
        geojson_path = os.path.join(ASSET_DIR, 'bzm_telraam_segments.geojson')
        geo_df = gpd.read_file(geojson_path, columns=geo_cols)

    if not DEPLOYED:
        print('Reading json data...')
    geo_file_path = os.path.join(ASSET_DIR, 'df_geojson.csv.gz')
    json_df_features = pd.read_csv(geo_file_path)

    # Read traffic data from file
    if not DEPLOYED:
        print('Reading traffic data...')
    traffic_file_path = os.path.join(ASSET_DIR, 'traffic_df_2023_2024_2025_YTD.csv.gz')
    traffic_df = pd.read_csv(traffic_file_path)

    # Set data types for clean representation
    traffic_df['segment_id']=traffic_df['segment_id'].astype(str)

    return geo_df, json_df_features, traffic_df

def update_language(lang_code):
    global language
    language=lang_code

    # Initiate translation
    appname = 'bzm'
    localedir = os.path.join(os.path.dirname(__file__), 'locales')
    # Set up Gettext
    translations = gettext.translation(appname, localedir, fallback=True, languages=[language])
    # Install translation function
    translations.install()

    return

def filter_uptime(df):
    # drop uptime rows < 0.7
    nan_rows = df[df['uptime'] < 0.7]
    traffic_df_upt = df.drop(nan_rows.index)
    return traffic_df_upt

def convert(date_time, format_string):
    datetime_obj = datetime.datetime.strptime(date_time, format_string)
    return datetime_obj

def format_str_date(str_date, from_date_format, to_date_format):
    timestamp_date = datetime.datetime.strptime(str_date, from_date_format)
    formatted_str_date = timestamp_date.strftime(to_date_format)
    return formatted_str_date

def filter_dt(df, start_date, end_date, hour_range):

    # Get min/max dates to set DatePicker range
    min_date = df['date_local'].min()
    max_date = df['date_local'].max()

    # Add one day as filter is in between
    max_date_dt = convert(str(max_date), format_string)
    max_date_dt = max_date_dt + datetime.timedelta(days=-1)
    # Re-format for DatePicker
    min_date = datetime.datetime.strptime(min_date, format_string).strftime('%Y-%m-%d')
    max_date = max_date_dt.strftime('%Y-%m-%d')

    # Remove today as it has only has hours up to actualization (currently 4:00)
    today = pd.to_datetime(datetime.datetime.now().date())
    today = today.strftime('%d/%m/%Y')
    nan_rows = df[df['date'] == today]
    df = df.drop(nan_rows.index)

    # Add one day to end time as filter filters until
    filter_end_date = convert(str(end_date), '%Y-%m-%d')
    filter_end_date = filter_end_date + datetime.timedelta(days=1)
    filter_end_date = filter_end_date.strftime('%Y-%m-%d')

    # Filter selected dates
    df_dates = df[df.date_local.between(start_date, filter_end_date)]

    # Get min/max street hours, add 1 to max for slider representation
    min_hour = df_dates["hour"].min()
    max_hour = df_dates["hour"].max()
    if max_hour < 24:
        max_hour = max_hour + 1

    # Set selected hours, leave minimum gap of 1, manage extremes
    if hour_range[0] == 24:
        hour_range[0] = hour_range[0] - 1
    if hour_range[1] == hour_range[0]:
        hour_range[1] = hour_range[1]+1

    df_dates_hours = df_dates.loc[df_dates['hour'].between(hour_range[0], hour_range[1]-1)]
    traffic_df_upt_dt = df_dates_hours

    # Free memory
    del df, df_dates, df_dates_hours

    return traffic_df_upt_dt, min_date, max_date, min_hour, max_hour

def get_comparison_data(df, radio_time_division, group_by, selected_value_A, selected_value_B):
    df_period_A = df[df[radio_time_division]==selected_value_A]
    df_period_grp_A = df_period_A.groupby(by=[group_by, 'street_selection'], sort=False, as_index=False).agg({'ped_total': 'sum', 'bike_total': 'sum', 'car_total': 'sum', 'heavy_total': 'sum'})
    if radio_time_division == 'year_month' or radio_time_division == 'date':
        df_period_grp_A = df_period_grp_A.sort_values(by=['street_selection', group_by], ascending=True)
    df_avg_traffic_delta_A = df_period_grp_A

    df_period_B = df[df[radio_time_division]==selected_value_B]
    df_period_grp_B = df_period_B.groupby(by=[group_by, 'street_selection'], sort=False, as_index=False).agg({'ped_total': 'sum', 'bike_total': 'sum', 'car_total': 'sum', 'heavy_total': 'sum'})
    if radio_time_division == 'date':
        df_period_grp_B = df_period_grp_B.sort_values(by=['street_selection', group_by], ascending=True)
    # Rename period B columns to new series
    df_period_grp_B_ren = df_period_grp_B.rename(columns={'ped_total': 'ped_total_d', 'bike_total': 'bike_total_d', 'car_total': 'car_total_d', 'heavy_total': 'heavy_total_d'})
    df_avg_traffic_delta_B = df_period_grp_B_ren

    df_avg_traffic_delta_concat = pd.concat([df_avg_traffic_delta_A, df_avg_traffic_delta_B])

    # Free memory
    del df, df_period_A, df_period_B, df_period_grp_A, df_period_grp_B, df_avg_traffic_delta_A, df_avg_traffic_delta_B

    return df_avg_traffic_delta_concat

def update_selected_street(df, segment_id, street_name):

    if segment_id == _('full street'):
        df_str = df[df['osm.name'] == street_name]
        df_str.loc[df_str['street_selection'] == 'All Streets', 'street_selection'] = street_name
    else:
        # Generate "selected street only" df and populate "street_selection"
        df_str = df[df['segment_id'] == segment_id]
        df_str.loc[df_str['street_selection'] == 'All Streets', 'street_selection'] = street_name

    # Add selected street to all streets
    traffic_df_upt_dt_str = df._append(df_str, ignore_index=True)

    # Free memory
    del df, df_str

    return traffic_df_upt_dt_str

def get_bike_car_ratios(df):
    traffic_df_id_bc = df.groupby(by=['segment_id'], as_index=False).agg(bike_total=('bike_total', 'sum'), car_total=('car_total', 'sum'))
    traffic_df_id_bc['bike_car_ratio'] = traffic_df_id_bc['bike_total'] / traffic_df_id_bc['car_total']

    bins = [0, 0.1, 0.2, 0.5, 1, 500]
    speed_labels = ['Over 10x more cars', 'Over 5x more cars', 'Over 2x more cars', 'More cars than bikes', 'More bikes than cars']
    traffic_df_id_bc['map_line_color'] = pd.cut(traffic_df_id_bc['bike_car_ratio'], bins=bins, labels=speed_labels)
    # Prepare traffic_df_id_bc for join operation
    traffic_df_id_bc['segment_id'] = traffic_df_id_bc['segment_id'].astype(int)
    traffic_df_id_bc.set_index('segment_id', inplace=True)

    # Free memory
    del df

    return traffic_df_id_bc

def update_map_data(df_map_base, df):
    # Create map info by joining geo_df_map_info with map_line_color from traffic_df_id_bc (based on bike/car ratios)
    df_map = df_map_base.join(df)

    nan_rows = df_map[df_map['segment_id'].isnull()]
    df_map = df_map.drop(nan_rows.index)

    # Add map_line_color category and add column information to cover inactive traffic counters
    df_map['map_line_color'] = df_map['map_line_color'].cat.add_categories([('Inactive - no data')])
    df_map.fillna({'map_line_color': ('Inactive - no data')}, inplace=True)
    # Sort data to get desired legend order
    df_map = df_map.sort_values(by=['map_line_color'])

    # Move segment_id index to column (avoid ambiguity by two segment_id columns in line_map Plotly v6.0)
    df_map = df_map.drop('segment_id', axis=1)
    df_map.reset_index(level=0, inplace=True)
    df_map['segment_id']=df_map['segment_id'].astype(str)

    # Free memory
    del df, df_map_base

    return df_map

def get_min_max_str(df, id_street, start_date, end_date):
    format_string = '%Y-%m-%d %H:%M:%S'
    missing_data = False
    message = 'none'
    segment_id = id_street[-11:-1]

    # Get min/max dates for the current street
    df_str = df[df['segment_id'] == segment_id]
    min_date_str = df_str['date_local'].min()
    max_date_str = df_str['date_local'].max()

    # Add one day from end_time as it was added before as well
    max_date_str_dt = datetime.datetime.strptime(max_date_str, format_string)
    max_date_str_dt = max_date_str_dt + datetime.timedelta(days=1)

    min_date_str = datetime.datetime.strptime(min_date_str, format_string).strftime('%Y-%m-%d')
    max_date_str = max_date_str_dt.strftime('%Y-%m-%d')

    if start_date > max_date_str or end_date < min_date_str:
        missing_data = True
        message = _('Dates out of range')
        start_date = min_date_str
        end_date = max_date_str
    elif min_date_str <= start_date <= max_date_str and end_date > max_date_str:
        missing_data = True
        message = _('End date out of range')
        end_date = max_date_str
    elif min_date_str <= end_date <= max_date_str and start_date < min_date_str:
        missing_data = True
        message = _('Start date out of range')
        start_date = min_date_str
    elif start_date < min_date_str or end_date > max_date_str:
        missing_data = True
        message = _('Narrowed down range')
        start_date = min_date_str
        end_date = max_date_str

    # Free memory
    del df

    return min_date_str, max_date_str, start_date, end_date, message, missing_data

ASSET_DIR = os.path.join(os.path.dirname(__file__), 'assets')

# Initialize constants, variables and get data
ADFC_green = '#1C9873'
ADFC_palegrey = '#F2F2F2'
ADFC_lightgrey = '#DEDEDE'
ADFC_darkgrey = '#737373'
ADFC_cyan = '#61CBF4'
ADFC_lightblue = '#95CBD8'
ADFC_skyblue = '#D7EDF2'
ADFC_blue = '#2C4B78'
ADFC_darkblue = '#331F45'
ADFC_orange = '#D78432'
ADFC_crimson = '#B44958'
ADFC_pink = '#EB9AAC'
ADFC_yellow = '#EEDE72'

street_name = 'Dresdener Straße'
segment_id = '9000006667'
init_id_street = 'Dresdener Straße (9000006667)'

info_icon = html.I(className='bi bi-info-circle-fill me-2')
email_icon = html.I(className='bi bi-envelope-at-fill me-2')
camera_icon = html.I(className='bi bi-camera-fill me-2')
zoom_factor = 11

geo_df, json_df_features, traffic_df = retrieve_data()

# Set initial language
init_language = 'de'
update_language(init_language)

# Format datetime columns to formatted strings
traffic_df = traffic_df.astype({'year': str}, errors='ignore')

# Start with traffic df with uptime filtered
traffic_df_upt = filter_uptime(traffic_df)

# Get start date, end date and hour range (str)
start_date = traffic_df_upt['date_local'].min()
end_date = traffic_df_upt['date_local'].max()

format_string = '%Y-%m-%d %H:%M:%S'
# Convert to dt do enable time.delta
start_date_dt = convert(str(start_date), format_string)
end_date_dt = convert(str(end_date), format_string)

# Subtract one day as to not to include today hours until 4:00"
end_date_dt = end_date_dt + datetime.timedelta(days=-1)
try_start_date = end_date_dt + datetime.timedelta(days=-14)
if try_start_date > start_date_dt:
    start_date_dt = try_start_date
    # Convert back to str, format for DatePicker
    start_date = start_date_dt.strftime('%Y-%m-%d')

# Convert back to str, format for DatePicker
end_date = end_date_dt.strftime('%Y-%m-%d')

hour_range = [traffic_df_upt['hour'].min(), traffic_df_upt['hour'].max()]

traffic_df_upt_dt, min_date, max_date, min_hour, max_hour = filter_dt(traffic_df_upt, start_date, end_date, hour_range)

### Prepare map data ###
if not DEPLOYED:
    print('Prepare map...')

# Add column with bike/car ratio for street map representation (skip rows where car_total is 0, set to 500 i.e. most favorable bike/car ratio)
if not DEPLOYED:
    print('Add bike/car ratio column...')

# Prepare consolidated bike/car ratios by segment_id
traffic_df_id_bc = get_bike_car_ratios(traffic_df_upt)
# Extract x y coordinates from geo_df (geopandas file)
geo_df_coords = geo_df.get_coordinates()
# Get ids to join with x y coordinates
geo_df_ids = geo_df[['segment_id']]
# Join x y and segment_id into e new dataframe
geo_df_map_info = geo_df_coords.join(geo_df_ids)

# Free memory
del geo_df_coords,geo_df_ids

# Prepare geo_df_map_info and json_df_features and join
geo_df_map_info['segment_id'] = geo_df_map_info['segment_id'].astype(int)
geo_df_map_info.set_index('segment_id', drop= False, inplace=True)
json_df_features['segment_id'] = json_df_features['segment_id'].astype(int)
json_df_features.set_index('segment_id', inplace=True)
#TODO: move json_df_features to geopandas
df_map_base = geo_df_map_info.join(json_df_features)

# Free memory
del json_df_features

# Prepare map data
df_map = update_map_data(df_map_base, traffic_df_id_bc)

### Run Dash app ###
if not DEPLOYED:
    print('Starting dash ...')

#PythonAnywhere
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.BOOTSTRAP],
           meta_tags=[{'name': 'viewport', 'content': 'width=device-width, initial-scale=1'}]
           )


app.title = "Berlin-zaehlt"

server = app.server

def serve_layout():
    return dbc.Container(
        [
        dbc.NavbarSimple(
            children=[
                dbc.NavItem(dbc.NavLink(_("Project partners: "), href="#"), class_name='align-top'),
                dbc.Col(html.Img(src=app.get_asset_url('DLR_und_adfc_logos-cut.png'), title='Das Deutsche Zentrum für Luft- und Raumfahrt, Allgemeiner Deutscher Fahrrad-Club', height="50px"), className='ms-3'),
                dbc.Col(html.Img(src=app.get_asset_url('CodeFor-berlin.svg'), title='Code for Berlin', height="50px"), className='ms-3'),
                dbc.Col(html.Img(src=app.get_asset_url('Telraam.png'), title='Berlin zählt Mobilität, Citizen Science Project: ADFC Berlin, DLR & Telraam', height="50px"), className='ms-3')
            ],
            brand="Berlin zählt Mobilität",
            brand_style={'font-size': 36,'font-weight': 'bold', 'color': ADFC_darkblue, 'font-style': 'italic', 'text-shadow': '3px 2px lightblue'},
            color=ADFC_skyblue,
            dark=False,
        ),
        dbc.Row([
            # Anchor for language swithch
            dcc.Location(id='url', refresh=True),
        ]),
        dbc.Row([
            # Street map
            dbc.Col([
                dcc.Graph(id='street_map', figure={}, className='bg-#F2F2F2', style={'height': 500}),
            ],  sm=8),
            # General controls
            dbc.Col([
                dbc.Row([
                    dbc.Col([
                        html.H6('Map info', id='popover_map_info', className='text-start', style={'color': ADFC_darkgrey}),
                        dbc.Popover(dbc.PopoverBody(_('Note: street colors represent bike/car ratios based on all data available and do not change with date- or hour selection. The map allows street segments to be selected individually. To select whole streets, select a street name from the drop down menu.')), target='popover_map_info', trigger='hover', placement='bottom'),
                    ], sm=5),
                    dbc.Col([
                        # Street drop down
                        dcc.Dropdown(
                            id='language_selector',
                            options=[
                                {'label': '🇬🇧' + ' ' + _('English'), 'value': 'en'},
                                {'label': '🇩🇪' + ' ' + _('Deutsch'), 'value': 'de'},
                            ],
                            value=language
                        ),
                    ], sm=7),
                ], justify='end'),
                html.H4(_('Select street:'), className='my-2'),
                dcc.Dropdown(id='street_name_dd',
                    options=[{'label': i, 'value': i} for i in sorted(traffic_df['id_street'].unique())],
                    value= init_id_street
                ),
                #dcc.Store(id='store_segment_id_value', storage_type='memory'),
                html.Span([
                    html.H4(_('Traffic type - selected street'), id='selected_street_header', style={'color': 'black'}, className='my-2 d-inline-block'),
                    html.I(className='bi bi-info-circle-fill h6 ms-1', id='popover_traffic_type', style={'align': 'top', 'color': ADFC_lightgrey}),
                    dbc.Popover(
                        dbc.PopoverBody(_('Traffic type split of the currently selected street, based on currently selected date and hour range.')),
                    target="popover_traffic_type", trigger="hover")
                ]),
                # Pie chart
                dcc.Graph(id='pie_traffic', figure={}),
            ], sm=4),
        ], className= 'g-2 mt-1 mb-3 text-start'), #style= {'margin-right': 40}),
        # Date/Time selection and Uptime filter
        dbc.Row([
            dbc.Col([
                html.H6(_('Set hour range:'), className='ms-2 mt-2'),
                # Hour slider
                dcc.RangeSlider(
                    id='range_slider',
                    min= min_hour,
                    max= max_hour,
                    step=1,
                    value = hour_range,
                    className='align-bottom mb-2',
                    tooltip={'always_visible': False, 'placement' : 'bottom', 'template': '{value}' + _(" Hour")}),
            ], sm=6),
            dbc.Col([
                html.H6(_('Pick date range:'), className='ms-2 mt-2 text-nowrap', id='date_range_text'),
                # Date picker
                dcc.DatePickerRange(
                    id="date_filter",
                    start_date=start_date,
                    end_date=end_date,
                    min_date_allowed=min_date,
                    max_date_allowed=max_date,
                    display_format='DD-MM-YYYY',
                    end_date_placeholder_text='DD-MM-YYYY',
                    number_of_months_shown=2,
                    minimum_nights=1,
                    className='align-bottom justify-center ms-2 mb-2',
                ),
            ], sm=3),
            dbc.Col([
                html.Span([
                    dbc.Checklist(
                        id='toggle_uptime_filter',
                        options=[{'label': _(' Filter uptime > 0.7'), 'value': 'filter_uptime_selected'}],
                        value= ['filter_uptime_selected'],
                        inline=False,
                        switch=True,
                        className='d-inline-block ms-2 mt-4'
                    ),
                    html.I(className='bi bi-info-circle-fill h6 ms-2',
                        id='popover_filter',
                        style={'color': ADFC_lightgrey}),
                    dbc.Popover(
                        dbc.PopoverBody(_('A high 0.7-0.8 uptime will always mean very good data. The first and last daylight hour of the day will always have lower uptimes. If uptimes during the day are below 0.5, that is usually a clear sign that something is probably wrong with the instance.')),
                        target="popover_filter", trigger="hover"),
                ]),
                html.Span([
                        dbc.Checklist(
                        id='hardware_version',
                        options=[{'label': _('V1 Sensor'), 'value': 1}, {'label': _('S2 Sensor'), 'value': 2}],
                        value=[1, 2],
                        inline=True,
                        switch=True,
                        className='d-inline-block ms-2 mt-0'
                    ),
                    html.I(className='bi bi-info-circle-fill h6 ms-0',
                        id='popover_hardware_version',
                        style={'color': ADFC_lightgrey}),
                    dbc.Popover(
                        dbc.PopoverBody(_("Click to show/hide cameras with hardware versions 1 and or 2. Switching off both, will re-enable both automatically. Note: the 'All streets' graphs below are based on all streets, regardless which camera hardware version is selected")),
                        target="popover_hardware_version", trigger="hover"),
                ]),
            ], sm=3),
        ], className='g-2 sticky-top rounded', style={'background-color': ADFC_skyblue}),
        #Absolute traffic
        dbc.Row([
            dbc.Col([
                # Radio time division
                html.H4(_('Absolute traffic'), className='my-3'),
                # Select a time division
                dcc.RadioItems(
                    id='radio_time_division',
                    options=[
                        {'label': _('Year'), 'value': 'year'},
                        {'label': _('Month'), 'value': _('year_month')},
                        {'label': _('Week'), 'value': 'year_week'},
                        {'label': _('Day'), 'value': 'date'},
                        {'label': _('Hour'), 'value': 'date_hour'}
                    ],
                    value='date',
                    inline=True,
                    inputStyle={"margin-right": "5px", "margin-left": "20px"},
                ),
            ], sm=10),
            dbc.Col([
                html.Span([
                    html.H6([_('Download graphs   '), info_icon], id='download_html_graphs', className='my-3'),
                    dbc.Popover(
                        dbc.PopoverBody(_('Hover over the top-right of a graph and click the camera symbol to download in png-format')),
                        target="download_html_graphs", trigger="hover")
                ], style={'display': 'inline-block', 'color': ADFC_lightgrey}),
            ], sm=2),
        ], className='g-2 p-1'),
        dbc.Row([
            dbc.Col([
                dcc.Graph(id='line_abs_traffic', figure={}), #style={'margin-left': 40, 'margin-right': 40, 'margin-top': 30, 'margin-bottom': 30}),
            ], sm=12
            ),
        ], className='g-2 p-1'),
        # Average traffic
        dbc.Row([
            dbc.Col([
                # Radio time unit
                html.H4(_('Average traffic'), className='my-3'), #style={'margin-left': 40, 'margin-right': 40, 'margin-top': 30, 'margin-bottom': 30}),

                dcc.RadioItems(
                    id='radio_time_unit',
                    options=[
                        {'label': _('Yearly'), 'value': 'year'},
                        {'label': _('Monthly'), 'value': _('month')},
                        {'label': _('Weekly'), 'value': _('weekday')},
                        {'label': _('Daily'), 'value': 'day'},
                        {'label': _('Hourly'), 'value': 'hour'}
                    ],
                    value=_('weekday'),
                    inline=True,
                    inputStyle={"margin-right": "5px", "margin-left": "20px"},
                ),
            ], sm=6
            ),
        ], className='g-2 p-1'),
        dbc.Row([
            dbc.Col([
                dcc.Graph(id='bar_avg_traffic', figure={}), #style={'margin-left': 40, 'margin-right': 40, 'margin-top': 30, 'margin-bottom': 30})
            ], sm=12
            ),
        ], className='g-2 p-1'),
        dbc.Row([
            dbc.Col([
                html.H4(_('Percentage car speed - by time unit'), className='my-3'),
                dcc.Graph(id='bar_perc_speed', figure={}), #style={'margin-left': 40, 'margin-right': 40, 'margin-top': 30, 'margin-bottom': 30})
            ], sm=12
            ),
        ], className='g-2 p-1'),
        dbc.Row([
            dbc.Col([
                html.H4(_('Percentage car speed - average'), className='my-3'),
                dcc.Graph(id='bar_avg_speed', figure={}),
                          #style={'margin-left': 40, 'margin-right': 40, 'margin-top': 30, 'margin-bottom': 30})
            ], sm=12
            ),
        ], className='g-2 p-1'),
        dbc.Row([
            dbc.Col([
                html.Span([html.H4(_('v85 car speed'), className='my-3 me-2', style={'display': 'inline-block'}),
                           html.I(className='bi bi-info-circle-fill h6', id='popover_v85_speed',
                                  style={'display': 'inline-block', 'color': ADFC_lightgrey})]),
                dbc.Popover(
                    dbc.PopoverBody(
                        _('The V85 is a widely used indicator in the world of mobility and road safety, as it is deemed to be representative of the speed one can reasonably maintain on a road.')),
                    target='popover_v85_speed',
                    trigger='hover'
                ),
                dcc.Graph(id='bar_v85', figure={}),
            ], sm=12
            ),
        ], className='g-2 p-1'),
        # Ranking bar chart
        dbc.Row([
            dbc.Col([
                html.H4(_('Street ranking by traffic type'), className='my-3'),
            ], sm=12
            ),
        ], className='g-2 p-1'),
        dbc.Row([
            dbc.Col([
                dcc.RadioItems(
                    id='radio_y_axis',
                    options=[
                        {'label': _('Pedestrians'), 'value': 'ped_total'},
                        {'label': _('Bikes'), 'value': 'bike_total'},
                        {'label': _('Cars'), 'value': 'car_total'},
                        {'label': _('Heavy'), 'value': 'heavy_total'},
                    ],
                    value='car_total',
                    inline=True,
                    inputStyle={"margin-right": "5px", "margin-left": "20px"},
                ),
            ], sm=12
            ),
        ], className='g-2 p-1'),
        dbc.Row([
            dbc.Col([
                dcc.Graph(id='bar_ranking', figure={})
            ], sm=12
            ),
        ], className='g-2 p-1 mb-3'),

        ## Compare traffic graph
        dbc.Row([
            dbc.Col([
                dbc.Col([
                    dbc.Col([
                        html.H6(_('Year') +' A:', className='fw-bold text-end'),
                        html.H6(_('Year') +' B:', className='fw-bold text-end'),
                        ], className='d-inline-block align-top me-3 my-3', style={'min-width': '90px'}),
                    dbc.Col([
                        dcc.Dropdown(
                            id='dropdown_year_A',
                            options=[{'label': i, 'value': i} for i in traffic_df['year'].unique()],
                            value=traffic_df['year'][len(traffic_df['year']) - 1],
                            clearable=False,
                            style={'min-width': '180px'}
                        ),
                        dcc.Dropdown(
                            id='dropdown_year_B',
                            options=[{'label': i, 'value': i} for i in traffic_df['year'].unique()],
                            value=traffic_df['year'][1],
                            clearable=False,
                            style={'min-width': '180px'}
                        ),
                    ], className='d-inline-block'),
                ], className='d-inline-block'),
                dbc.Col([
                    dbc.Col([
                        html.H6(_('Month')+' A:', className='fw-bold text-end'),
                        html.H6(_('Month')+' B:', className='fw-bold text-end'),
                    ], className='d-inline-block align-top me-3 my-3', style={'min-width': '90px'}),
                    dbc.Col([
                        dcc.Dropdown(
                            id='dropdown_year_month_A',
                            options=[{'label': i, 'value': i} for i in traffic_df['year_month'].unique()],
                            value=traffic_df['year_month'][len(traffic_df['year_month']) - 1],
                            clearable=False,
                            style={'min-width': '180px'}
                        ),
                        dcc.Dropdown(
                            id='dropdown_year_month_B',
                            options=[{'label': i, 'value': i} for i in traffic_df['year_month'].unique()],
                            value=traffic_df['year_month'][1],
                            clearable=False,
                            style={'min-width': '180px'}
                        ),
                    ], className='d-inline-block'),
                ], className='d-inline-block'),
            ]),
            dbc.Col([
                dbc.Col([
                    dbc.Col([
                        html.H6(_('Week') + ' A:', className='fw-bold text-end'),
                        html.H6(_('Week') + ' B:', className='fw-bold text-end'),
                    ], className='d-inline-block align-top me-3 my-3', style={'min-width': '90px'}),
                    dbc.Col([
                        dcc.Dropdown(
                            id='dropdown_year_week_A',
                            options=[{'label': i, 'value': i} for i in traffic_df['year_week'].unique()],
                            value=traffic_df['year_week'][len(traffic_df['year_week']) - 1],
                            clearable=False,
                            style={'min-width': '180px'}
                        ),
                        dcc.Dropdown(
                            id='dropdown_year_week_B',
                            options=[{'label': i, 'value': i} for i in traffic_df['year_week'].unique()],
                            value=traffic_df['year_week'][1],
                            clearable=False,
                            style={'min-width': '180px'}
                        ),
                    ], className='d-inline-block'),
                ], className='d-inline-block'),
                dbc.Col([
                    dbc.Col([
                        html.H6(_('Day') + ' A:', className='fw-bold text-end'),
                        html.H6(_('Day') + ' B:', className='fw-bold text-end'),
                    ], className='d-inline-block align-top me-3 my-3', style={'min-width': '90px'}),
                    dbc.Col([
                        dcc.Dropdown(
                            id='dropdown_date_A',
                            options=[{'label': i, 'value': i} for i in traffic_df['date'].unique()],
                            value=traffic_df['date'][len(traffic_df['date']) - 1],
                            clearable=False,
                            style={'min-width': '180px'}
                        ),
                        dcc.Dropdown(
                            id='dropdown_date_B',
                            options=[{'label': i, 'value': i} for i in traffic_df['date'].unique()],
                            value=traffic_df['date'][1],
                            clearable=False,
                            style={'min-width': '180px'}
                        ),
                    ], className='d-inline-block'),
                ], className='d-inline-block'),
            ]),
        ], className='sticky-top rounded g-2 p-1 d-flex flex-wrap', style={'background-color': ADFC_skyblue, 'opacity': 1.0}),
        dbc.Row([
            html.Span(
                [html.H4(_('Compare traffic periods'), className='my-3 me-2', style={'display': 'inline-block'}),
                 html.I(className='bi bi-info-circle-fill h6', id='compare_traffic_periods',
                        style={'display': 'inline-block', 'color': ADFC_lightgrey})]),
            dbc.Popover(
                dbc.PopoverBody(
                    _('This chart allows four period-lengths to be compared: day, week, month or year. For each of these, two periods can be compared, period A and period B (e.g. week A vs. week B or day A vs. day B). Solid lines represent period A and dashed lines represent period B. The date and hour filters in the upper menu bar have no effect, however \'filter uptime\' does!')),
                target='compare_traffic_periods',
                trigger='hover'
            ),
        ], className='g-2 p-1 mb-3'),
        dbc.Row([
            dbc.Col([
                dcc.Graph(id='line_avg_delta_traffic', figure={})
            ], sm=12),
        ], className='g-2 p-1 mb-3'),

        ### Feedback and contact
        dbc.Row([
            dbc.Col([
                html.H4(_('Feedback and contact'), className='ms-2, my-2'),
            ], className= 'ms-3', sm=12),
            dbc.Col([
                html.H6([_('More information about the '),
                        html.A('Berlin zählt Mobilität', href='https://adfc-tk.de/wir-zaehlen/', target="_blank"),_(' (BzM) initiative'),],
                        #style={'margin-left': 40, 'margin-right': 40, 'margin-top': 10, 'margin-bottom': 10},
                        className='ms-2',
                       ),
                html.H6([_('Request a counter at the '),
                        html.A(_('Citizen Science-Projekt'), href="https://telraam.net/en/candidates/berlin-zaehlt-mobilitaet/berlin-zaehlt-mobilitaet", target="_blank"),],
                        className='ms-2',
                        ),
                html.H6([_('Data protection around the '),
                        html.A(_('Telraam camera'), href="https://telraam.net/home/blog/telraam-privacy", target="_blank"),_(' measurements'),],
                        className='ms-2',
                        ),
            ], className= 'ms-3', sm=5),
            dbc.Col([
                html.H6([_('Dashboard development & creation:'),  html.Br(), ('Egbert Klaassen'), _(' and '),('Michael Behrisch')],
                        className='ms-2',
                        ), #className='ms-5'),
                html.H6([_('For dashboard improvement requests email us:')],
                        className='ms-2',
                        ), #className='my-2'),
            ], className= 'ms-3', sm=4),
            dbc.Col([
                dbc.Button([_('Contact Us'), html.Br(), email_icon],
                    id='floating_button',
                    class_name='btn-info rounded-pill',  # rounded-pill
                    href='mailto: kontakt@berlin-zaehlt.de',
                ),
            ], className='ms-4', sm=2),
        ], className= 'rounded text-black g-0 p-1 mb-3', style={'background-color': ADFC_yellow, 'opacity': 1.0}),

        ### Legal disclaimeers
        dbc.Row([
            dbc.Col([
                html.P(_('Disclaimer'), style= {'font-size': 12, 'color': ADFC_darkgrey}),
                html.P(_('The content published in the offer has been researched with the greatest care. Nevertheless, the Berlin Counts Mobility team cannot assume any liability for the topicality, correctness or completeness of the information provided. All information is provided without guarantee. liability claims against the Berlin zählt Mobilität team or its supporting organizations derived from the use of this information are excluded. Despite careful control of the content, the Berlin zählt Mobilität team and its supporting organizations assume no liability for the content of external links. The operators of the linked pages are solely responsible for their content. A constant control of the external links is not possible for the provider. If there are indications or knowledge of legal violations, the illegal links will be deleted immediately.'), style= {'font-size': 10, 'color': ADFC_darkgrey}),
                html.P(_('Copyright'), style= {'font-size': 12, 'color': ADFC_darkgrey}),
                html.P(_('The layout and design of the offer as a whole as well as its individual elements are protected by copyright. The same applies to the images, graphics and editorial contributions used in detail as well as their selection and compilation. Further use and reproduction are only permitted for private purposes. No changes may be made to it. Public use of the offer may only take place with the consent of the operator.'), style= {'font-size': 10, 'color': ADFC_darkgrey}),
            ], sm=12),
        ], className='g-2 p-1'),
    ],
    fluid = 'sm',
    className = 'dbc'
)

app.layout = serve_layout


@app.callback(
    Output('url', 'href'),
    Input('language_selector', 'value'),
    prevent_initial_call=True
)
def get_language(lang_code_dd):
    update_language(lang_code_dd)
    return '/'

### Map callback ###
@callback(
    Output(component_id='street_name_dd',component_property='value', allow_duplicate= True),
    Input(component_id='street_map', component_property='clickData'),
    prevent_initial_call=True
)

def get_street_name(clickData):

    if clickData:
        # Get street name and segment id from map click
        street_name = clickData['points'][0]['hovertext']
        segment_id = str(clickData['points'][0]['customdata'][0])

        # Check if street inactive, if so, prevent update
        idx = df_map.loc[df_map['segment_id'] == segment_id]
        map_color_status = idx['map_line_color'].values[0]
        if map_color_status == 'Inactive - no data':
            raise PreventUpdate

        # Otherwise, change to selected street
        id_street = street_name + ' (' + segment_id + ')'

    return id_street

@callback(
    Output(component_id='street_map', component_property='figure'),
    Output(component_id='hardware_version', component_property='value'),
    Output(component_id='street_name_dd', component_property='options'),
    Output(component_id='street_name_dd', component_property='value'),
    Input(component_id='street_map', component_property='clickData'),
    Input(component_id='street_name_dd', component_property='value'),
    Input(component_id='language_selector',component_property= 'value'),
    Input(component_id='hardware_version',component_property= 'value'),
)

def update_map(clickData, id_street, lang_code_dd, hardware_version):
    callback_trigger = ctx.triggered_id

    # Get hardware version of currently selected street
    current_hw = int(df_map.loc[df_map['id_street'] == id_street, 'hardware_version'].iloc[0])

    # Camera hardware version change
    if callback_trigger == 'hardware_version':
        if hardware_version == [1]:
            df_map_hw = df_map[df_map['hardware_version'] == 1]
            # Switch selected street if camera hardware version does not fit selection
            if current_hw == 2:
                id_street = 'Alte Jakobstraße (9000002582)'
        elif hardware_version == [2]:
            df_map_hw = df_map[df_map['hardware_version'] == 2]
            # Switch selected street if camera hardware version does not fit selection
            if current_hw == 1:
                id_street = 'Dresdener Straße (9000006667)'
        else:
            # Set both camera hardware versions if both or none are selected
            hardware_version = [1, 2]
            df_map_hw = df_map
    else:
        df_map_hw = df_map

    # Update options for street_name_dd, remove inactive
    df_map_hw_options = df_map_hw[df_map_hw['map_line_color']!='Inactive - no data']
    street_name_dd_options = [{'label': i, 'value': i} for i in sorted(df_map_hw_options['id_street'].unique())]
    # Free up memory
    del df_map_hw_options

    if callback_trigger == 'street_map':
        street_name = clickData['points'][0]['hovertext']
        segment_id = clickData['points'][0]['customdata'][0]
        idx = df_map_hw.loc[df_map_hw['segment_id'] == segment_id]
        # Check if street inactive, if so, prevent update
        map_color_status = idx['map_line_color'].values[0]
        if map_color_status == 'Inactive - no data':
            raise PreventUpdate
        else:
            if hardware_version == [1] or hardware_version == [2]:
                # Provide overview after camera hardware version change
                zoom_factor = 11
            else:
                zoom_factor = 13
    elif callback_trigger == 'street_name_dd' or hardware_version == [1] or hardware_version == [2]:
        street_name = id_street.split(' (')[0]
        segment_id = id_street[-11:-1]
        idx = df_map_hw.loc[df_map_hw['segment_id'] == segment_id]
        if hardware_version == [1] or hardware_version == [2]:
            # Provide overview after camera hardware version change
            zoom_factor = 11
        else:
            zoom_factor = 13
    else:
        # Initial view
        street_name = id_street.split(' (')[0]
        segment_id = id_street[-11:-1]
        idx = df_map_hw.loc[df_map_hw['segment_id'] == segment_id]
        zoom_factor = 11

    # TODO: improve efficiency by managing translation w/o recalculating bc ratios
    lon_str = idx['x'].values[0]
    lat_str = idx['y'].values[0]

    sep = '&nbsp;|&nbsp;'
    street_map = px.line_map(df_map_hw, lat='y', lon='x', custom_data=['segment_id', 'hardware_version'],line_group='segment_id', hover_name = 'osm.name', color= 'map_line_color',
        color_discrete_map= {
        'More bikes than cars': ADFC_green,
        'More cars than bikes': ADFC_blue,
        'Over 2x more cars': ADFC_orange,
        'Over 5x more cars': ADFC_crimson,
        'Over 10x more cars': ADFC_pink,
        'Inactive - no data': ADFC_lightgrey},
        hover_data={'map_line_color': False, 'osm.highway': True, 'osm.address.city': True, 'osm.address.suburb': True, 'osm.address.postcode': True, 'hardware_version': True},
        labels={'segment_id': 'Segment', 'osm.highway': _('Highway type'), 'x': 'Lon', 'y': 'Lat', 'osm.address.city': _('City'), 'osm.address.suburb': _('District'), 'osm.address.postcode': _('Postal code')},
        map_style="streets", center= dict(lat=lat_str, lon=lon_str), zoom= zoom_factor)

    street_map.update_traces(line_width=5, opacity=1.0)
    street_map.update_traces({'name': _('More bikes than cars')}, selector={'name': 'More bikes than cars'})
    street_map.update_traces({'name': _('More cars than bikes')}, selector={'name': 'More cars than bikes'})
    street_map.update_traces({'name': _('Over 2x more cars')}, selector={'name': 'Over 2x more cars'})
    street_map.update_traces({'name': _('Over 5x more cars')}, selector={'name': 'Over 5x more cars'})
    street_map.update_traces({'name': _('Over 10x more cars')}, selector={'name': 'Over 10x more cars'})
    street_map.update_traces({'name': _('Inactive - no data')}, selector={'name': 'Inactive - no data'}, visible='legendonly')
    street_map.update_layout(autosize=False)
    street_map.update_layout(margin=dict(l=0, r=0, t=0, b=0))
    street_map.update_layout(legend_title=_('Street color'))
    street_map.update_layout(legend=dict(yanchor="top", y=0.99, xanchor="right", x=0.99))
    street_map.update_layout(annotations=[
        dict(
            text=(
                sep.join([
                    '<a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>',
                    '<a href="https://telraam.net">Telraam</a>',
                    '<a href="https://www.berlin.de/sen/uvk/mobilitaet-und-verkehr/verkehrsplanung/radverkehr/weitere-radinfrastruktur/zaehlstellen-und-fahrradbarometer/">SenUMVK Berlin<br></a>'
                ]) + '' +
                sep.join([
                    '<a href="https://berlin-zaehlt.de/csv/">CSV data</a> under <a href="https://creativecommons.org/licenses/by/4.0/">CC-BY 4.0</a> and <a href="https://www.govdata.de/dl-de/by-2-0">dl-de/by-2-0</a>'
                ])
            ),
            showarrow=False, align='left', xref='paper', yref='paper', x=0, y=0
        )
    ])

    return street_map, hardware_version, street_name_dd_options, id_street


### General traffic callback ###
@callback(
    Output(component_id='selected_street_header', component_property='children'),
    Output(component_id='selected_street_header', component_property='style'),
    Output(component_id='date_range_text', component_property='children'),
    Output(component_id='date_range_text', component_property='style'),
    Output(component_id='pie_traffic', component_property='figure'),
    Output(component_id='line_abs_traffic', component_property='figure'),
    Output(component_id='bar_avg_traffic', component_property='figure'),
    Output(component_id='line_avg_delta_traffic', component_property='figure'),
    Output(component_id='bar_perc_speed', component_property='figure'),
    Output(component_id='bar_avg_speed', component_property='figure'),
    Output(component_id='bar_v85', component_property='figure'),
    Output(component_id='bar_ranking', component_property='figure'),
    Input(component_id='radio_time_division', component_property='value'),
    Input(component_id='radio_time_unit', component_property='value'),
    Input(component_id='street_name_dd', component_property='value'),
    Input(component_id='dropdown_year_A', component_property='value'),
    Input(component_id='dropdown_year_month_A', component_property='value'),
    Input(component_id='dropdown_year_week_A', component_property='value'),
    Input(component_id='dropdown_date_A', component_property='value'),
    Input(component_id='dropdown_year_B', component_property='value'),
    Input(component_id='dropdown_year_month_B', component_property='value'),
    Input(component_id='dropdown_year_week_B', component_property='value'),
    Input(component_id='dropdown_date_B', component_property='value'),
    Input(component_id="date_filter", component_property="start_date"),
    Input(component_id="date_filter", component_property="end_date"),
    Input(component_id='range_slider', component_property='value'),
    Input(component_id='toggle_uptime_filter', component_property='value'),
    Input(component_id='radio_y_axis', component_property='value'),
    Input(component_id='floating_button', component_property='n_clicks'),
    Input(component_id='language_selector', component_property='value'),
#prevent_initial_call='initial_duplicate',
)

def update_graphs(radio_time_division, radio_time_unit, id_street, dropdown_year_A, dropdown_year_month_A, dropdown_year_week_A, dropdown_date_A, dropdown_year_B, dropdown_year_month_B, dropdown_year_week_B, dropdown_date_B, start_date, end_date, hour_range, toggle_uptime_filter, radio_y_axis, floating_button, lang_code_dd):

    callback_trigger = ctx.triggered_id

    # If uptime filter changed, reload traffic_df_upt
    if 'filter_uptime_selected' in toggle_uptime_filter:
        traffic_df_upt = filter_uptime(traffic_df)
    else:
        traffic_df_upt = traffic_df

    # Get segment_id/street name
    segment_id = id_street[-11:-1]
    street_name = id_street.split(' (')[0]
    selected_street_header = street_name

    #map_color_status = df_map.loc[df_map['segment_id'] == segment_id, 'map_line_color'].iloc[0]
    #print(map_color_status)
    #if map_color_status == 'Inactive - no data':
    #    selected_street_header = 'Select active street'
    #    selected_street_header_color = {'color': ADFC_lightgrey}
    #else:
    #    selected_street_header = street_name

    # Check if selected street has data for selected data range
    min_date_str, max_date_str, start_date, end_date, message, missing_data = get_min_max_str(traffic_df_upt, id_street, start_date, end_date)
    traffic_df_upt_dt, min_date, max_date, min_hour, max_hour = filter_dt(traffic_df_upt, start_date, end_date, hour_range)
    traffic_df_upt_dt_str = update_selected_street(traffic_df_upt_dt, segment_id, street_name)

    # Format min_max output
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').strftime('%d %b %Y')
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').strftime('%d %b %Y')
    min_date_str = datetime.datetime.strptime(min_date_str, '%Y-%m-%d').strftime('%d-%m-%Y')
    max_date_str = datetime.datetime.strptime(max_date_str, '%Y-%m-%d').strftime('%d-%m-%Y')

    # Provide warnings in case of missing data
    if missing_data:
        # Add warnings to layout
        date_range_text = _(message +', ' + _('available') + ': ' + min_date_str + _(' to ') + max_date_str)
        if message == _('Dates out of range'):
            selected_street_header_color = {'color': ADFC_crimson}
            date_range_color = {'color': ADFC_crimson}
        else:
            selected_street_header_color = {'color': ADFC_orange}
            date_range_color = {'color': ADFC_orange}
    else:
        # Street data range covered
        selected_street_header_color = {'color': ADFC_green}
        date_range_text = _('Pick date range:')
        date_range_color = {'color': 'black'}

    # Create pie chart
    df_pie = traffic_df_upt_dt_str[traffic_df_upt_dt_str['street_selection'] == street_name]
    df_pie_traffic = df_pie[['ped_total', 'bike_total', 'car_total', 'heavy_total']]
    df_pie_traffic_ren = df_pie_traffic.rename(columns={'ped_total': _('Pedestrians'), 'bike_total': _('Bikes'), 'car_total': _('Cars'), 'heavy_total': _('Heavy')})
    df_pie_traffic_sum = df_pie_traffic_ren.aggregate(['sum'])
    df_pie_traffic_sum_T = df_pie_traffic_sum.transpose().reset_index()

    pie_traffic = px.pie(df_pie_traffic_sum_T, names='index', values='sum', color='index', height=300,
    color_discrete_map={_('Pedestrians'): ADFC_lightblue, _('Bikes'): ADFC_green, _('Cars'): ADFC_orange, _('Heavy'): ADFC_crimson})

    pie_traffic.update_layout(margin=dict(l=00, r=00, t=00, b=00))
    pie_traffic.update_layout(showlegend=False)
    pie_traffic.update_traces(textposition='inside', textinfo='percent+label')

    ### Create absolute line chart
    if radio_time_division == 'date_hour':
        df_line_abs_traffic = traffic_df_upt_dt_str.groupby(by=['street_selection', 'date_local', radio_time_division], sort=False, as_index=False).agg({'ped_total': 'sum', 'bike_total': 'sum', 'car_total': 'sum', 'heavy_total': 'sum'})
        df_line_abs_traffic = df_line_abs_traffic.sort_values(by=['date_local'], ascending=True)
    else:
        df_line_abs_traffic = traffic_df_upt_dt_str.groupby(by=[radio_time_division, 'street_selection'], sort=False, as_index=False).agg({'ped_total': 'sum', 'bike_total': 'sum', 'car_total': 'sum', 'heavy_total': 'sum'})

    line_abs_traffic = px.scatter(df_line_abs_traffic,
        x=radio_time_division, y=['ped_total', 'bike_total', 'car_total', 'heavy_total'],
        #markers=True, # In case of line graph
        facet_col='street_selection',
        category_orders={'street_selection': [street_name, 'All Streets']},
        labels={'year': _('Year'), _('year_month'): _('Month'), 'year_week': _('Week'), 'date': _('Day'), 'date_hour': _('Hour')},
        color_discrete_map={'ped_total': ADFC_lightblue, 'bike_total': ADFC_green, 'car_total': ADFC_orange, 'heavy_total': ADFC_crimson},
        facet_col_spacing=0.04,
        title=_('Absolute traffic count')
    ).update_traces(mode="lines+markers", connectgaps=False)

    line_abs_traffic.update_layout({'plot_bgcolor': ADFC_palegrey, 'paper_bgcolor': ADFC_palegrey})
    line_abs_traffic.update_layout(legend_title_text=_('Traffic Type'))
    #line_abs_traffic.update_layout(legend=dict(orientation='h', yanchor= 'bottom', y= 1.14, xanchor= 'right', x=0.65))
    line_abs_traffic.update_layout(yaxis_title= _('Absolute traffic count'))
    line_abs_traffic.update_yaxes(matches=None)
    line_abs_traffic.update_xaxes(matches=None)
    line_abs_traffic.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))
    line_abs_traffic.for_each_annotation(lambda a: a.update(text=a.text.split("=")[1]))
    for annotation in line_abs_traffic.layout.annotations: annotation['font'] = {'size': 14}
    line_abs_traffic.update_traces({'name': _('Pedestrians')}, selector={'name': 'ped_total'})
    line_abs_traffic.update_traces({'name': _('Bikes')}, selector={'name': 'bike_total'})
    line_abs_traffic.update_traces({'name': _('Cars')}, selector={'name': 'car_total'})
    line_abs_traffic.update_traces({'name': _('Heavy')}, selector={'name': 'heavy_total'})
    line_abs_traffic.for_each_annotation(lambda a: a.update(text=a.text.replace(street_name, street_name + _(' (segment:') + segment_id + ')')))
    line_abs_traffic.for_each_annotation(lambda a: a.update(text=a.text.replace('All Streets', _('All Streets'))))

    ### Create average traffic bar chart
    # Sort on hour to avoid line graph jumps around hour gaps
    if radio_time_unit == 'hour' or radio_time_unit == 'day':
        #df_avg_traffic = df_avg_traffic.sort_values(by=[radio_time_unit], ascending=True)
        df_avg_traffic = traffic_df_upt_dt_str.groupby(by=[radio_time_unit, 'street_selection'], as_index=False).agg({'ped_total': 'mean', 'bike_total': 'mean', 'car_total': 'mean', 'heavy_total': 'mean'})
    else:
        df_avg_traffic = traffic_df_upt_dt_str.groupby(by=[radio_time_unit, 'street_selection'], sort=False, as_index=False).agg({'ped_total': 'mean', 'bike_total': 'mean', 'car_total': 'mean', 'heavy_total': 'mean'})

    bar_avg_traffic = px.bar(df_avg_traffic,
        x=radio_time_unit, y=['ped_total', 'bike_total', 'car_total', 'heavy_total'],
        barmode='stack',
        facet_col='street_selection',
        facet_col_spacing=0.04,
        category_orders={'street_selection': [street_name, 'All Streets']},
        labels={'year': _('Year'), 'month': _('Month'), 'weekday': _('Week'), 'day': _('Day'), 'hour': _('Hour')},
        color_discrete_map={'ped_total': ADFC_lightblue, 'bike_total': ADFC_green, 'car_total': ADFC_orange, 'heavy_total': ADFC_crimson},
        title=(_('Average traffic count')  + ' (' + start_date + ' - ' + end_date + ', ' + str(hour_range[0]) + ' - ' + str(hour_range[1]) + ' h)')
    )

    bar_avg_traffic.for_each_annotation(lambda a: a.update(text=a.text.replace(street_name, street_name + _(' (segment:') + segment_id + ')')))
    bar_avg_traffic.for_each_annotation(lambda a: a.update(text=a.text.replace('All Streets', _('All Streets'))))
    bar_avg_traffic.for_each_annotation(lambda a: a.update(text=a.text.split("=")[1]))
    bar_avg_traffic.update_layout({'plot_bgcolor': ADFC_palegrey,'paper_bgcolor': ADFC_palegrey})
    bar_avg_traffic.update_layout(yaxis_title=_('Average traffic count'))
    bar_avg_traffic.update_layout(legend_title_text=_('Traffic Type'))
    bar_avg_traffic.update_traces({'name': _('Pedestrians')}, selector={'name': 'ped_total'})
    bar_avg_traffic.update_traces({'name': _('Bikes')}, selector={'name': 'bike_total'})
    bar_avg_traffic.update_traces({'name': _('Cars')}, selector={'name': 'car_total'})
    bar_avg_traffic.update_traces({'name': _('Heavy')}, selector={'name': 'heavy_total'})
    bar_avg_traffic.update_xaxes(dtick = 1, tickformat=".0f")
    bar_avg_traffic.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))
    for annotation in bar_avg_traffic.layout.annotations: annotation['font'] = {'size': 14}


    ### Create percentage speed bar chart

    # Add column with all car speed %
    df_bar_speed = traffic_df_upt_dt_str
    cols = ['car_speed0', 'car_speed10', 'car_speed20', 'car_speed30', 'car_speed40', 'car_speed50', 'car_speed60', 'car_speed70']
    df_bar_speed['sum_speed_perc'] = df_bar_speed[cols].sum(axis=1)

    # Drop empty rows
    nan_rows = df_bar_speed[df_bar_speed['sum_speed_perc']==0]
    df_bar_speed = df_bar_speed.drop(nan_rows.index)

    df_bar_speed_traffic = df_bar_speed.groupby(by=[radio_time_unit, 'street_selection'], sort= False, as_index=False).agg({'car_speed0': 'mean', 'car_speed10': 'mean', 'car_speed20': 'mean', 'car_speed30': 'mean', 'car_speed40': 'mean', 'car_speed50': 'mean', 'car_speed60': 'mean', 'car_speed70': 'mean'})
    bar_perc_speed = px.bar(df_bar_speed_traffic,
         x=radio_time_unit, y=cols,
         barmode='stack',
         facet_col='street_selection',
         category_orders={'street_selection': [street_name, 'All Streets']},
         labels={'year': _('Year'), 'month': _('Month'), 'weekday': _('Week'), 'day': _('Day'), 'hour': _('Hour')},
         color_discrete_map={'car_speed0': ADFC_lightgrey, 'car_speed10': ADFC_lightblue,
                             'car_speed20': ADFC_lightblue, 'car_speed30': ADFC_green,
                             'car_speed40': ADFC_green, 'car_speed50': ADFC_orange,
                             'car_speed60': ADFC_crimson, 'car_speed70': ADFC_pink},
         facet_col_spacing=0.04,
         title=(_('Percentage car speed') + ' (' + start_date + ' - ' + end_date + ', ' + str(hour_range[0]) + ' - ' + str(hour_range[1]) + ' h)')
    )

    bar_perc_speed.for_each_annotation(lambda a: a.update(text=a.text.split("=")[1]))
    bar_perc_speed.for_each_annotation(lambda a: a.update(text=a.text.replace(street_name, street_name + _(' (segment:') + segment_id + ')')))
    bar_perc_speed.for_each_annotation(lambda a: a.update(text=a.text.replace('All Streets', _('All Streets'))))
    bar_perc_speed.update_layout(legend_title_text=_('Car speed'))
    bar_perc_speed.update_traces({'name': ' 0 kmh'}, selector={'name': 'car_speed0'})
    bar_perc_speed.update_traces({'name': _('until') + ' 10 kmh'}, selector={'name': 'car_speed10'})
    bar_perc_speed.update_traces({'name': _('until') + ' 20 kmh'}, selector={'name': 'car_speed20'})
    bar_perc_speed.update_traces({'name': _('until') + ' 30 kmh'}, selector={'name': 'car_speed30'})
    bar_perc_speed.update_traces({'name': _('until') + ' 40 kmh'}, selector={'name': 'car_speed40'})
    bar_perc_speed.update_traces({'name': _('until') + ' 50 kmh'}, selector={'name': 'car_speed50'})
    bar_perc_speed.update_traces({'name': _('until') + ' 60 kmh'}, selector={'name': 'car_speed60'})
    bar_perc_speed.update_traces({'name': _('until') + ' 70 kmh'}, selector={'name': 'car_speed70'})
    bar_perc_speed.update_layout({'plot_bgcolor': ADFC_palegrey, 'paper_bgcolor': ADFC_palegrey})
    bar_perc_speed.update_layout(yaxis_title=_('Percentage car speed'))
    bar_perc_speed.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))
    for annotation in bar_perc_speed.layout.annotations:
        annotation['font'] = {'size': 14}

    ### Create percentage speed average bar chart
    df_bar_avg_speed_traffic = df_bar_speed.groupby(by=[radio_time_unit, 'street_selection'], sort= False, as_index=False).agg({'car_speed0': 'mean', 'car_speed10': 'mean', 'car_speed20': 'mean', 'car_speed30': 'mean', 'car_speed40': 'mean', 'car_speed50': 'mean', 'car_speed60': 'mean', 'car_speed70': 'mean'})

    bar_avg_speed = px.bar(df_bar_avg_speed_traffic,
        x=radio_time_unit, y=cols,
        barmode='group',
        facet_col='street_selection',
        category_orders={'street_selection': [street_name, 'All Streets']},
        labels={'year': _('Year'), 'month': _('Month'), 'weekday': _('Week'), 'day': _('Day'), 'hour': _('Hour')},
        color_discrete_map={'car_speed0': ADFC_lightgrey, 'car_speed10': ADFC_lightblue,
                            'car_speed20': ADFC_lightblue, 'car_speed30': ADFC_green,
                            'car_speed40': ADFC_green, 'car_speed50': ADFC_orange,
                            'car_speed60': ADFC_crimson, 'car_speed70': ADFC_pink},
        facet_col_spacing=0.04,
        title=(_('Average percentage car speed') + ' (' + start_date + ' - ' + end_date + ', ' + str(hour_range[0]) + ' - ' + str(hour_range[1]) + ' h)')
    )

    bar_avg_speed.for_each_annotation(lambda a: a.update(text=a.text.split("=")[1]))
    bar_avg_speed.for_each_annotation(lambda a: a.update(text=a.text.replace(street_name, street_name + _(' (segment:') + segment_id + ')')))
    bar_avg_speed.for_each_annotation(lambda a: a.update(text=a.text.replace('All Streets', _('All Streets'))))
    bar_avg_speed.update_layout(legend_title_text=_('Car speed'))
    bar_avg_speed.update_traces({'name': ' 0 kmh'}, selector={'name': 'car_speed0'})
    bar_avg_speed.update_traces({'name': _('until') + ' 10 kmh'}, selector={'name': 'car_speed10'})
    bar_avg_speed.update_traces({'name': _('until') + ' 20 kmh'}, selector={'name': 'car_speed20'})
    bar_avg_speed.update_traces({'name': _('until') + ' 30 kmh'}, selector={'name': 'car_speed30'})
    bar_avg_speed.update_traces({'name': _('until') + ' 40 kmh'}, selector={'name': 'car_speed40'})
    bar_avg_speed.update_traces({'name': _('until') + ' 50 kmh'}, selector={'name': 'car_speed50'})
    bar_avg_speed.update_traces({'name': _('until') + ' 60 kmh'}, selector={'name': 'car_speed60'})
    bar_avg_speed.update_traces({'name': _('until') + ' 70 kmh'}, selector={'name': 'car_speed70'})
    bar_avg_speed.update_layout({'plot_bgcolor': ADFC_palegrey, 'paper_bgcolor': ADFC_palegrey})
    bar_avg_speed.update_layout(yaxis_title=_('Percentage car speed'))
    bar_avg_speed.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))
    for annotation in bar_avg_speed.layout.annotations: annotation['font'] = {'size': 14}

    ### Create v85 bar graph
    df_bar_v85 = traffic_df_upt_dt_str.groupby(by=[radio_time_unit, 'street_selection'], sort= False, as_index=False).agg({'v85': 'mean'})

    bar_v85 = px.bar(df_bar_v85,
        x=radio_time_unit, y='v85',
        color='v85',
        color_continuous_scale='temps',
        facet_col='street_selection',
        category_orders={'street_selection': [street_name, 'All Streets']},
        facet_col_spacing=0.04,
        labels={'year': _('Year'), 'month': _('Month'), 'weekday': _('Week'), 'day': _('Day'), 'hour': _('Hour')},
        title=(_('Speed cars v85') + ' (' + start_date + ' - ' + end_date + ', ' + str(hour_range[0]) + ' - ' + str(hour_range[1]) + ' h)')
    )

    bar_v85.for_each_annotation(lambda a: a.update(text=a.text.split("=")[1]))
    bar_v85.for_each_annotation(lambda a: a.update(text=a.text.replace(street_name, street_name + _(' (segment:') + segment_id + ')')))
    bar_v85.for_each_annotation(lambda a: a.update(text=a.text.replace('All Streets', _('All Streets'))))
    bar_v85.update_layout(legend_title_text=_('Traffic Type'))
    bar_v85.update_layout({'plot_bgcolor': ADFC_palegrey,'paper_bgcolor': ADFC_palegrey})
    bar_v85.update_layout(yaxis_title= _('v85 in km/h'))
    bar_v85.update_xaxes(dtick=1, tickformat=".0f")
    bar_v85.update_yaxes(dtick=5, tickformat=".0f")
    bar_v85.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))
    for annotation in bar_v85.layout.annotations:
        annotation['font'] = {'size': 14}

    ### Create ranking chart
    df_bar_ranking = traffic_df_upt_dt.groupby(by=['id_street', 'street_selection'], sort=False, as_index=False).agg({'ped_total': 'sum', 'bike_total': 'sum', 'car_total': 'sum', 'heavy_total': 'sum'})
    df_bar_ranking = df_bar_ranking.sort_values(by=[radio_y_axis], ascending=False)
    df_bar_ranking.reset_index(inplace=True)

    # Assess x and y for annotation
    #if not missing_data:
    annotation_index = df_bar_ranking[df_bar_ranking['id_street'] == id_street].index[0]
    annotation_x = annotation_index
    annotation_y = df_bar_ranking[radio_y_axis].values[annotation_x]

    bar_ranking = px.bar(df_bar_ranking,
        x='id_street', y=radio_y_axis,
        color=radio_y_axis,
        color_continuous_scale='temps',
        labels={'ped_total': _('Pedestrians'), 'bike_total': _('Bikes'), 'car_total': _('Cars'), 'heavy_total': _('Heavy'), 'id_street': _('Street (segment id)')},
        title=(_('Absolute traffic') + ' (' + start_date + ' - ' + end_date + ', ' + str(hour_range[0]) + ' - ' + str(hour_range[1]) + ' h)'),
        height=600,
    )

    bar_ranking.for_each_annotation(lambda a: a.update(text=a.text.split("=")[1]))
    bar_ranking.add_annotation(x=annotation_x, y=annotation_y, text= street_name + '<br>' + _(' (segment:') + segment_id + ')', showarrow=True)
    bar_ranking.update_annotations(ax=0, ay=-40, arrowhead=2, arrowsize=2, arrowwidth = 1, arrowcolor= ADFC_darkgrey, xanchor='left')
    bar_ranking.update_layout(legend_title_text=_('Traffic Type'))
    bar_ranking.update_layout({'plot_bgcolor': ADFC_palegrey,'paper_bgcolor': ADFC_palegrey})
    bar_ranking.update_layout(yaxis_title= _('Absolute count'))
    for annotation in bar_ranking.layout.annotations: annotation['font'] = {'size': 14}

    ### Create comparison Graph
    #TODO: bug dec 2023-2024 on day sort order
    callback_trigger = ctx.triggered_id
    if callback_trigger == 'dropdown_year_A' or callback_trigger == 'dropdown_year_B':
        time_division = 'year'
        selected_value_A = dropdown_year_A
        selected_value_B = dropdown_year_B
        group_by = 'month'
        label = 'Year'
    elif callback_trigger == 'dropdown_year_month_A' or callback_trigger == 'dropdown_year_month_B':
        time_division = 'year_month'
        selected_value_A = dropdown_year_month_A
        selected_value_B = dropdown_year_month_B
        group_by = 'day'
        label = 'Month'
    elif callback_trigger == 'dropdown_year_week_A' or callback_trigger == 'dropdown_year_week_B':
        time_division = 'year_week'
        selected_value_A = dropdown_year_week_A
        selected_value_B = dropdown_year_week_B
        group_by = 'weekday'
        label = 'Week'
    elif callback_trigger == 'dropdown_date_A' or callback_trigger == 'dropdown_date_B':
        time_division = 'date'
        selected_value_A = dropdown_date_A
        selected_value_B = dropdown_date_B
        group_by = 'hour'
        label = 'Date'
    else:
        time_division = radio_time_division
        selected_value_A = dropdown_date_A
        selected_value_B = dropdown_date_B
        group_by = 'hour'
        label = 'Date'

    # Prepare traffic_df_upt by selected street
    traffic_df_upt_str = update_selected_street(traffic_df_upt, segment_id, street_name)
    df_avg_traffic_delta_concat = get_comparison_data(traffic_df_upt_str, time_division, group_by, selected_value_A, selected_value_B)

    line_avg_delta_traffic = px.line(df_avg_traffic_delta_concat,
        x=group_by, y=['ped_total', 'bike_total', 'car_total', 'heavy_total', 'ped_total_d', 'bike_total_d', 'car_total_d', 'heavy_total_d'],
        facet_col='street_selection',
        facet_col_spacing=0.04,
        category_orders={'street_selection': [street_name, 'All Streets']},
        labels={'year': _('Year'), 'month': _('Month'), 'weekday': _('Week day'), 'day': _('Day')},
        color_discrete_map={'ped_total': ADFC_lightblue, 'bike_total': ADFC_green, 'car_total': ADFC_orange, 'heavy_total': ADFC_crimson, 'ped_total_d': ADFC_lightblue, 'bike_total_d': ADFC_green, 'car_total_d': ADFC_orange, 'heavy_total_d': ADFC_crimson},
    )

    # Create average traffic line chart with delta
    line_avg_delta_traffic.update_traces(selector={'name': 'ped_total_d'}, line={'dash': 'dash'})
    line_avg_delta_traffic.update_traces(selector={'name': 'bike_total_d'}, line={'dash': 'dash'})
    line_avg_delta_traffic.update_traces(selector={'name': 'car_total_d'}, line={'dash': 'dash'})
    line_avg_delta_traffic.update_traces(selector={'name': 'heavy_total_d'}, line={'dash': 'dash'})
    line_avg_delta_traffic.for_each_annotation(lambda a: a.update(text=a.text.replace(street_name, street_name + _(' (segment:') + segment_id + ')')))
    line_avg_delta_traffic.for_each_annotation(lambda a: a.update(text=a.text.split("=")[1]))
    line_avg_delta_traffic.for_each_annotation(lambda a: a.update(text=a.text.replace('All Streets', _('All Streets'))))
    line_avg_delta_traffic.update_layout({'plot_bgcolor': ADFC_palegrey,'paper_bgcolor': ADFC_palegrey})
    line_avg_delta_traffic.update_layout(title_text=_('Period') + ' A : ' + _(label) + ' - ' + selected_value_A + ' , ' + _('Period') + ' B (----): ' + _(label) + ' - ' + selected_value_B)
    line_avg_delta_traffic.update_layout(yaxis_title=_('Absolute traffic count'))
    line_avg_delta_traffic.update_layout(legend_title_text=_('Traffic Type'))
    line_avg_delta_traffic.update_traces({'name': _('Pedestrians') + ' A'}, selector={'name': 'ped_total'})
    line_avg_delta_traffic.update_traces({'name': _('Bikes') + ' A'}, selector={'name': 'bike_total'})
    line_avg_delta_traffic.update_traces({'name': _('Cars') + ' A'}, selector={'name': 'car_total'})
    line_avg_delta_traffic.update_traces({'name': _('Heavy') + ' A'}, selector={'name': 'heavy_total'})
    line_avg_delta_traffic.update_traces({'name': _('Pedestrians') + ' B'}, selector={'name': 'ped_total_d'})
    line_avg_delta_traffic.update_traces({'name': _('Bikes') + ' B'}, selector={'name': 'bike_total_d'})
    line_avg_delta_traffic.update_traces({'name': _('Cars') + ' B'}, selector={'name': 'car_total_d'})
    line_avg_delta_traffic.update_traces({'name': _('Heavy') + ' B'}, selector={'name': 'heavy_total_d'})
    line_avg_delta_traffic.update_yaxes(matches=None)
    line_avg_delta_traffic.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))
    line_avg_delta_traffic.update_xaxes(dtick = 1, tickformat=".0f")
    for annotation in line_avg_delta_traffic.layout.annotations: annotation['font'] = {'size': 14}

    return selected_street_header, selected_street_header_color, date_range_text, date_range_color, pie_traffic, line_abs_traffic, bar_avg_traffic, line_avg_delta_traffic, bar_perc_speed, bar_avg_speed, bar_v85, bar_ranking

if __name__ == "__main__":
    app.run(debug=False)
