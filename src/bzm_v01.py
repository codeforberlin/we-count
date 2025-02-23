#!/usr/bin/env python3
# Copyright (c) 2024-2025 Berlin zaehlt Mobilitaet
# SPDX-License-Identifier: MIT

# @file    bzm_v01.py
# @author  Egbert Klaassen
# @date    2025-02-23

# traffic_df        - dataframe with measured traffic data file
# geo_df            - geopandas dataframe, street coordinates for px.line_map
# json_df           - json dataframe based on the same geojson as geo_df, providing features such as street names

import gettext
import os
import dash_bootstrap_components as dbc
import geopandas as gpd
import pandas as pd
import plotly.express as px
from dash import Dash, html, dcc, Output, Input, callback, ctx
from dash.exceptions import PreventUpdate
import datetime
from datetime import timedelta

import bzm_get_data
import common

DEPLOYED = __name__ != '__main__'

#### Retrieve Data ####
def retrieve_data():

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
    traffic_df['hour']=traffic_df['hour'].astype(int)

    # Replace nan values
    # traffic_df['car_total'] = traffic_df['car_total'].fillna(0)
    # traffic_df = traffic_df.fillna(0)

    # Add street column for facet graphs - check efficiency!
    traffic_df['street_selection'] = traffic_df.loc[:, 'osm.name']
    traffic_df.loc[traffic_df['street_selection'] != 'does not exist', 'street_selection'] = _('All')
    """" Can move to bzm_get_data? - End """

    return geo_df, json_df_features, traffic_df

#### Set Language ####

def update_language(language):

    # Initiate translation
    appname = 'bzm'
    localedir = os.path.join(os.path.dirname(__file__), 'locales')
    # Set up Gettext
    translations = gettext.translation(appname, localedir, fallback=True, languages=[language])
    # Install translation function
    translations.install()
    # Translate message (for testing)

def filter_uptime(df):
    # drop uptime rows < 0.7
    nan_rows = df[df['uptime'] < 0.7]
    traffic_df_upt = df.drop(nan_rows.index)
    return traffic_df_upt

def convert(date_time, format_string):
    datetime_obj = datetime.datetime.strptime(date_time, format_string)
    return datetime_obj

def filter_dt(df, start_date, end_date, hour_range):

    # Get min/max dates
    min_date = df['date_local'].min()
    max_date = df['date_local'].max()
    # Set selected dates
    df_dates = df.loc[df['date_local'].between(start_date, end_date)]

    # TODO: add delta line (work in progress)
    # Add delta timeframe
    # delta = -14
    # format_string = '%Y-%m-%d %H:%M:%S'
    # start_date_dt = convert(start_date, format_string)
    # alt_start_date_dt = start_date_dt + timedelta(days=delta)
    # end_date_dt = convert(end_date, format_string)
    # alt_end_date_dt = end_date_dt + timedelta(days=delta)
    # alt_start_date = alt_start_date_dt.strftime(format_string)
    # alt_end_date = alt_end_date_dt.strftime(format_string)
    # df_dates_delta_org_names = df.loc[df['date_local'].between(alt_start_date, alt_end_date)]
    # df_dates_delta = df_dates_delta_org_names.rename(columns={'ped_total': 'ped_total_delta', 'bike_total': 'bike_total_delta', 'car_total': 'car_total_delta', 'heavy_total': 'heavy_total_delta'}, inplace=False)

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
    #df_dates_delta_hours = df_dates_delta.loc[df_dates_delta['hour'].between(hour_range[0], hour_range[1]-1)]

    traffic_df_upt_dt = df_dates_hours

    return traffic_df_upt_dt, min_date, max_date, min_hour, max_hour

def update_selected_street(df, segment_id, street_name):

    # Generate "selected street only" df and populate "street_selection"
    df_str = df[df['segment_id'] == segment_id]
    df_str.loc[df_str['street_selection'] == _('All'), 'street_selection'] = street_name

    # Add selected street to all streets
    traffic_df_upt_dt_str = df._append(df_str, ignore_index=True)
    return traffic_df_upt_dt_str

def get_bike_car_ratios(df):
    traffic_df_id_bc = df.groupby(by=['segment_id'], as_index=False).agg(bike_total=('bike_total', 'sum'), car_total=('car_total', 'sum'))
    traffic_df_id_bc['bike_car_ratio'] = traffic_df_id_bc['bike_total'] / traffic_df_id_bc['car_total']

    bins = [0, 0.1, 0.2, 0.5, 1, 500]
    labels = [_('Over 10x more cars'), _('Over 5x more cars'), _('Over 2x more cars'), _('More cars than bikes'),
              _('More bikes than cars')]
    traffic_df_id_bc['map_line_color'] = pd.cut(traffic_df_id_bc['bike_car_ratio'], bins=bins, labels=labels)

    # Prepare traffic_df_id_bc for join operation
    traffic_df_id_bc['segment_id'] = traffic_df_id_bc['segment_id'].astype(int)
    traffic_df_id_bc.set_index('segment_id', inplace=True)

    return traffic_df_id_bc

def update_map_data(df_map_base, df):
    # Create map info by joining geo_df_map_info with map_line_color from traffic_df_id_bc (based on bike/car ratios)
    df_map = df_map_base.join(df)

    nan_rows = df_map[df_map['osm.name'].isnull()]
    df_map = df_map.drop(nan_rows.index)

    # Add map_line_color category and add column information to cover inactive traffic counters
    df_map['map_line_color'] = df_map['map_line_color'].cat.add_categories([_('Inactive - no data')])
    df_map.fillna({"map_line_color": _("Inactive - no data")}, inplace=True)

    # Sort data to get desired legend order
    df_map = df_map.sort_values(by=['map_line_color'])

    return df_map

# Initialize constants, variables and get data
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

street_name = 'Köpenicker Straße'
segment_id = '9000006435'

zoom_factor = 11

language = 'de'
update_language(language)

geo_df, json_df_features, traffic_df = retrieve_data()

# Set weekday labels depending on language
weekday_map = {0: _('Mon'), 1: _('Tue'), 2: _('Wed'), 3: _('Thu'), 4: _('Fri'), 5: _('Sat'), 6: _('Sun')}
traffic_df['weekday'] = traffic_df['weekday'].map(weekday_map)
month_map = {1: _('Jan'), 2: _('Feb'), 3: _('Mar'), 4: _('Apr'), 5: _('May'), 6: _('Jun'), 7: _('Jul'), 8: _('Aug'), 9: _('Sep'), 10: _('Oct'), 11: _('Nov'), 12: _('Dec')}
traffic_df['month'] = traffic_df['month'].map(month_map)

# Start with traffic df with uptime filtered
traffic_df_upt = filter_uptime(traffic_df)

# traffic_df_upt_dt
start_date = traffic_df_upt['date_local'].min()
end_date = traffic_df_upt['date_local'].max()

format_string = '%Y-%m-%d %H:%M:%S'
start_date_dt = convert(start_date, format_string)
end_date_dt = convert(end_date, format_string)
try_start_date = end_date_dt + datetime.timedelta(days=-13)
if try_start_date > start_date_dt:
    start_date_dt = try_start_date
    start_date = start_date_dt.strftime(format_string)

hour_range = [traffic_df_upt["hour"].min(), traffic_df_upt["hour"].max()]

traffic_df_upt_dt, min_date, max_date, min_hour, max_hour = filter_dt(traffic_df_upt, start_date, end_date, hour_range)

# traffic_df_upt_dt_str
traffic_df_upt_dt_str = update_selected_street(traffic_df_upt_dt, segment_id, street_name)


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

# Prepare geo_df_map_info anf json_df_features and join
geo_df_map_info['segment_id'] = geo_df_map_info['segment_id'].astype(int)
geo_df_map_info.set_index('segment_id', drop= False, inplace=True)
json_df_features['segment_id'] = json_df_features['segment_id'].astype(int)
json_df_features.set_index('segment_id', inplace=True)
# join geo_df_map_info and json_df_features to get map info with name date (extract from geo_df json?)
df_map_base = geo_df_map_info.join(json_df_features)

# Prepare map data
df_map = update_map_data(df_map_base, traffic_df_id_bc)

### Run Dash app ###

if not DEPLOYED:
    print(_('Start dash...'))
dbc_css = "https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-templates/dbc.min.css"
app = Dash(__name__, requests_pathname_prefix="/cgi-bin/bzm.cgi/" if DEPLOYED else None,
           external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.BOOTSTRAP, dbc_css],
           meta_tags=[{'name': 'viewport', 'content': 'width=device-width, initial-scale=1'}]
           )

app.layout = dbc.Container(
    [
        dbc.Row([
            dbc.Col([
                html.H1(_('Berlin Counts Mobility'), style={'margin-left': 40, 'margin-top': 20, 'margin-bottom': 00, 'margin-right': 00}, className='bg-#F2F2F2'),
            ], width=5),
            dbc.Col([
                html.H6('Map info', id='popover_map_info', className= 'text-end', style={'margin-left': 00, 'margin-top': 45, 'margin-bottom': 00, 'margin-right': 30, 'color': ADFC_darkgrey}),
                dbc.Popover(dbc.PopoverBody(_('Note: street colors represent bike/car ratios based on all data available and do not change with date- or hour selection.')), target="popover_map_info", trigger="hover"),
            ], width=3),
            dbc.Col([
                dcc.Dropdown(
                id='language-selector',
                options=[
                    {'label': _('English'), 'value': 'en'},
                    {'label': _('Deutsch'), 'value': 'de'},
                ],
                value=language
                ),
            ], width=4),
        ]),
        dbc.Row([
            # Street map
            dbc.Col([
                dcc.Graph(id='street_map', figure={},className='bg-#F2F2F2'),
            ], width=8),

            # General controls
            dbc.Col([
                # Street drop down
                html.H4(_('Select street:'), style={'margin-top': 50, 'margin-bottom': 20}),
                dcc.Dropdown(id='street_name_dd',
                    options=sorted([{'label': i, 'value': i} for i in traffic_df['osm.name'].unique()], key=lambda x: x['label']),
                    value=street_name,
                    style={'margin-top': 10, 'margin-bottom': 30}
                ),
                html.Hr(),
                html.Span([html.H4(_('Traffic type - selected street'), style={'margin-top': 10, 'margin-bottom': 20, 'display': 'inline-block'}),
                html.I(className='bi bi-info-circle-fill h6', id='popover_traffic_type', style={'margin-left': 5, 'margin-top': 10, 'margin-bottom': 20, 'display': 'inline-block', 'align': 'top', 'color': ADFC_lightgrey})]),
                dbc.Popover(
                    dbc.PopoverBody(_('Traffic type split of the currently selected street, based on currently selected date and hour range.')),
                    target="popover_traffic_type",
                    trigger="hover"
                ),
                # Pie chart
                dcc.Graph(id='pie_traffic', figure={}),
                #html.Hr(),
            ], width=4),
        ]),
        # Date/Time selection and Uptime filter
        dbc.Row([
            html.Hr(),
            dbc.Col([
                html.H6(_('Set hour range:'), style={'margin-left': 40, 'margin-right': 40, 'margin-top': 10, 'margin-bottom': 10}),
                # Hour slice
                dcc.RangeSlider(
                    id='range_slider',
                    min= min_hour,
                    max= max_hour,
                    step=1,
                    value = hour_range,
                    tooltip={'always_visible': True, 'placement' : 'bottom', 'template': "{value}" + _(" Hour")}),
            ], width=6),
            dbc.Col([
                html.H6(_('Pick date range:'), style={'margin-left': 00, 'margin-right': 40, 'margin-top': 10, 'margin-bottom': 10}),
                # Date picker
                dcc.DatePickerRange(
                    id="date_filter",
                    start_date=start_date,
                    end_date=end_date,
                    min_date_allowed=min_date,
                    max_date_allowed=max_date,
                    display_format='DD-MMM-YYYY',
                    end_date_placeholder_text='DD-MMMM-YYYY',
                    minimum_nights=1
                ),
            ], width=3),
            dbc.Col([
                dbc.Checklist(
                    id='toggle_uptime_filter',
                    options=[{'label': _(' Filter uptime > 0.7'), 'value': 'filter_uptime_selected'}],
                    value= ['filter_uptime_selected'],
                    style = {'color' : ADFC_darkgrey, 'font_size' : 14, 'margin-left': 30, 'margin-top': 40, 'margin-bottom': 30}
                ),
            ], width=2),
            dbc.Col([
                html.Span([html.I(className='bi bi-info-circle-fill h6', id='popover_filter', style={'margin-top': 30, 'display': 'inline-block', 'color': ADFC_lightgrey})]),
                dbc.Popover(
                    dbc.PopoverBody(_('A high 0.7-0.8 uptime will always mean very good data. The first and last daylight hour of the day will always have lower uptimes. If uptimes during the day are below 0.5, that is usually a clear sign that something is probably wrong with the instance.')),
                    target="popover_filter", trigger="hover"
                ),
            ], width=1),
        ], style={'margin-left': 40, 'margin-right': 40}, className='rounded'),

        # Absolute traffic
        dbc.Row([
            dbc.Col([
                # Radio time division
                html.H4(_('Absolute traffic'), style={'margin-left': 40, 'margin-right': 40, 'margin-top': 30, 'margin-bottom': 30}),

                # Select a time division
                dcc.RadioItems(
                    id='radio_time_division',
                    options=[
                        {'label': _('Year'), 'value': 'year'},
                        {'label': _('Month'), 'value': 'year_month'},
                        {'label': _('Week'), 'value': 'year_week'},
                        {'label': _('Day'), 'value': 'date'}
                    ],
                    value='date',
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
                html.H4(_('Average traffic'), style={'margin-left': 40, 'margin-right': 40, 'margin-top': 30, 'margin-bottom': 30}),

                dcc.RadioItems(
                    id='radio_time_unit',
                    options=[
                        {'label': _('Yearly'), 'value': 'year'},
                        {'label': _('Monthly'), 'value': 'month'},
                        {'label': _('Weekly'), 'value': 'weekday'},
                        {'label': _('Daily'), 'value': 'day'},
                        {'label': _('Hourly'), 'value': 'hour'}
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
                dcc.Graph(id='line_avg_delta_traffic', figure={}, style={'margin-left': 40, 'margin-right': 40, 'margin-top': 30, 'margin-bottom': 30})
            ], width=12
            ),
        ]),
        dbc.Row([
            dbc.Col([
                html.H4(_('Percentage car speed - by time unit'),style={'margin-left': 40, 'margin-right': 40, 'margin-top': 30, 'margin-bottom': 00}),
                dcc.Graph(id='bar_perc_speed', figure={}, style={'margin-left': 40, 'margin-right': 40, 'margin-top': 30, 'margin-bottom': 30})
            ], width=12
            ),
        ]),
        dbc.Row([
            dbc.Col([
                html.H4(_('Percentage car speed - average'),style={'margin-left': 40, 'margin-right': 40, 'margin-top': 30, 'margin-bottom': 00}),
                dcc.Graph(id='bar_avg_speed', figure={},
                          style={'margin-left': 40, 'margin-right': 40, 'margin-top': 30, 'margin-bottom': 30})
            ], width=12
            ),
        ]),
        dbc.Row([
            dbc.Col([
                html.Span([html.H4(_('v85 car speed'),
                                   style={'margin-left': 40, 'margin-right': 00, 'margin-top': 30, 'margin-bottom': 00, 'display': 'inline-block'}),
                           html.I(className='bi bi-info-circle-fill h6', id='popover_v85_speed',
                                  style={'margin-left': 5, 'margin-top': 30, 'margin-bottom': 00,
                                         'display': 'inline-block', 'color': ADFC_lightgrey})]),
                dbc.Popover(
                    dbc.PopoverBody(
                        _('The V85 is a widely used indicator in the world of mobility and road safety, as it is deemed to be representative of the speed one can reasonably maintain on a road.')),
                    target="popover_v85_speed",
                    trigger="hover"
                ),

                dcc.Graph(id='bar_v85', figure={},
                          style={'margin-left': 40, 'margin-right': 40, 'margin-top': 30, 'margin-bottom': 30})
            ], width=12
            ),
        ]),
        # Explore with x- and y-axis scatter
        dbc.Row([
            dbc.Col([
                html.H4(_('Street ranking by traffic type'), style={'margin-left': 40, 'margin-right': 40, 'margin-top': 30, 'margin-bottom': 30}),
            ], width=12
            ),
        ]),
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
                    style={'margin-left': 40, 'margin-bottom': 00, 'margin-right': 40},
                ),
            ], width=12
            ),
        ]),
        html.Br(),
        dbc.Row([
            dbc.Col([
                dcc.Graph(id='sc_explore', figure={}, style={'margin-left': 40, 'margin-right': 40, 'margin-top': 30, 'margin-bottom': 30})
            ], width=12
            ),
        ]),

        # Date/Time selection and Uptime filter
        dbc.Row([
            html.H4(_('Feedback and contact'),
                    style={'margin-left': 40, 'margin-right': 40, 'margin-top': 10, 'margin-bottom': 30}),
            dbc.Col([
                html.H6([_('More information about the '),
                        html.A(_('Berlin Zählt Mobilität'), href="https://berlin.adfc.de/artikel/berlin-zaehlt-mobilitaet-adfc-berlin-dlr-rufen-zu-citizen-science-projekt-auf", target="_blank"),_(' initiative.'),],
                        style={'margin-left': 40, 'margin-right': 40, 'margin-top': 10, 'margin-bottom': 30}
                       ),
                html.H6([_('Request a counter at the '),
                        html.A(_('Citizen Science-Projekt'), href="https://telraam.net/en/candidates/berlin-zaehlt-mobilitaet/berlin-zaehlt-mobilitaet", target="_blank"),".",],
                        style={'margin-left': 40, 'margin-right': 40, 'margin-top': 10, 'margin-bottom': 30}
                       ),
                html.H6([_('Data protection around the '),
                        html.A(_('Telraam camera'), href="https://telraam.net/home/blog/telraam-privacy", target="_blank"),_(' measurements.'),],
                        style={'margin-left': 40, 'margin-right': 40, 'margin-top': 10, 'margin-bottom': 30}
                       ),
            ], width=6),
            dbc.Col([
                html.H6([_('Contribute to the dashboard development ('),
                        html.A(_('GitHub'), href="https://github.com/codeforberlin/we-count", target="_blank"),").",],
                        style={'margin-left': 40, 'margin-right': 40, 'margin-top': 10, 'margin-bottom': 30}
                       ),
                html.H6([_('For dashboard improvement- or new functionality suggestions,'), html.Br(), _('EMAIL: '),
                        html.A("contact@example.com", href="mailto:contact@example.com"),"."],
                        style={'margin-left': 40, 'margin-right': 40, 'margin-top': 10, 'margin-bottom': 30},
                        ),
            ], width=6),
        ], style={'margin-left': 40, 'margin-right': 40, 'background-color': ADFC_skyblue, 'opacity': 0.7}, className='rounded'),
    html.Br(),
    ],
    fluid = True,
    className = 'dbc'
)

@callback(
    #Output('textarea-example', 'children'),
    Input('language-selector', 'value'),
)

def get_language(lang_code):
    update_language(lang_code)
    return


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

        # Check if street inactive
        idx = df_map.loc[df_map['osm.name'] == street_name]
        map_color_status = idx['map_line_color'].values[0]
        if map_color_status == _('Inactive - no data'):
            raise PreventUpdate
        else:
            return street_name

@callback(
    Output(component_id='street_map', component_property='figure'),
    Input(component_id='street_name_dd', component_property='value'),
    Input(component_id="date_filter", component_property="end_date"),
    Input(component_id='range_slider', component_property='value'),
)

def update_map(street_name, date_filter, range_slider):

    callback_trigger = ctx.triggered_id
    if (callback_trigger == 'street_name_dd'):
        zoom_factor = 13
    elif (callback_trigger == 'range_slider') or (callback_trigger == 'date_filter'):
        zoom_factor = 11
    else:
        zoom_factor = 11

    df_map = update_map_data(df_map_base, traffic_df_id_bc)
    idx = df_map.loc[df_map['osm.name'] == street_name]
    lon_str = idx['x'].values[0]
    lat_str = idx['y'].values[0]

    sep = '&nbsp;|&nbsp;'

    street_map = px.line_map(df_map, lat='y', lon='x', line_group='segment_id', hover_name = 'osm.name', color= 'map_line_color', color_discrete_map= {
        _('More bikes than cars'): ADFC_green,
        _('More cars than bikes'): ADFC_blue,
        _('Over 2x more cars'): ADFC_orange,
        _('Over 5x more cars'): ADFC_crimson,
        _('Over 10x more cars'): ADFC_pink,
        _('Inactive - no data'): ADFC_lightgrey},
        map_style="streets", center= dict(lat=lat_str, lon=lon_str), height=600, zoom= zoom_factor)

    street_map.update_traces(line_width=5, opacity=1.0)
    street_map.update_layout(margin=dict(l=40, r=20, t=40, b=30))
    street_map.update_layout(legend_title=_('Street color'))
    street_map.update_layout(legend=dict(yanchor="top", y=0.99, xanchor="right", x=0.99))
    street_map.update_layout(annotations=[
        dict(
            text=(
                sep.join([
                    '<a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>',
                    '<a href="https://telraam.net">Telraam</a>',
                    '<a href="https://www.berlin.de/sen/uvk/mobilitaet-und-verkehr/verkehrsplanung/radverkehr/weitere-radinfrastruktur/zaehlstellen-und-fahrradbarometer/">SenUMVK Berlin<br></a>'
                    #'<a href="https://berlin-zaehlt.de">Berlin zählt Mobilität<br></a>'
                ]) + '' +
                sep.join([
                    '<a href="https://berlin-zaehlt.de/csv/">CSV data</a> under <a href="https://creativecommons.org/licenses/by/4.0/">CC-BY 4.0</a> and <a href="https://www.govdata.de/dl-de/by-2-0">dl-de/by-2-0</a>'
                ])
            ),
            showarrow=False, align='left', xref='paper', yref='paper', x=0, y=0
        )
    ])

    return street_map

### General traffic callback ###
@callback(
    Output(component_id='pie_traffic', component_property='figure'),
    Output(component_id='line_abs_traffic', component_property='figure'),
    Output(component_id='bar_avg_traffic', component_property='figure'),
    Output(component_id='line_avg_delta_traffic', component_property='figure'),
    Output(component_id='bar_perc_speed', component_property='figure'),
    Output(component_id='bar_avg_speed', component_property='figure'),
    Output(component_id='bar_v85', component_property='figure'),
    Output(component_id='sc_explore', component_property='figure'),
    Input(component_id='radio_time_division', component_property='value'),
    Input(component_id='radio_time_unit', component_property='value'),
    Input(component_id='street_name_dd', component_property='value'),
    Input(component_id="date_filter", component_property="start_date"),
    Input(component_id="date_filter", component_property="end_date"),
    Input(component_id='range_slider', component_property='value'),
    Input(component_id='toggle_uptime_filter', component_property='value'),
    Input(component_id='radio_y_axis', component_property='value'),
)

def update_graphs(radio_time_division, radio_time_unit, street_name, start_date, end_date, hour_range, toggle_uptime_filter, radio_y_axis):

    # If uptime filter changed, reload traffic_df_upt
    if 'filter_uptime_selected' in toggle_uptime_filter:
        traffic_df_upt = filter_uptime(traffic_df)
    else:
        traffic_df_upt = traffic_df

    # Filter on selected dates and hours
    # TODO: trigger only in case of relevant callback
    traffic_df_upt_dt, min_date, max_date, min_hour, max_hour = filter_dt(traffic_df_upt, start_date, end_date, hour_range)

    # Get segment_id
    # TODO: trigger only in case of relevant callback
    segment_id_index = traffic_df_upt.loc[traffic_df_upt['osm.name'] == street_name]
    segment_id = segment_id_index['segment_id'].values[0]

    # Update selected street
    traffic_df_upt_dt_str = update_selected_street(traffic_df_upt_dt, segment_id, street_name)

    # Create abs line chart
    df_line_abs_traffic = traffic_df_upt_dt_str.groupby(by=[radio_time_division, 'street_selection'], sort = False, as_index=False).agg({'ped_total': 'sum', 'bike_total': 'sum', 'car_total': 'sum', 'heavy_total': 'sum'})

    # Set readable date format for day-view
    if radio_time_division == _('date'):
        df_line_abs_traffic['date'] = pd.to_datetime(df_line_abs_traffic.date).dt.strftime('%a %d %b %y')

    line_abs_traffic = px.line(df_line_abs_traffic,
        x=radio_time_division, y=['ped_total', 'bike_total', 'car_total', 'heavy_total'],
        markers=True,
        facet_col='street_selection',
        category_orders={'street_selection': [street_name, _('All')]},
        labels={'year': _('Year'), 'year_month': _('Month'), 'year_week': _('Week'), 'date': _('Day')},
        color_discrete_map={'ped_total': ADFC_lightblue, 'bike_total': ADFC_green, 'car_total': ADFC_orange, 'heavy_total': ADFC_crimson},
        facet_col_spacing=0.04,
        title=_('Absolute traffic count')
    )

    line_abs_traffic.update_layout({'plot_bgcolor': ADFC_palegrey,'paper_bgcolor': ADFC_palegrey})
    line_abs_traffic.update_layout({'plot_bgcolor': ADFC_palegrey, 'paper_bgcolor': ADFC_palegrey})
    line_abs_traffic.update_layout(legend_title_text=_('Traffic Type'))
    line_abs_traffic.update_layout(yaxis_title= _('Absolute traffic count'))
    line_abs_traffic.update_yaxes(matches=None)
    line_abs_traffic.update_xaxes(matches= None, showticklabels=True)
    line_abs_traffic.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))
    line_abs_traffic.for_each_annotation(lambda a: a.update(text=a.text.split("=")[1]))
    for annotation in line_abs_traffic.layout.annotations: annotation['font'] = {'size': 14}
    line_abs_traffic.update_traces({'name': _('Pedestrians')}, selector={'name': 'ped_total'})
    line_abs_traffic.update_traces({'name': _('Bikes')}, selector={'name': 'bike_total'})
    line_abs_traffic.update_traces({'name': _('Cars')}, selector={'name': 'car_total'})
    line_abs_traffic.update_traces({'name': _('Heavy')}, selector={'name': 'heavy_total'})
    line_abs_traffic.for_each_annotation(lambda a: a.update(text=a.text.replace(street_name, street_name + ' (segment no:' + segment_id + ')')))

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

    # Create average traffic bar chart
    df_avg_traffic = traffic_df_upt_dt_str.groupby(by=[radio_time_unit, 'street_selection'], sort= False, as_index=False).agg({'ped_total': 'mean', 'bike_total': 'mean', 'car_total': 'mean', 'heavy_total': 'mean'})
    # Sort on hour to avoid line graph jumps around hour gaps
    if radio_time_unit == 'hour' or radio_time_unit == 'day':
        df_avg_traffic = df_avg_traffic.sort_values(by=[radio_time_unit], ascending=True)

    bar_avg_traffic = px.bar(df_avg_traffic,
        x=radio_time_unit, y=['ped_total', 'bike_total', 'car_total', 'heavy_total'],
        barmode='stack',
        facet_col='street_selection',
        facet_col_spacing=0.04,
        category_orders={'street_selection': [street_name, _('All')]},
        labels={'year': _('Yearly'), 'year_month': _('Monthly'), 'weekday': _('Weekly'), 'day': _('Daily'), 'hour': _('Hourly'), '1': 'Mon'},
        color_discrete_map={'ped_total': ADFC_lightblue, 'bike_total': ADFC_green, 'car_total': ADFC_orange, 'heavy_total': ADFC_crimson},
        title=(_('Average traffic count')  + ' (' + start_date.split(' ')[0] + ' - ' + end_date.split(' ')[0] + ', ' + str(hour_range[0]) + ' - ' + str(hour_range[1]) + ' h)')
    )

    bar_avg_traffic.for_each_annotation(lambda a: a.update(text=a.text.replace(street_name, street_name + _(' (segment no:') + segment_id + ')')))
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

    #bzm_get_data.save_df(df_avg_traffic,'df_avg_traffic.xlsx')

    line_avg_delta_traffic = px.line(df_avg_traffic,
        x=radio_time_unit, y=['ped_total', 'bike_total', 'car_total', 'heavy_total'],
        facet_col='street_selection',
        facet_col_spacing=0.04,
        category_orders={'street_selection': [street_name, _('All')]},
        labels={'year': _('Yearly'), 'year_month': _('Monthly'), 'weekday': _('Weekly'), 'day': _('Daily'), 'hour': _('Hourly'), '1': 'Mon'},
        color_discrete_map={'ped_total': ADFC_lightblue, 'bike_total': ADFC_green, 'car_total': ADFC_orange, 'heavy_total': ADFC_crimson},
        title=(_('Average traffic count')  + ' (' + start_date.split(' ')[0] + ' - ' + end_date.split(' ')[0] + ', ' + str(hour_range[0]) + ' - ' + str(hour_range[1]) + ' h)')
    )

    # Create average traffic line chart with delta
    line_avg_delta_traffic.for_each_annotation(lambda a: a.update(text=a.text.replace(street_name, street_name + _(' (segment no:') + segment_id + ')')))
    line_avg_delta_traffic.for_each_annotation(lambda a: a.update(text=a.text.split("=")[1]))
    line_avg_delta_traffic.update_layout({'plot_bgcolor': ADFC_palegrey,'paper_bgcolor': ADFC_palegrey})
    line_avg_delta_traffic.update_layout(yaxis_title=_('Average traffic count'))
    line_avg_delta_traffic.update_layout(legend_title_text=_('Traffic Type'))
    line_avg_delta_traffic.update_traces({'name': _('Pedestrians')}, selector={'name': 'ped_total'})
    line_avg_delta_traffic.update_traces({'name': _('Bikes')}, selector={'name': 'bike_total'})
    line_avg_delta_traffic.update_traces({'name': _('Cars')}, selector={'name': 'car_total'})
    line_avg_delta_traffic.update_traces({'name': _('Heavy')}, selector={'name': 'heavy_total'})
    line_avg_delta_traffic.update_xaxes(dtick = 1, tickformat=".0f")
    line_avg_delta_traffic.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))
    for annotation in line_avg_delta_traffic.layout.annotations: annotation['font'] = {'size': 14}


    # Create percentage speed bar chart

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
         category_orders={'street_selection': [street_name, _('All')]},
         labels={'year': _('Yearly'), 'year_month': _('Monthly'), 'weekday': _('Weekly'), 'day': _('Daily'), 'hour': _('Hourly')},
         color_discrete_map={'car_speed0': ADFC_lightblue, 'car_speed10': ADFC_lightblue,'car_speed20': ADFC_lightblue, 'car_speed30': ADFC_green, 'car_speed40': ADFC_green, 'car_speed50': ADFC_orange, 'car_speed60': ADFC_crimson, 'car_speed70': ADFC_pink},
         facet_col_spacing=0.04,
         title=(_('Percentage car speed') + ' (' + start_date.split(' ')[0] + ' - ' + end_date.split(' ')[0] + ', ' + str(hour_range[0]) + ' - ' + str(hour_range[1]) + ' h)')
    )

    bar_perc_speed.for_each_annotation(lambda a: a.update(text=a.text.split("=")[1]))
    bar_perc_speed.for_each_annotation(lambda a: a.update(text=a.text.replace(street_name, street_name + _(' (segment no:') + segment_id + ')')))
    bar_perc_speed.update_layout(legend_title_text=_('Car speed'))
    bar_perc_speed.update_layout({'plot_bgcolor': ADFC_palegrey, 'paper_bgcolor': ADFC_palegrey})
    bar_perc_speed.update_layout(yaxis_title=_('Percentage car speed'))
    bar_perc_speed.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))
    for annotation in bar_perc_speed.layout.annotations:
        annotation['font'] = {'size': 14}

    # Create percentage speed average bar chart
    df_bar_avg_speed_traffic = df_bar_speed.groupby(by=[radio_time_unit, 'street_selection'], sort= False, as_index=False).agg({'car_speed0': 'mean', 'car_speed10': 'mean', 'car_speed20': 'mean', 'car_speed30': 'mean', 'car_speed40': 'mean', 'car_speed50': 'mean', 'car_speed60': 'mean', 'car_speed70': 'mean'})

    bar_avg_speed = px.bar(df_bar_avg_speed_traffic,
        x=radio_time_unit, y=cols,
        barmode='group',
        facet_col='street_selection',
        category_orders={'street_selection': [street_name, _('All')]},
        labels={'year': _('Yearly'), 'year_month': _('Monthly'), 'weekday': _('Weekly'), 'day': _('Daily'), 'hour': _('Hourly')},
        color_discrete_map={'car_speed0': ADFC_lightblue, 'car_speed10': ADFC_lightblue,
                            'car_speed20': ADFC_lightblue, 'car_speed30': ADFC_green,
                            'car_speed40': ADFC_green, 'car_speed50': ADFC_orange,
                            'car_speed60': ADFC_crimson, 'car_speed70': ADFC_pink},
        facet_col_spacing=0.04,
        title=(_('Average percentage car speed') + ' (' + start_date.split(' ')[0] + ' - ' + end_date.split(' ')[0] + ', ' + str(hour_range[0]) + ' - ' + str(hour_range[1]) + ' h)')
    )

    bar_avg_speed.for_each_annotation(lambda a: a.update(text=a.text.split("=")[1]))
    bar_avg_speed.for_each_annotation(lambda a: a.update(text=a.text.replace(street_name, street_name + _(' (segment no:') + segment_id + ')')))
    bar_avg_speed.update_layout(legend_title_text=_('Car speed'))
    bar_avg_speed.update_layout({'plot_bgcolor': ADFC_palegrey, 'paper_bgcolor': ADFC_palegrey})
    bar_avg_speed.update_layout(yaxis_title=_('Percentage car speed'))
    bar_avg_speed.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))
    for annotation in bar_avg_speed.layout.annotations: annotation['font'] = {'size': 14}

    # Create v85 graph
    df_bar_v85 = traffic_df_upt_dt_str.groupby(by=[radio_time_unit, 'street_selection'], sort= False, as_index=False).agg({'v85': 'mean'})

    # Create v85 bar chart
    bar_v85 = px.bar(df_bar_v85,
        x=radio_time_unit, y='v85',
        color='v85',
        color_continuous_scale='temps',
        facet_col='street_selection',
        category_orders={'street_selection': [street_name, _('All')]},
        facet_col_spacing=0.04,
        labels={'year': _('Yearly'), 'year_month': _('Monthly'), 'weekday': _('Weekly'), 'day': _('Daily'), 'hour': _('Hourly')},
        title=(_('Speed cars v85') + ' (' + start_date.split(' ')[0] + ' - ' + end_date.split(' ')[0] + ', ' + str(hour_range[0]) + ' - ' + str(hour_range[1]) + ' h)')
    )

    bar_v85.for_each_annotation(lambda a: a.update(text=a.text.split("=")[1]))
    bar_v85.for_each_annotation(lambda a: a.update(text=a.text.replace(street_name, street_name + _(' (segment no:') + segment_id + ')')))
    bar_v85.update_layout(legend_title_text=_('Traffic Type'))
    bar_v85.update_layout({'plot_bgcolor': ADFC_palegrey,'paper_bgcolor': ADFC_palegrey})
    bar_v85.update_layout(yaxis_title= _('v85 in km/h'))
    bar_v85.update_xaxes(dtick=1, tickformat=".0f")
    bar_v85.update_yaxes(dtick=5, tickformat=".0f")
    bar_v85.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))
    for annotation in bar_v85.layout.annotations:
        annotation['font'] = {'size': 14}

    # Create explorer chart
    df_sc_explore = traffic_df_upt_dt
    df_sc_explore = df_sc_explore.groupby(by=['osm.name', 'street_selection'], sort= False, as_index=False).agg({'ped_total': 'sum', 'bike_total': 'sum', 'car_total': 'sum', 'heavy_total': 'sum'})
    df_sc_explore = df_sc_explore.sort_values(by=[radio_y_axis], ascending=False)
    df_sc_explore.reset_index(inplace=True)

    # Assess x and y for annotation
    annotation_index= df_sc_explore[df_sc_explore['osm.name'] == street_name].index.item()
    annotation_x = annotation_index
    annotation_y = df_sc_explore[radio_y_axis].values[annotation_index]

    sc_explore = px.bar(df_sc_explore,
        x='osm.name', y=radio_y_axis,
        color=radio_y_axis,
        color_continuous_scale='temps',
        labels={'ped_total': _('Pedestrians'), 'bike_total': _('Bikes'), 'car_total': _('Cars'), 'heavy_total': _('Heavy'), 'osm.length': _('Street Length'), 'osm.maxspeed': _('Max Speed'), 'osm.name': _('Street')},
        title=(_('Absolute traffic') + ' (' + start_date.split(' ')[0] + ' - ' + end_date.split(' ')[0] + ', ' + str(hour_range[0]) + ' - ' + str(hour_range[1]) + ' h)'),
        height=600,
    )

    sc_explore.for_each_annotation(lambda a: a.update(text=a.text.split("=")[1]))
    sc_explore.add_annotation(x=annotation_x, y=annotation_y, text= street_name + _('<br>(segment no:') + segment_id + ')', showarrow=True)
    sc_explore.update_annotations(ax=0, ay=-40, arrowhead=2, arrowsize=2, arrowwidth = 1, arrowcolor= ADFC_darkgrey, xanchor='left')
    sc_explore.update_layout(legend_title_text=_('Traffic Type'))
    sc_explore.update_layout({'plot_bgcolor': ADFC_palegrey,'paper_bgcolor': ADFC_palegrey})
    sc_explore.update_layout(yaxis_title= 'Total count')
    for annotation in sc_explore.layout.annotations: annotation['font'] = {'size': 14}

    return pie_traffic, line_abs_traffic, bar_avg_traffic, line_avg_delta_traffic, bar_perc_speed, bar_avg_speed, bar_v85, sc_explore

if __name__ == "__main__":
    app.run_server(debug=False)
