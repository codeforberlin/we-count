#!/usr/bin/env python3
# Copyright (c) 2024-2025 Berlin zählt Mobilität
# SPDX-License-Identifier: MIT

# @file    app.py
# @author  Egbert Klaassen
# @author  Michael Behrisch
# @date    2026-01-26

""""
# traffic_df        - dataframe with measured traffic data file
# geo_df            - geopandas dataframe, street coordinates for px.line_map
# json_df           - json dataframe based on the same geojson as geo_df, providing features such as street names
"""

import os
import gettext
from datetime import datetime, timedelta
import pandas as pd
import geopandas as gpd
import duckdb
from dash import Dash, Output, Input, callback, ctx
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate
import plotly.express as px
from threading import Lock
from dateutil import parser

# the following is basically to suppress warnings about "_" being undefined
# "from gettext import gettext as _" does not work because we use gettext.install later on, which installs "_"
from typing import Callable
_: Callable[[str], str]

from .layout import serve_layout, INITIAL_STREET_ID, INITIAL_LANGUAGE
from .layout import ADFC_blue, ADFC_crimson, ADFC_darkgrey, ADFC_green, ADFC_green_L
from .layout import ADFC_lightblue, ADFC_lightblue_D, ADFC_lightgrey, ADFC_orange, ADFC_palegrey, ADFC_pink

DEPLOYED = __name__ != '__main__'
ASSET_DIR = os.path.join(os.path.dirname(__file__), 'assets')
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'data')

db_lock = Lock()

def output_excel(df, file_name):
    path = os.path.join(ASSET_DIR, file_name + '.xlsx')
    df.to_excel(path, index=False)

def output_csv(df, file_name):
    path = os.path.join(ASSET_DIR, file_name + '.csv')
    df.to_csv(path, index=False)

def duckdb_info(con):
    query = """
    SELECT table_name, column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'main'
    ORDER BY table_name, ordinal_position;
    """

    # Fetch results
    tables_and_columns = con.execute(query).fetchall()

    # Print results in a readable format
    current_table = None
    for table, column, dtype in tables_and_columns:
        if table != current_table:
            print(f"\nTable: {table}")
            current_table = table
        print(f"  - {column} ({dtype})")

    mem_usage = con.execute("SELECT * FROM duckdb_memory()").fetchdf()
    print(mem_usage)

def retrieve_data():
    # Read geojson data file to access geometry coordinates
    if not DEPLOYED:
        print('Reading geojson data...')

    data_dir = DATA_DIR
    if not os.path.exists(os.path.join(data_dir, 'bzm_telraam_segments.geojson')):
        data_dir = ASSET_DIR
    geojson_path = os.path.join(data_dir, 'bzm_telraam_segments.geojson')
    geo_cols = ['segment_id', 'osm', 'cameras', 'geometry']
    geo_df = gpd.read_file(geojson_path, columns=geo_cols)

    if not DEPLOYED:
        print('Reading json data...')

    geo_file_path = os.path.join(data_dir, 'df_geojson.parquet')
    json_df_features = pd.read_parquet(geo_file_path)

    # Read traffic data from file
    if not DEPLOYED:
        print('Reading traffic data...')

    # Initialize Duckdb
    db_file = 'traffic.db'
    if os.path.exists(os.path.join(data_dir, db_file)):
        if not DEPLOYED:
            print('Replace existing database file')
        os.remove(os.path.join(data_dir, db_file))

    conn = duckdb.connect(database=os.path.join(data_dir, db_file))
    #conn = duckdb.connect(':memory:')
    # conn.execute('SET threads = 4;')  # limit the number of parallel threads

    traffic_relation = conn.read_parquet(os.path.join(data_dir, 'traffic_df_*.parquet'), union_by_name=True)
    traffic_relation.to_table('all_traffic')

    with db_lock:
        # Alter dtypes for data processing and to enable sort order
        conn.execute('ALTER TABLE all_traffic ALTER COLUMN day SET DATA TYPE INTEGER')

        # TODO: remove from parquet files
        conn.execute('ALTER TABLE all_traffic DROP COLUMN last_data_package')

    # Prepare bike/care ratios
    query = f"""
    SELECT 
        segment_id,
        SUM(bike_total) AS bike_total,
        SUM(car_total) AS car_total,
        CASE 
            WHEN SUM(car_total) = 0 THEN NULL  -- Avoid division by zero
            ELSE CAST(SUM(bike_total) AS DOUBLE) / SUM(car_total)
        END AS bike_car_ratio
    FROM all_traffic
    GROUP BY segment_id
    """

    with db_lock:
        traffic_df_id_bc = conn.execute(query).fetch_df()

    # Add last_data_package from json_df_features to all_traffic
    last_data_package_df = pd.DataFrame(json_df_features[['segment_id', 'last_data_package']])
    last_data_package_df['last_data_package'] = pd.to_datetime(last_data_package_df['last_data_package'], format='mixed')

    with db_lock:
        conn.register('last_data_package_table', last_data_package_df)

    query = """
    CREATE OR REPLACE TABLE all_traffic AS
    SELECT a.*,
        j.last_data_package AT TIME ZONE 'UTC' AS last_data_package_naive
    FROM all_traffic AS a
    LEFT JOIN last_data_package_table AS j ON a.segment_id = j.segment_id
    """

    with db_lock:
        conn.execute(query)
        conn.unregister('last_data_package_table')

    # Free memory
    del last_data_package_df

    return geo_df, json_df_features, traffic_df_id_bc, conn

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

def convert(date_time, format_string):
    datetime_obj = datetime.strptime(date_time, format_string)
    return datetime_obj

def format_str_date(str_date, from_date_format, to_date_format):
    timestamp_date = datetime.strptime(str_date, from_date_format)
    formatted_str_date = timestamp_date.strftime(to_date_format)
    return formatted_str_date

def add_selected_street(from_table_name, id_street, street_name):

    # Add or update table with selected street
    query = (f'CREATE OR REPLACE TEMP TABLE selected_street AS '
             f'SELECT * FROM {from_table_name} '
             f'WHERE id_street = ?')
    params = [id_street]
    conn.execute(query, params)

    # Replace "All streets" with selected street name
    query = ('UPDATE selected_street '
             'SET street_selection = ?')
    params = [street_name]
    conn.execute(query, params)

    # Add selected street to filtered_traffic_dt
    to_table_name = from_table_name + '_str'
    query = (f'CREATE OR REPLACE TEMP TABLE {to_table_name} AS '
             f'SELECT * FROM {from_table_name} '
             f'UNION ALL '
             f'SELECT * FROM selected_street')
    conn.execute(query)

    # Delete (drop) the street_selection table
    conn.execute('DROP TABLE IF EXISTS selected_street')

    return

def get_bike_car_ratios(traffic_df_id_bc):

    bins = [0, 0.1, 0.2, 0.5, 1, 500]
    speed_labels = ['Over 10x more cars', 'Over 5x more cars', 'Over 2x more cars', 'More cars than bikes', 'More bikes than cars']
    traffic_df_id_bc['map_line_color'] = pd.cut(traffic_df_id_bc['bike_car_ratio'], bins=bins, labels=speed_labels)

    # Prepare traffic_df_id_bc for join operation
    traffic_df_id_bc.set_index('segment_id', inplace=True)

    return traffic_df_id_bc

def update_map_data(df_map_base, df, active_selected, hardware_version):

    # Prepare map info by joining geo_df_map_info with map_line_color from traffic_df_id_bc (based on bike/car ratios)
    df_map = df_map_base.join(df)
    # Remove rows w/o segment_id
    nan_rows = df_map[df_map['segment_id'].isnull()]
    df_map = df_map.drop(nan_rows.index)
    # TODO: some streets in "bzm_telraam_segments.geojson" have no camera info and so appear as hardware version "0", the below puts these to "1"
    df_map['hardware_version'] = df_map['hardware_version'].replace(0,1)

    # TODO: Filter uptime although at the moment it looks like there are no streets with < 0.7 uptime only
    # Filter on active cameras (data available later than two weeks ago)
    if active_selected == ['filter_active_selected']:
        # get segment_id's with data >= two weeks ago
        df_map_active = df_map[df_map['last_data_package'] >= two_weeks_ago]
        active_segment_ids = df_map_active['segment_id'].unique()
        df_map = df_map[df_map['segment_id'].isin(active_segment_ids)]

    # Filter on camera hardware version
    if hardware_version == [1]:
        df_map = df_map[df_map['hardware_version'] == 1]
    elif hardware_version == [2]:
        df_map = df_map[df_map['hardware_version'] == 2]

    # Add map_line_color category and add column information to cover inactive cameras
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

def get_min_max_str(start_date, end_date, id_street, table):
    missing_data = False
    message = 'none'

    query = f"""
    SELECT min(date_local)
    FROM {table}
    WHERE id_street = ?
    """
    params = [id_street]

    with db_lock:
        min_date = conn.execute(query, params).fetchone()
    min_date = min_date[0]
    min_date = min_date.strftime('%Y-%m-%dT%H:%M:%S')

    query = f"""
    SELECT max(date_local)
    FROM {table}
    WHERE id_street = ?
    """
    params = [id_street]

    with db_lock:
        max_date = conn.execute(query, params).fetchone()
    max_date = max_date[0]
    max_date = max_date.strftime('%Y-%m-%dT%H:%M:%S')

    if start_date > max_date or end_date < min_date:
        missing_data = True
        message = _('Dates out of range')
        start_date = min_date
        end_date = max_date
    elif min_date <= start_date <= max_date and end_date > max_date:
        missing_data = True
        message = _('End date out of range')
        end_date = max_date
    elif min_date <= end_date <= max_date and start_date < min_date:
        missing_data = True
        message = _('Start date out of range')
        start_date = min_date
    elif start_date < min_date or end_date > max_date:
        missing_data = True
        message = _('Narrowed down range')
        start_date = min_date
        end_date = max_date

    return min_date, max_date, start_date, end_date, message, missing_data

def get_min_max_dates(id_street: str):

    query = ('SELECT min(date_local) '
             'FROM all_traffic '
             'WHERE id_street = ?')

    params = [id_street]

    with db_lock:
        min_date = conn.execute(query, params).fetchone()
    min_date = min_date[0]
    min_date = min_date.strftime('%Y-%m-%dT%H:%M:%S')

    query = ('SELECT max(date_local) '
             'FROM all_traffic '
             'WHERE id_street = ?')
    params = [id_street]

    with db_lock:
        max_date = conn.execute(query, params).fetchone()

    max_date = max_date[0]
    max_date = max_date.strftime('%Y-%m-%dT%H:%M:%S')

    return min_date, max_date

# this assumes an initial street id of the form "name (segment_id)"
street_name, segment_id = INITIAL_STREET_ID[:-1].split(" (")

zoom_factor = 11

geo_df, json_df_features, traffic_df_id_bc, conn = retrieve_data()

update_language(INITIAL_LANGUAGE)

# Michael for def segment_id_from_url?
# EK: commented out as not used (yet)
# street_names = {id: name for id, name in zip(traffic_df['segment_id'], traffic_df['id_street'])}

# Get min max dates from complete data set
query = """
SELECT 
    MIN(STRPTIME(date, '%d-%m-%Y')) AS start_date,
    MAX(STRPTIME(date, '%d-%m-%Y')) AS end_date
FROM all_traffic
"""

with db_lock:
    min_max = conn.execute(query).fetchdf()

# Define date filter min/max
start_date = min_max.loc[0, 'start_date']   # Access by label + row index
end_date = min_max.loc[0, 'end_date']

#Free memory
del min_max

#TODO: capture if date not available
try_start_date = end_date - timedelta(days=14)
if try_start_date > start_date:
    start_date = try_start_date

# Put dates to required dropdown format
to_date_format = '%Y-%m-%d'
start_date = datetime.strftime(start_date, to_date_format)
end_date = datetime.strftime(end_date, to_date_format)

# Get min/max selectable dates, based on selected street
# TODO: ensure initial street has data in the last two weeks
min_date, max_date, start_date, end_date, message, missing_data = get_min_max_str(start_date, end_date, INITIAL_STREET_ID, 'all_traffic')

# Get active filter date (two weeks ago from the last date in the dataset)
from_date_format = '%Y-%m-%d'
end_date_dt = convert(end_date, from_date_format)
two_weeks_ago_dt = end_date_dt - timedelta(weeks=2)
two_weeks_ago = two_weeks_ago_dt.strftime('%Y-%m-%d')

# Prepare street options for dropdown menu
query = f"""
SELECT DISTINCT id_street, last_data_package_naive
FROM all_traffic
WHERE uptime > 0.7
AND CAST(last_data_package_naive AS DATE) >= ?
ORDER BY id_street
"""
params = [two_weeks_ago]

with db_lock:
    id_street_options_df = conn.execute(query, params).fetch_df()
    # Convert df to list
    id_street_options = id_street_options_df['id_street'].tolist()

### Prepare map data ###
if not DEPLOYED:
    print('Prepare map...')

# Add column with bike/car ratio for street map representation (skip rows where car_total is 0, set to 500 i.e. most favorable bike/car ratio)
if not DEPLOYED:
    print('Add bike/car ratio column...')

# Extract x y coordinates from geo_df (geopandas file)
geo_df_coords = geo_df.get_coordinates()
# Get ids to join with x y coordinates
geo_df_ids = geo_df[['segment_id']]
# Join x y and segment_id into e new dataframe
geo_df_map_info = geo_df_coords.join(geo_df_ids)

# Free memory
del geo_df_coords, geo_df_ids

# Prepare geo_df_map_info and json_df_features and join
geo_df_map_info['segment_id'] = geo_df_map_info['segment_id'].astype(int)
geo_df_map_info.set_index('segment_id', drop= False, inplace=True)
json_df_features['segment_id'] = json_df_features['segment_id'].astype(int)
json_df_features.set_index('segment_id', inplace=True)
#TODO: move json_df_features to geopandas
df_map_base = geo_df_map_info.join(json_df_features)

# Free memory
del json_df_features

# Get consolidated bike/car ratios by segment_id
traffic_df_id_bc = get_bike_car_ratios(traffic_df_id_bc)

# Join map data and bike/car ratio data to df_map
df_map = update_map_data(df_map_base, traffic_df_id_bc, 'toggle_active_filter', [1,2])

### Run Dash app ###
if not DEPLOYED:
    print('Starting dash ...')

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.BOOTSTRAP],
           meta_tags=[{'name': 'viewport', 'content': 'width=device-width, initial-scale=1'}]
           )

app.title = "Berlin-zaehlt"
app.layout = lambda: serve_layout(app, id_street_options, start_date, end_date, min_date, max_date)


@app.callback(
    Output('url', 'href'),
    Input('language_selector', 'value'),
    prevent_initial_call=True
)
def get_language(lang_code_dd):
    update_language(lang_code_dd)
    return '/'

# TODO: Store file for multiple use
# Storing traffic_df on client side does not work because this requires 600+ MB memory...
# @app.callback(
#     Output('store_traffic_data', 'data'),
#     Input('hardware_version', 'value')
# )
#
# def store_traffic_data(value):
#     print('init_id_street')
#     traffic_df_use = filter_traffic_data(traffic_df, 'filter_uptime_selected', 'filter_active_selected', [1, 2], init_id_street, 2)
#     print(traffic_df_use.info())
#     dataset = traffic_df_use
#     return dataset.to_dict('records')

# @callback(
#     Output(component_id='street_name_dd', component_property='value'),
#     Output(component_id='date_filter', component_property='start_date'),
#     Output(component_id='date_filter', component_property='end_date'),
#     Input(component_id='url', component_property='search'),
# )
#
# def segment_id_from_url(url):
#     query = parse_qs(urlparse(url).query)
#     segment = query.get('segment_id', [segment_id])[0]
#     start = query.get('start', [start_date])[0]
#     end = query.get('end', [end_date])[0]
#     return street_names.get(segment, init_id_street), start, end

### Update Map ###
@callback(
    Output(component_id='street_map', component_property='figure'),
    Output(component_id='hardware_version', component_property='value'),
    Output(component_id='street_name_dd', component_property='options'),
    Output(component_id='street_name_dd', component_property='value'),
    Output(component_id='nof_selected_segments', component_property='children'),
    Input(component_id='street_map', component_property='clickData'),
    Input(component_id='street_name_dd', component_property='value'),
    Input(component_id='hardware_version',component_property= 'value'),
    Input(component_id='toggle_active_filter',component_property= 'value'),
)

def update_map(clickData, id_street, hardware_version, toggle_active_filter):

    callback_trigger = ctx.triggered_id

    # Get hardware version of currently selected street
    current_hw = int(df_map_base.loc[df_map_base['id_street'] == id_street, 'hardware_version'].iloc[0])

    # Update df-map data in case of uptime change, active filter change or hardware change
    if callback_trigger == 'toggle_active_filter' or 'hardware_version':
        df_map = update_map_data(df_map_base, traffic_df_id_bc, toggle_active_filter, hardware_version)

    # Switch selected street if camera hardware version does not fit selection
    if hardware_version == [1] and current_hw == 2:
        id_street = 'Alte Jakobstraße (9000002582)'
    elif hardware_version == [2] and current_hw == 1:
        id_street = 'Dresdener Straße (9000006667)'
    elif hardware_version == [1, 2] or hardware_version == [] or hardware_version == [2, 1]:
        hardware_version = [1, 2]

    # Get number of selected segments
    nof_selected_segments = 'Number of selected segments: ' + str(len(df_map['segment_id'].unique()))

    # Update options for street_name_dd, without inactive
    df_map_options = df_map[df_map['map_line_color']!='Inactive - no data']
    street_name_dd_options = sorted(df_map_options['id_street'].unique())

    # Switch selected street if not in options
    if not id_street in street_name_dd_options:
        if current_hw == 1:
            id_street = 'Alte Jakobstraße (9000002582)'
        elif current_hw == 2:
           id_street = 'Dresdener Straße (9000006667)'
        else:
            id_street = street_name_dd_options[0]

    # Free up memory
    del df_map_options

    # Update map in case of selected street change
    if callback_trigger == 'street_map':
        street_name = clickData['points'][0]['hovertext']
        segment_id = clickData['points'][0]['customdata'][0]
        idx = df_map.loc[df_map['segment_id'] == segment_id]
        # Check if street inactive, if so, prevent update
        map_color_status = idx['map_line_color'].values[0]
        if map_color_status == 'Inactive - no data':
            raise PreventUpdate
        else:
            zoom_factor = 13
            id_street = street_name + ' (' + segment_id + ')'
    elif callback_trigger == 'street_name_dd':
        segment_id = id_street[-11:-1]
        idx = df_map.loc[df_map['segment_id'] == segment_id]
        zoom_factor = 13
    else:
        # Zoom out upon initial load or hardware change
        segment_id = id_street[-11:-1]
        idx = df_map.loc[df_map['segment_id'] == segment_id]
        zoom_factor = 11

    # TODO: improve efficiency by managing translation w/o recalculating bc ratios
    lon_str = idx['x'].values[0]
    lat_str = idx['y'].values[0]

    sep = '&nbsp;|&nbsp;'
    street_map = px.line_map(df_map, lat='y', lon='x', custom_data=['segment_id', 'hardware_version'],line_group='segment_id', hover_name = 'osm.name', color= 'map_line_color',
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

    return street_map, hardware_version, street_name_dd_options, id_street, nof_selected_segments

### General traffic callback ###
@callback(
    Output(component_id='selected_street_header', component_property='children'),
    Output(component_id='selected_street_header', component_property='style'),
    Output(component_id='street_id_text', component_property='children'),
    Output(component_id='date_range_text', component_property='children'),
    Output(component_id="date_filter", component_property="start_date", allow_duplicate=True),
    Output(component_id="date_filter", component_property="end_date", allow_duplicate=True),
    Output(component_id="date_filter", component_property="min_date_allowed"),
    Output(component_id="date_filter", component_property="max_date_allowed"),
    Output(component_id='date_range_text', component_property='style'),
    Output(component_id='pie_traffic', component_property='figure'),
    Output(component_id='line_abs_traffic', component_property='figure'),
    Output(component_id='bar_avg_traffic', component_property='figure'),
    Output(component_id='bar_perc_speed', component_property='figure'),
    Output(component_id='bar_v85', component_property='figure'),
    Output(component_id='bar_ranking', component_property='figure'),
    Input(component_id='radio_time_division', component_property='value'),
    Input(component_id='radio_time_unit', component_property='value'),
    Input(component_id='street_name_dd', component_property='value'),
    Input(component_id="date_filter", component_property="start_date"),
    Input(component_id="date_filter", component_property="end_date"),
    Input(component_id='range_slider', component_property='value'),
    Input(component_id='toggle_uptime_filter', component_property='value'),
    Input(component_id='toggle_active_filter', component_property='value'),
    Input(component_id='hardware_version', component_property='value'),
    Input(component_id='radio_y_axis', component_property='value'),
    Input(component_id='floating_button', component_property='n_clicks'),
    Input(component_id='language_selector', component_property='value'),
    prevent_initial_call='initial_duplicate',
)

def update_graphs(radio_time_division, radio_time_unit, id_street, start_date, end_date, hour_range, toggle_uptime_filter, toggle_active_filter, hardware_version, radio_y_axis, floating_button, lang_code_dd):

    callback_trigger = ctx.triggered_id
    print(callback_trigger)

    # Get segment_id/street name
    segment_id = id_street[-11:-1]
    street_id_text = 'Selected segment ID: ' + str(segment_id)
    street_name = id_street.split(' (')[0]
    selected_street_header = street_name

    #TODO: First callback triggers "hardware version"?

    ### Filter all traffic
    if callback_trigger in ['toggle_uptime_filter', 'toggle_active_filter', 'hardware_version']:

        query = ('CREATE OR REPLACE TEMP TABLE filtered_traffic AS '
                 'SELECT * '
                 'FROM all_traffic ')
        params = []

        # Filter all_traffic
        if toggle_uptime_filter == ['filter_uptime_selected']:
            # Filter uptime
            query += 'WHERE uptime > 0.7 '
            if toggle_active_filter == ['filter_active_selected']:
                # Filter active cameras
                query += 'AND CAST(last_data_package_naive AS DATE) >= ? '
                params = [two_weeks_ago]
            if hardware_version == [1]:
                query += 'AND hardware_version = 1 '
            elif hardware_version == [2]:
                query += 'AND hardware_version = 2 '
        else:
            # Filter active selected
            if toggle_active_filter == ['filter_active_selected']:
                query += 'WHERE CAST(last_data_package_naive AS DATE) >= ? '
                params = [two_weeks_ago]
                if hardware_version == [1]:
                    query += 'AND hardware_version = 1 '
                elif hardware_version == [2]:
                    query += 'AND hardware_version = 2 '
            else:
                if hardware_version == [1]:
                    query += 'WHERE hardware_version = 1 '
                elif hardware_version == [2]:
                    query += 'WHERE hardware_version = 2 '

        # Add or update table filtered_traffic
        with db_lock:  # Ensure thread safety for writes
            conn.execute(query, params)
            conn.execute('CREATE OR REPLACE TEMP TABLE filtered_traffic AS SELECT * EXCLUDE (uptime, hardware_version, last_data_package_naive) FROM filtered_traffic')

    # Check if selected street has data for selected data range
    min_date, max_date, start_date, end_date, message, missing_data = get_min_max_str(start_date, end_date, id_street, 'filtered_traffic')

    if callback_trigger in ['toggle_uptime_filter', 'toggle_active_filter', 'hardware_version', 'date_filter', 'range_slider']:

        # Create/update filtered traffic by start/end date
        query = """
        CREATE OR REPLACE TEMP TABLE filtered_traffic_dt AS
        SELECT *
        FROM filtered_traffic
        WHERE STRPTIME(date, '%d-%m-%Y') >= ? AND STRPTIME(date, '%d-%m-%Y') <= ?
        """
        params = [start_date, end_date]

        query += 'AND hour >= ? AND hour <= ?'
        params.append(hour_range[0])
        params.append(hour_range[1])

        with db_lock:  # Ensure thread safety for writes
            conn.execute(query, params)

    if callback_trigger in ['toggle_uptime_filter', 'toggle_active_filter', 'hardware_version', 'date_filter', 'range_slider', 'street_name_dd']:

        # Add selected street to filtered_traffic_dt table
        add_selected_street('filtered_traffic_dt', id_street, street_name)

    # Format dates for chart representation / processing
    if callback_trigger in ['date_filter']:
        from_date_format = '%Y-%m-%dT%H:%M:%S'
    else:
        from_date_format = '%Y-%m-%d'

    to_date_format = '%d %b %Y'

    # Align date formats
    start_date = parser.parse(start_date)
    end_date = parser.parse(end_date)
    start_date_str = datetime.strftime(start_date, to_date_format)
    end_date_str = datetime.strftime(end_date, to_date_format)

    min_date_str = format_str_date(min_date, '%Y-%m-%dT%H:%M:%S', to_date_format)
    max_date_str = format_str_date(max_date, '%Y-%m-%dT%H:%M:%S', to_date_format)

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
    query = ('SELECT street_selection, '
             'SUM(ped_total) AS ped_total, '
             'SUM(bike_total) AS bike_total, '
             'SUM(car_total) AS car_total, '
             'SUM(heavy_total) AS heavy_total '
             'FROM filtered_traffic_dt_str '
             'WHERE street_selection = ? '
             'GROUP BY street_selection')
    params = [street_name]

    with db_lock:  # Ensure thread safety for writes
        df_pie = conn.execute(query, params).fetchdf()

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
    group_cols = [radio_time_division, 'street_selection']
    group_clause = ", ".join(group_cols)
    query = f"""
    SELECT 
        {group_clause},
        SUM(ped_total) AS ped_total,
        SUM(bike_total) AS bike_total,
        SUM(car_total) AS car_total,
        SUM(heavy_total) AS heavy_total,
    MIN(date_local) AS first_seen
    FROM filtered_traffic_dt_str
    GROUP BY {group_clause}
    ORDER BY first_seen
    """

    with db_lock:  # Ensure thread safety for writes
        df_line_abs_traffic = conn.execute(query).fetchdf()

    line_abs_traffic = px.scatter(df_line_abs_traffic,
        x=radio_time_division, y=['ped_total', 'bike_total', 'car_total', 'heavy_total'],
        facet_col='street_selection',
        category_orders={'street_selection': [street_name, 'All Streets']},
        labels={'year': _('Year'), _('year_month'): _('Month'), 'year_week': _('Week'), 'date': _('Day'), 'date_hour': _('Hour')},
        color_discrete_map={'ped_total': ADFC_lightblue, 'bike_total': ADFC_green, 'car_total': ADFC_orange, 'heavy_total': ADFC_crimson},
        facet_col_spacing=0.04,
        title = (_('Absolute traffic count') + ' (' + start_date_str + ' - ' + end_date_str + ', ' + str(hour_range[0]) + ' - ' + str(hour_range[1]) + ' h)')
    ).update_traces(mode="lines+markers", connectgaps=False)

    line_abs_traffic.update_layout({'plot_bgcolor': ADFC_palegrey, 'paper_bgcolor': ADFC_palegrey})
    line_abs_traffic.update_layout(legend_title_text=_('Traffic Type'))
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
    #Range Slider: line_abs_traffic.update_xaxes(rangeslider_visible=True)


    ### Create average traffic bar chart
    group_cols = [radio_time_unit, 'street_selection']
    group_clause = ", ".join(group_cols)
    query = f"""
    SELECT 
        {group_clause},
        MEAN(ped_total) AS ped_total,
        MEAN(bike_total) AS bike_total,
        MEAN(car_total) AS car_total,
        MEAN(heavy_total) AS heavy_total,
    MIN(date_local) AS first_seen
    FROM filtered_traffic_dt_str
    GROUP BY {group_clause}
    ORDER BY first_seen
    """

    with db_lock:  # Ensure thread safety for writes
        df_avg_traffic = conn.execute(query).fetchdf()

    bar_avg_traffic = px.bar(df_avg_traffic,
        x=radio_time_unit, y=['ped_total', 'bike_total', 'car_total', 'heavy_total'],
        barmode='stack',
        facet_col='street_selection',
        facet_col_spacing=0.04,
        category_orders={'street_selection': [street_name, 'All Streets']},
        labels={'year': _('Year'), 'month': _('Month'), 'weekday': _('Week'), 'day': _('Day'), 'hour': _('Hour')},
        color_discrete_map={'ped_total': ADFC_lightblue, 'bike_total': ADFC_green, 'car_total': ADFC_orange, 'heavy_total': ADFC_crimson},
        title=(_('Average traffic count')  + ' (' + start_date_str + ' - ' + end_date_str + ', ' + str(hour_range[0]) + ' - ' + str(hour_range[1]) + ' h)')
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
    cols = ['car_speed0', 'car_speed10', 'car_speed20', 'car_speed30', 'car_speed40', 'car_speed50', 'car_speed60', 'car_speed70']
    sum_expr = " + ".join(cols)

    query = f"""
    WITH grouped AS (
        SELECT
            {radio_time_unit},
            street_selection,
            AVG(car_speed0)  AS car_speed0,
            AVG(car_speed10) AS car_speed10,
            AVG(car_speed20) AS car_speed20,
            AVG(car_speed30) AS car_speed30,
            AVG(car_speed40) AS car_speed40,
            AVG(car_speed50) AS car_speed50,
            AVG(car_speed60) AS car_speed60,
            AVG(car_speed70) AS car_speed70,
        MIN(date_local) AS first_seen
        FROM filtered_traffic_dt_str
        GROUP BY {radio_time_unit}, street_selection
        ORDER BY first_seen
    ),
    totals AS (
        SELECT
            *,
            car_speed0 + car_speed10 + car_speed20 + car_speed30 +
            car_speed40 + car_speed50 + car_speed60 + car_speed70
            AS total_speed
        FROM grouped
        WHERE ({sum_expr}) > 0 
    )
    SELECT
        {radio_time_unit},
        street_selection,
        car_speed0  / total_speed * 100 AS car_speed0,
        car_speed10 / total_speed * 100 AS car_speed10,
        car_speed20 / total_speed * 100 AS car_speed20,
        car_speed30 / total_speed * 100 AS car_speed30,
        car_speed40 / total_speed * 100 AS car_speed40,
        car_speed50 / total_speed * 100 AS car_speed50,
        car_speed60 / total_speed * 100 AS car_speed60,
        car_speed70 / total_speed * 100 AS car_speed70
    FROM totals
    """

    with db_lock:  # Ensure thread safety for writes
        df_bar_speed_traffic = conn.execute(query).fetchdf()

    bar_perc_speed = px.bar(df_bar_speed_traffic,
         x=radio_time_unit, y=cols,
         barmode='stack',
         facet_col='street_selection',
         category_orders={'street_selection': [street_name, 'All Streets']},
         labels={'year': _('Year'), 'month': _('Month'), 'weekday': _('Week'), 'day': _('Day'), 'hour': _('Hour')},
         color_discrete_map={'car_speed0': ADFC_lightgrey, 'car_speed10': ADFC_lightblue_D,
                             'car_speed20': ADFC_lightblue, 'car_speed30': ADFC_green,
                             'car_speed40': ADFC_green_L, 'car_speed50': ADFC_orange,
                             'car_speed60': ADFC_crimson, 'car_speed70': ADFC_pink},
         facet_col_spacing=0.04,
         title=(_('Average car speed %') + ' (' + start_date_str + ' - ' + end_date_str + ', ' + str(hour_range[0]) + ' - ' + str(hour_range[1]) + ' h)')
    )

    bar_perc_speed.for_each_annotation(lambda a: a.update(text=a.text.split("=")[1]))
    bar_perc_speed.for_each_annotation(lambda a: a.update(text=a.text.replace(street_name, street_name + _(' (segment:') + segment_id + ')')))
    bar_perc_speed.for_each_annotation(lambda a: a.update(text=a.text.replace('All Streets', _('All Streets'))))
    bar_perc_speed.update_layout(legend_title_text=_('Car speed'))
    bar_perc_speed.update_traces({'name': '0 - 10 km/h'}, selector={'name': 'car_speed0'})
    bar_perc_speed.update_traces({'name': '10 - 20 km/h'}, selector={'name': 'car_speed10'})
    bar_perc_speed.update_traces({'name': '20 - 30 km/h'}, selector={'name': 'car_speed20'})
    bar_perc_speed.update_traces({'name': '30 - 40 km/h'}, selector={'name': 'car_speed30'})
    bar_perc_speed.update_traces({'name': '40 - 50 km/h'}, selector={'name': 'car_speed40'})
    bar_perc_speed.update_traces({'name': '50 - 60 km/h'}, selector={'name': 'car_speed50'})
    bar_perc_speed.update_traces({'name': '60 - 70 km/h'}, selector={'name': 'car_speed60'})
    bar_perc_speed.update_traces({'name': '70 - 80 km/h'}, selector={'name': 'car_speed70'})
    bar_perc_speed.update_layout({'plot_bgcolor': ADFC_palegrey, 'paper_bgcolor': ADFC_palegrey})
    bar_perc_speed.update_layout(yaxis_title=_('Average car speed %'))
    bar_perc_speed.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))
    for annotation in bar_perc_speed.layout.annotations:
        annotation['font'] = {'size': 14}

    ### Create v85 bar graph

    group_cols = [radio_time_unit, 'street_selection']
    group_clause = ", ".join(group_cols)
    query = f"""
    SELECT 
        {group_clause},
        MEAN(v85) AS v85,
    MIN(date_local) AS first_seen
    FROM filtered_traffic_dt_str
    GROUP BY {group_clause}
    ORDER BY first_seen
    """

    with db_lock:  # Ensure thread safety for writes
        df_bar_v85_traffic = conn.execute(query).fetchdf()

    df_bar_v85 = df_bar_v85_traffic.groupby(by=[radio_time_unit, 'street_selection'], sort= False, as_index=False).agg({'v85': 'mean'})

    bar_v85 = px.bar(df_bar_v85,
        x=radio_time_unit, y='v85',
        color='v85',
        color_continuous_scale='temps',
        facet_col='street_selection',
        category_orders={'street_selection': [street_name, 'All Streets']},
        facet_col_spacing=0.04,
        labels={'year': _('Year'), 'month': _('Month'), 'weekday': _('Week'), 'day': _('Day'), 'hour': _('Hour')},
        title=(_('Speed cars v85') + ' (' + start_date_str + ' - ' + end_date_str + ', ' + str(hour_range[0]) + ' - ' + str(hour_range[1]) + ' h)')
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
    group_cols = ['id_street', 'street_selection']
    group_clause = ", ".join(group_cols)
    query = f"""
    SELECT 
        {group_clause},
        SUM(ped_total) AS ped_total,
        SUM(bike_total) AS bike_total,
        SUM(car_total) AS car_total,
        SUM(heavy_total) AS heavy_total,
    MIN(date_local) AS first_seen
    FROM filtered_traffic_dt
    GROUP BY {group_clause}
    ORDER BY {radio_y_axis} DESC
    """

    # Add or update table filtered_traffic_dt (for ranking chart)
    with db_lock:  # Ensure thread safety for writes
        df_bar_ranking = conn.execute(query).fetch_df()

    # Remove '90000' from the labels to reduce x-labels space required
    df_bar_ranking['x-labels'] = df_bar_ranking['id_street'].copy()
    df_bar_ranking['x-labels'] = df_bar_ranking['x-labels'].astype('string')
    df_bar_ranking['x-labels'] = df_bar_ranking['x-labels'].str.replace('900000', '')

    # Assess x and y for annotation
    #if not missing_data:
    annotation_index = df_bar_ranking[df_bar_ranking['id_street'] == id_street].index[0]
    annotation_x = annotation_index
    annotation_y = df_bar_ranking[radio_y_axis].values[annotation_x]

    bar_ranking = px.bar(df_bar_ranking,
        x='x-labels', y=radio_y_axis,
        color=radio_y_axis,
        color_continuous_scale='temps',
        labels={'ped_total': _('Pedestrians'), 'bike_total': _('Bikes'), 'car_total': _('Cars'), 'heavy_total': _('Heavy'), 'id_street': _('Street (segment id)')},
        title=(_('Absolute traffic') + ' (' + start_date_str + ' - ' + end_date_str + ', ' + str(hour_range[0]) + ' - ' + str(hour_range[1]) + ' h)'),
        height=600,
    )

    bar_ranking.for_each_annotation(lambda a: a.update(text=a.text.split("=")[1]))
    bar_ranking.add_annotation(x=annotation_x, y=annotation_y, text= street_name + '<br>' + _(' (segment:') + segment_id + ')', showarrow=True)
    bar_ranking.update_annotations(ax=0, ay=-40, arrowhead=2, arrowsize=2, arrowwidth = 1, arrowcolor= ADFC_darkgrey, xanchor='left')
    bar_ranking.update_layout(legend_title_text=_('Traffic Type'))
    bar_ranking.update_layout({'plot_bgcolor': ADFC_palegrey,'paper_bgcolor': ADFC_palegrey})
    bar_ranking.update_layout(yaxis_title= _('Absolute count'))
    for annotation in bar_ranking.layout.annotations: annotation['font'] = {'size': 14}

    return selected_street_header, selected_street_header_color, street_id_text, date_range_text, start_date, end_date, min_date, max_date, date_range_color, pie_traffic, line_abs_traffic, bar_avg_traffic, bar_perc_speed, bar_v85, bar_ranking

### Comparison Graph
@app.callback(
    Output('period_values_year', 'options'),
    Output('period_values_year', 'value'),
    Input('street_id_text', 'children'),
    Input(component_id="date_filter", component_property="min_date_allowed"),
    Input(component_id="date_filter", component_property="max_date_allowed")
)
def update_period_year_values(street_id_text, min_date, max_date):

    segment_id = street_id_text[-10:]

    query = ('SELECT DISTINCT segment_id, year '
             'FROM filtered_traffic '
             'WHERE segment_id = ? '
             'AND date_local >= ? AND date_local <= ? '
             'ORDER BY year')
    params = [segment_id, min_date, max_date]

    with db_lock:
        period_values_year_df = conn.execute(query, params).fetch_df()
        # Convert df to list
        period_values_year = period_values_year_df['year'].tolist()

    return period_values_year, period_values_year

@app.callback(
    Output('period_values_others', 'options'),
    Input('period_values_year', 'value'),
    Input('period_type_others', 'value'),
    Input('street_id_text', 'children'),
    Input(component_id="date_filter", component_property="min_date_allowed"),
    Input(component_id="date_filter", component_property="max_date_allowed")
)
def update_period_other_values(period_values_year, period_type_others, street_id_text, min_date, max_date):

    segment_id = street_id_text[-10:]

    placeholders = ','.join(['?'] * len(period_values_year))
    query = (f'SELECT DISTINCT segment_id, year, {_(period_type_others)}, '
             f'MIN(date_local) AS first_seen '
             f'FROM filtered_traffic '
             f'WHERE year IN ({placeholders}) '
             f'AND segment_id = ? '
             f'AND date_local >= ? AND date_local <= ? '
             f'GROUP BY segment_id, year, {_(period_type_others)} '
             f'ORDER BY first_seen')
    params = period_values_year
    params.append(segment_id)
    params.append(min_date)
    params.append(max_date)

    with db_lock:
        period_values_others_df = conn.execute(query, params).fetch_df()
        # Convert df to list
        period_values_others = period_values_others_df[period_type_others].tolist()

    return period_values_others

@app.callback(
    Output(component_id='line_avg_delta_traffic', component_property= 'figure'),
    Output(component_id='select_two', component_property= 'children'),
    Output(component_id='select_two', component_property= 'style'),
    Input(component_id='period_values_year', component_property='value'),
    Input(component_id='period_values_year', component_property='options'),
    Input(component_id='period_type_others', component_property='value'),
    Input(component_id='period_values_others', component_property='value'),
    Input(component_id='period_values_others', component_property='options'),
    Input(component_id='street_name_dd', component_property='value'),
    Input(component_id="date_filter", component_property="min_date_allowed"),
    Input(component_id="date_filter", component_property="max_date_allowed")
)

def comparison_chart(period_values_year, period_options_year,
                     period_type_others, period_values_others, period_options_others, id_street, min_date, max_date):

    callback_trigger = ctx.triggered_id

    segment_id = id_street[-11:-1]
    street_name = id_street.split(' (')[0]

    if not period_values_others or len(period_values_others) != 2:
        select_two_color = {'color': ADFC_orange}
        select_two_text = _('Select (exactly) two periods to compare:')
        period_type_others = 'year'
        period_values_others = ['2025', '2026']
    else:
        select_two_text = _('Select two periods to compare:')
        select_two_color = {'color': 'black'}

    # Add selected street to filtered_traffic
    add_selected_street('filtered_traffic', id_street, street_name)
    # Exclude speed columns to reduce size
    conn.execute('CREATE OR REPLACE TEMP TABLE filtered_traffic_str AS '
                 'SELECT * EXCLUDE (car_speed0, car_speed10, car_speed20, car_speed30, car_speed40, car_speed50, car_speed60, car_speed70, v85) '
                 'FROM filtered_traffic_str')

    # Create period A and B, based on period_type and values
    query_A = (f'CREATE OR REPLACE TEMP TABLE df_period_A AS '
               f'SELECT * '
               f'FROM filtered_traffic_str '
               f'WHERE {period_type_others} = ? '
               f'AND date_local >= ? AND date_local <= ?')

    params_A = [period_values_others[0], min_date, max_date]

    query_B = (f'CREATE OR REPLACE TEMP TABLE df_period_B AS '
               f'SELECT * '
               f'FROM filtered_traffic_str '
               f'WHERE {period_type_others} = ? '
               f'AND date_local >= ? AND date_local <= ?')
    params_B = [period_values_others[1], min_date, max_date]

    with db_lock:
        conn.execute(query_A, params_A)
        conn.execute(query_B, params_B)
        # duckdb_info(conn)

    # Prepare grouping and graph labels
    if period_type_others == _('year_month'):
        group_by = 'day'
        label = _('Month')
    elif period_type_others == 'year_week':
        group_by = _('weekday')
        label = _('Week')
    elif period_type_others == 'date':
        group_by = 'hour'
        label = _('Day')
    elif period_type_others == 'year':
        group_by = _('month')
        label = _('Year')

    # Prepare comparison graph data for periods A and B
    group_cols = ['street_selection', group_by]
    group_clause = ", ".join(group_cols)
    query_A = f"""
    CREATE OR REPLACE TEMP TABLE df_period_grp_A AS 
    SELECT 
        {group_clause},
        SUM(ped_total) AS ped_total,
        SUM(bike_total) AS bike_total,
        SUM(car_total) AS car_total,
        SUM(heavy_total) AS heavy_total,
    MIN(date_local) AS first_seen
    FROM df_period_A
    GROUP BY {group_clause}
    ORDER BY first_seen
    """

    query_B = f"""
    CREATE OR REPLACE TEMP TABLE df_period_grp_B AS 
    SELECT 
        {group_clause},
        SUM(ped_total) AS ped_total_d,
        SUM(bike_total) AS bike_total_d,
        SUM(car_total) AS car_total_d,
        SUM(heavy_total) AS heavy_total_d,
    MIN(date_local) AS first_seen
    FROM df_period_B
    GROUP BY {group_clause}
    ORDER BY first_seen
    """

    with db_lock:  # Ensure thread safety for writes
        #df_period_grp_B = conn.execute(query_B).fetchdf()
        conn.execute(query_A).fetchdf()
        conn.execute(query_B).fetchdf()

    # TODO: check if you need ORDER BY {group_by}
    # Merge period A and period B
    if period_type_others == _('date'):
        query = f"""
        SELECT *
        FROM df_period_grp_B
        FULL OUTER JOIN df_period_grp_A
        USING ({group_by}, street_selection)
        """
    else:
        query = f"""
        SELECT *
        FROM df_period_grp_B
        FULL OUTER JOIN df_period_grp_A
        USING ({group_by}, street_selection)
        """

    df_avg_traffic_delta_AB = conn.execute(query).fetchdf()

    duckdb_info(conn)

    # Draw graph
    line_avg_delta_traffic = px.line(df_avg_traffic_delta_AB,
        x=group_by, y=['ped_total', 'bike_total', 'car_total', 'heavy_total', 'ped_total_d', 'bike_total_d', 'car_total_d', 'heavy_total_d'],
        facet_col='street_selection',
        facet_col_spacing=0.04,
        category_orders={'street_selection': [street_name, 'All Streets']},
        labels={'year': _('Year'), 'month': _('Month'), 'weekday': _('Week day'), 'day': _('Day'), 'hour': _('Hour')},
        color_discrete_map={'ped_total': ADFC_lightblue, 'bike_total': ADFC_green, 'car_total': ADFC_orange, 'heavy_total': ADFC_crimson, 'ped_total_d': ADFC_lightblue, 'bike_total_d': ADFC_green, 'car_total_d': ADFC_orange, 'heavy_total_d': ADFC_crimson},
    )

    # Apply graph layout updates
    line_avg_delta_traffic.update_traces(selector={'name': 'ped_total_d'}, line={'dash': 'dash'})
    line_avg_delta_traffic.update_traces(selector={'name': 'bike_total_d'}, line={'dash': 'dash'})
    line_avg_delta_traffic.update_traces(selector={'name': 'car_total_d'}, line={'dash': 'dash'})
    line_avg_delta_traffic.update_traces(selector={'name': 'heavy_total_d'}, line={'dash': 'dash'})
    line_avg_delta_traffic.for_each_annotation(lambda a: a.update(text=a.text.replace(street_name, street_name + _(' (segment:') + segment_id + ')')))
    line_avg_delta_traffic.for_each_annotation(lambda a: a.update(text=a.text.split("=")[1]))
    line_avg_delta_traffic.for_each_annotation(lambda a: a.update(text=a.text.replace('All Streets', _('All Streets'))))
    line_avg_delta_traffic.update_layout({'plot_bgcolor': ADFC_palegrey,'paper_bgcolor': ADFC_palegrey})
    line_avg_delta_traffic.update_layout(title_text=_('Period') + ' A : ' + label + ' - ' + period_values_others[0] + ' , ' + _('Period') + ' B (----): ' + label + ' - ' + period_values_others[1])
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
    line_avg_delta_traffic.update_xaxes(matches=None)
    line_avg_delta_traffic.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))
    line_avg_delta_traffic.update_xaxes(dtick = 1, tickformat=".0f")
    for annotation in line_avg_delta_traffic.layout.annotations: annotation['font'] = {'size': 14}

    return line_avg_delta_traffic, select_two_text, select_two_color

if __name__ == "__main__":
    app.run(debug=False)
