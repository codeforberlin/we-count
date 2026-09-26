#!/usr/bin/env python3
# Copyright (c) 2024-2026 Berlin zählt Mobilität
# SPDX-License-Identifier: MIT

# @file    app.py
# @author  Egbert Klaassen
# @author  Michael Behrisch
# @date    2026-08-31

"""Dash application for the Berlin zählt Mobilität traffic dashboard.

Data flow
---------
``geo_df``      geopandas frame with the street geometries used by ``px.line_map``
``df_map_base`` geometry coordinates joined with the per-segment OSM features
``all_traffic`` DuckDB table holding the measurements from the parquet files

Concurrency
-----------
The DuckDB connection created at import time is shared, but every callback runs
its queries on its own ``conn.cursor()``.  A cursor is an independent connection
onto the same database with its own temporary-object catalog, so callbacks can
create temp tables with fixed names without clobbering each other and without
serialising all readers behind a lock.
"""
import gettext
import os
import random
from contextlib import contextmanager
from datetime import datetime, timedelta
from functools import lru_cache
from threading import Lock
from typing import Callable

import dash_bootstrap_components as dbc
import duckdb
import geopandas as gpd
import pandas as pd
import plotly.express as px
from dash import Dash, Input, Output, callback, ctx
from dash.exceptions import PreventUpdate

from .layout import (ADFC_blue, ADFC_crimson, ADFC_darkgrey, ADFC_green, ADFC_green_L,
                     ADFC_lightblue, ADFC_lightblue_D, ADFC_lightgrey, ADFC_orange,
                     ADFC_orange_L, ADFC_palegrey, ADFC_pink, ADFC_red,
                     INITIAL_LANGUAGE, INITIAL_STREET_ID, serve_layout)

# the following is basically to suppress warnings about "_" being undefined
# "from gettext import gettext as _" does not work because we use gettext.install later on, which installs "_"
_: Callable[[str], str]


# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
DEPLOYED = __name__ != '__main__'
ASSET_DIR = os.path.join(os.path.dirname(__file__), 'assets')
DATA_DIR = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'data')

ISO_FORMAT = '%Y-%m-%dT%H:%M:%S'
DISPLAY_DATE_FORMAT = '%d %b %Y'
ACTIVE_WINDOW = timedelta(weeks=2)
DEFAULT_RANGE = timedelta(days=14)

ALL_STREETS = 'All Streets'
INACTIVE = 'Inactive - no data'

TRAFFIC_COLUMNS = ('ped_total', 'bike_total', 'car_total', 'heavy_total')
SPEED_COLUMNS = tuple('car_speed%d' % speed for speed in range(0, 80, 10))

#: Values accepted for the street-type dropdown, doubles as the SQL allow list.
STREET_TYPES = ('primary', 'secondary', 'tertiary', 'residential')

#: Columns that may be aggregated on for the ranking chart (SQL allow list).
RANKING_COLUMNS = TRAFFIC_COLUMNS

#: Streets offered first when the current selection drops out of the filter.
PREFERRED_STREETS = (
    'Dresdener Straße (9000006667)',
    'Platz der Luftbrücke (9000007879)',
    'Wilhelmstraße (9000008514)',
    'Leipziger Straße (9000008543)',
    'Köpenicker Straße (9000006435)',
    'Adalbertstraße (9000009042)',
    'Alte Jakobstraße (9000002582)',
)

# The parquet files carry both English and German spellings of the calendar
# columns (e.g. "Mar" in `month` and "Mrz" in `Monat`) so that axis ticks appear
# in the user's language.  Mapping them explicitly here keeps the UI values
# locale independent and doubles as the allow list for the column names that get
# interpolated into SQL.
TIME_UNIT_COLUMNS = {
    'year': {'en': 'year', 'de': 'year'},
    'month': {'en': 'month', 'de': 'Monat'},
    'weekday': {'en': 'weekday', 'de': 'Wochentag'},
    'day': {'en': 'day', 'de': 'day'},
    'hour': {'en': 'hour', 'de': 'hour'},
}

TIME_DIVISION_COLUMNS = {
    'year': {'en': 'year', 'de': 'year'},
    'year_month': {'en': 'year_month', 'de': 'jahr_monat'},
    'year_week': {'en': 'year_week', 'de': 'year_week'},
    'date': {'en': 'date', 'de': 'date'},
    'date_hour': {'en': 'date_hour', 'de': 'date_hour'},
}

#: Which time division a "average per <unit>" chart has to average over.
TIME_UNIT_TO_DIVISION = {
    'year': 'year',
    'month': 'year_month',
    'weekday': 'year_week',
    'day': 'date',
    'hour': 'date_hour',
}

#: Grouping and title used by the period comparison chart.
COMPARISON_GROUPING = {
    'year': ('month', 'Year'),
    'year_month': ('day', 'Month'),
    'year_week': ('weekday', 'Week'),
    'date': ('hour', 'Day'),
}

DEFAULT_TIME_UNIT = 'weekday'
DEFAULT_TIME_DIVISION = 'date'
DEFAULT_PERIOD_TYPE = 'year'

TRAFFIC_TRACE_LABELS = {
    'ped_total': 'Pedestrians',
    'bike_total': 'Bikes',
    'car_total': 'Cars',
    'heavy_total': 'Heavy',
}

TIME_UNIT_LABELS = {
    'year': 'Year',
    'month': 'Month',
    'weekday': 'Week',
    'day': 'Day',
    'hour': 'Hour',
}

TIME_DIVISION_LABELS = {
    'year': 'Year',
    'year_month': 'Month',
    'year_week': 'Week',
    'date': 'Day',
    'date_hour': 'Hour',
}

TRAFFIC_COLOURS = {
    'ped_total': ADFC_lightblue,
    'bike_total': ADFC_green,
    'car_total': ADFC_orange,
    'heavy_total': ADFC_crimson,
}

MAP_COLOURS = {
    'More bikes than cars': ADFC_green,
    'More cars than bikes': ADFC_blue,
    'Over 2x more cars': ADFC_orange,
    'Over 5x more cars': ADFC_crimson,
    'Over 10x more cars': ADFC_pink,
    INACTIVE: ADFC_lightgrey,
}

#: Bike/car ratio bins and the map legend entry each bin maps to.
RATIO_BINS = [0, 0.1, 0.2, 0.5, 1, 500]
RATIO_LABELS = [
    'Over 10x more cars',
    'Over 5x more cars',
    'Over 2x more cars',
    'More cars than bikes',
    'More bikes than cars',
]

SPEED_COLOUR_MAP_50 = {
    'car_speed0': ADFC_lightgrey, 'car_speed10': ADFC_lightblue_D,
    'car_speed20': ADFC_lightblue, 'car_speed30': ADFC_green,
    'car_speed40': ADFC_green_L, 'car_speed50': ADFC_orange,
    'car_speed60': ADFC_crimson, 'car_speed70': ADFC_pink,
}

SPEED_COLOUR_MAP_30 = {
    'car_speed0': ADFC_lightgrey, 'car_speed10': ADFC_green_L,
    'car_speed20': ADFC_green, 'car_speed30': ADFC_orange_L,
    'car_speed40': ADFC_orange, 'car_speed50': ADFC_pink,
    'car_speed60': ADFC_red, 'car_speed70': ADFC_crimson,
}

MAP_ATTRIBUTION = (
    '<a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>&nbsp;|&nbsp;'
    '<a href="https://telraam.net">Telraam</a>&nbsp;|&nbsp;'
    '<a href="https://www.berlin.de/sen/uvk/mobilitaet-und-verkehr/verkehrsplanung/radverkehr/'
    'weitere-radinfrastruktur/zaehlstellen-und-fahrradbarometer/">SenUMVK Berlin</a><br>'
    '<a href="https://berlin-zaehlt.de/csv/">CSV</a> and '
    '<a href="https://berlin-zaehlt.de/parquet/">Parquet</a> data under '
    '<a href="https://creativecommons.org/licenses/by/4.0/">CC-BY 4.0</a> and '
    '<a href="https://www.govdata.de/dl-de/by-2-0">dl-de/by-2-0</a>'
)

db_lock = Lock()


# --------------------------------------------------------------------------- #
# Debug helpers (not used by the app, handy from a REPL)
# --------------------------------------------------------------------------- #
def output_excel(df, file_name):
    df.to_excel(os.path.join(ASSET_DIR, file_name + '.xlsx'), index=False)


def output_csv(df, file_name):
    df.to_csv(os.path.join(ASSET_DIR, file_name + '.csv'), index=False)


def duckdb_info(con):
    query = """
    SELECT table_name, column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'main'
    ORDER BY table_name, ordinal_position;
    """
    current_table = None
    for table, column, dtype in con.execute(query).fetchall():
        if table != current_table:
            print(f'\nTable: {table}')
            current_table = table
        print(f'  - {column} ({dtype})')
    print(con.execute('SELECT * FROM duckdb_memory()').fetchdf())


# --------------------------------------------------------------------------- #
# Data loading
# --------------------------------------------------------------------------- #
def retrieve_data():
    """Load the geo data and build the DuckDB database from the parquet files."""
    data_dir = DATA_DIR
    if not os.path.exists(os.path.join(data_dir, 'bzm_telraam_segments.geojson')):
        data_dir = ASSET_DIR

    if not DEPLOYED:
        print('Reading geojson data...')
    geo_df = gpd.read_file(os.path.join(data_dir, 'bzm_telraam_segments.geojson'),
                           columns=['segment_id', 'osm', 'cameras', 'geometry'])

    if not DEPLOYED:
        print('Reading json data...')
    json_df_features = pd.read_parquet(os.path.join(data_dir, 'df_geojson.parquet'))

    if not DEPLOYED:
        print('Reading traffic data...')
    db_path = os.path.join(data_dir, 'traffic.db')
    if os.path.exists(db_path):
        if not DEPLOYED:
            print('Replace existing database file')
        os.remove(db_path)

    connection = duckdb.connect(database=db_path)
    connection.read_parquet(os.path.join(data_dir, 'traffic_df_*.parquet'),
                            union_by_name=True).to_table('all_traffic')

    with db_lock:
        # Alter dtypes for data processing and to enable sort order
        connection.execute('ALTER TABLE all_traffic ALTER COLUMN day SET DATA TYPE INTEGER')
        # TODO: remove from parquet files
        connection.execute('ALTER TABLE all_traffic DROP COLUMN last_data_package')

    # Add last_data_package and osm.highway from the geo features to all_traffic
    features = json_df_features[['segment_id', 'last_data_package', 'osm.highway']].copy()
    features['last_data_package'] = pd.to_datetime(features['last_data_package'], format='mixed')
    features = features.rename(columns={'osm.highway': 'street_type'})
    features['street_type'] = features['street_type'].astype(object)

    with db_lock:
        connection.register('last_data_package_table', features)
        connection.execute("""
            CREATE OR REPLACE TABLE all_traffic AS
            SELECT a.*,
                   j.last_data_package AT TIME ZONE 'UTC' AS last_data_package_naive,
                   j.street_type AS street_type
            FROM all_traffic AS a
            LEFT JOIN last_data_package_table AS j ON a.segment_id = j.segment_id
        """)
        connection.unregister('last_data_package_table')

    del features
    return geo_df, json_df_features, connection


@contextmanager
def request_cursor():
    """Yield a private DuckDB connection for the duration of one callback.

    Temporary tables created on a cursor are invisible to every other cursor, so
    concurrent requests cannot overwrite each other's intermediate results.
    """
    cursor = conn.cursor()
    try:
        yield cursor
    finally:
        cursor.close()


# --------------------------------------------------------------------------- #
# Translation
# --------------------------------------------------------------------------- #
def update_language(lang_code):
    """Install the gettext catalogue for ``lang_code`` process wide."""
    global language
    language = lang_code if lang_code in ('en', 'de') else INITIAL_LANGUAGE
    localedir = os.path.join(os.path.dirname(__file__), 'locales')
    gettext.translation('bzm', localedir, fallback=True, languages=[language]).install()


def resolve_column(mapping, key, lang, default):
    """Map a UI value onto the matching DuckDB column for ``lang``.

    Unknown keys fall back to ``default``, so only column names that appear in
    the mapping can ever reach an f-string interpolated query.
    """
    entry = mapping.get(key) or mapping[default]
    return entry.get(lang, entry['en'])


# --------------------------------------------------------------------------- #
# Date helpers
# --------------------------------------------------------------------------- #
def format_str_date(str_date, from_format, to_format):
    return datetime.strptime(str_date, from_format).strftime(to_format)


def to_iso(value):
    """Normalise a date coming from a Dash component or DuckDB to an ISO string."""
    if isinstance(value, datetime):
        return value.strftime(ISO_FORMAT)
    if isinstance(value, str):
        # DatePickerRange hands back either "YYYY-MM-DD" or a full ISO timestamp
        return value if 'T' in value else value + 'T00:00:00'
    return datetime.combine(value, datetime.min.time()).strftime(ISO_FORMAT)


def clamp_date_range(start_date, end_date, min_date, max_date):
    """Clip the requested range to the range that actually holds data.

    Returns ``(start, end, message, missing_data)`` where ``message`` is already
    translated and ``missing_data`` says whether the request had to be adjusted.
    """
    if min_date is None or max_date is None:
        return start_date, end_date, _('Dates out of range'), True

    if start_date > max_date or end_date < min_date:
        return min_date, max_date, _('Dates out of range'), True
    if start_date < min_date and end_date > max_date:
        return min_date, max_date, _('Narrowed down range'), True
    if end_date > max_date:
        return start_date, max_date, _('End date out of range'), True
    if start_date < min_date:
        return min_date, end_date, _('Start date out of range'), True
    return start_date, end_date, None, False


def street_date_range(cursor, filter_sql, filter_params, id_street):
    """Return the ISO min/max measurement dates of one street under a filter."""
    row = cursor.execute(
        f'SELECT min(date_local), max(date_local) FROM all_traffic WHERE {filter_sql} AND id_street = ?',
        [*filter_params, id_street]).fetchone()
    if not row or row[0] is None:
        return None, None
    return row[0].strftime(ISO_FORMAT), row[1].strftime(ISO_FORMAT)


# --------------------------------------------------------------------------- #
# SQL building
# --------------------------------------------------------------------------- #
def build_filter(uptime_filter, active_filter, hardware_version, street_type):
    """Translate the filter controls into a WHERE fragment plus its parameters.

    Every branch appends to the same condition list, which avoids the malformed
    ``... FROM all_traffic AND street_type = ?`` that the previous nested
    if/else chain produced when only a street type was selected.
    """
    conditions, params = [], []

    if 'filter_uptime_selected' in (uptime_filter or []):
        conditions.append('uptime > 0.7')

    if 'filter_active_selected' in (active_filter or []):
        conditions.append('CAST(last_data_package_naive AS DATE) >= ?')
        params.append(TWO_WEEKS_AGO)

    hardware = sorted(hardware_version or [])
    if hardware in ([1], [2]):
        conditions.append('hardware_version = ?')
        params.append(hardware[0])

    if street_type in STREET_TYPES:
        conditions.append('street_type = ?')
        params.append(street_type)

    return ' AND '.join(conditions) if conditions else 'TRUE', params


def materialise_traffic(cursor, filter_sql, filter_params, start_date, end_date,
                        hour_range, id_street, street_name, drop_speed=False):
    """Build the per-request ``traffic`` temp table used by the chart queries.

    It holds the filtered measurements twice: once as they are (the "All
    Streets" facet) and once relabelled with the selected street name, flagged
    by ``is_selected``.  Narrowing by date, hour and filters happens in the same
    single pass over ``all_traffic``.
    """
    dropped = ['uptime', 'hardware_version', 'last_data_package_naive', 'street_type']
    if drop_speed:
        dropped.extend(SPEED_COLUMNS)
        dropped.append('v85')

    cursor.execute(f"""
        CREATE OR REPLACE TEMP TABLE traffic AS
        WITH filtered AS (
            SELECT * EXCLUDE ({', '.join(dropped)})
            FROM all_traffic
            WHERE {filter_sql}
              AND date_local >= CAST(? AS DATE)
              AND date_local <  CAST(? AS DATE) + INTERVAL 1 DAY
              AND hour BETWEEN ? AND ?
        )
        SELECT *, FALSE AS is_selected FROM filtered
        UNION ALL
        SELECT * REPLACE (? AS street_selection), TRUE AS is_selected
        FROM filtered WHERE id_street = ?
    """, [*filter_params, start_date, end_date, hour_range[0], hour_range[1], street_name, id_street])


def aggregate_columns(columns, function='SUM', suffix='', decimals=None):
    """Build the ``SUM(x) AS x`` / ``ROUND(AVG(x), 1) AS x`` list of a query."""
    def expression(col):
        call = f'{function}({col})'
        return call if decimals is None else f'ROUND({call}, {decimals})'
    return ',\n                   '.join(f'{expression(col)} AS {col}{suffix}' for col in columns)


# --------------------------------------------------------------------------- #
# Map data
# --------------------------------------------------------------------------- #
def bike_car_ratios(cursor, period=None):
    """Aggregate the bike/car ratio per segment, optionally for a single period.

    ``period`` is ``(start_date, end_date, (first_hour, last_hour))``, the same
    shape :func:`materialise_traffic` takes, so that the map colours follow the
    date and hour selection of the charts.  Without a period the ratios cover
    the whole table.
    """
    conditions, params = ['TRUE'], []
    if period is not None:
        conditions.append('date_local >= CAST(? AS DATE)')
        conditions.append('date_local <  CAST(? AS DATE) + INTERVAL 1 DAY')
        conditions.append('hour BETWEEN ? AND ?')
        start_date, end_date, hour_range = period
        params = [start_date, end_date, hour_range[0], hour_range[1]]

    frame = cursor.execute(f"""
        SELECT segment_id,
               SUM(bike_total) AS bike_total,
               SUM(car_total) AS car_total,
               CASE WHEN SUM(car_total) = 0 THEN NULL   -- avoid division by zero
                    ELSE CAST(SUM(bike_total) AS DOUBLE) / SUM(car_total)
               END AS bike_car_ratio
        FROM all_traffic
        WHERE {' AND '.join(conditions)}
        GROUP BY segment_id
    """, params).fetch_df()
    return get_bike_car_ratios(frame)


def get_bike_car_ratios(ratio_df):
    """Bin the bike/car ratio into the colour categories used by the map."""
    ratio_df['map_line_color'] = pd.cut(
        ratio_df['bike_car_ratio'], bins=RATIO_BINS, labels=RATIO_LABELS)
    ratio_df.set_index('segment_id', inplace=True)
    return ratio_df


def selected_period(start_date, end_date, hour_range):
    """Build the hashable period key of the current date and hour selection.

    Returns ``None`` while the selection is incomplete, which makes the map fall
    back to the ratios over all data instead of failing.
    """
    if not start_date or not end_date or not hour_range or len(hour_range) != 2:
        return None
    return to_iso(start_date), to_iso(end_date), (hour_range[0], hour_range[1])


@lru_cache(maxsize=32)
def map_data(active_only: bool, hardware: tuple, street_type: str, period) -> pd.DataFrame:
    """Return the map frame for one filter and period combination.

    ``period`` is the ``(start, end, hours)`` key from :func:`selected_period`,
    or ``None`` to colour the map from all data.  There are only a few dozen
    filter combinations and the frame is small (one row per geometry vertex), so
    caching avoids repeating the ratio query, the join, the category fill and
    the sort on every single map interaction.
    """
    with request_cursor() as cursor:
        ratios = bike_car_ratios(cursor, period)

    df_map = df_map_base.join(ratios)
    df_map = df_map[df_map['segment_id'].notnull()]

    # TODO: some streets in "bzm_telraam_segments.geojson" have no camera info
    # and so appear as hardware version "0", the below puts these to "1"
    df_map['hardware_version'] = df_map['hardware_version'].replace(0, 1)

    # TODO: Filter uptime although at the moment it looks like there are no streets with < 0.7 uptime only
    if active_only:
        active_ids = df_map.loc[df_map['last_data_package'] >= TWO_WEEKS_AGO, 'segment_id'].unique()
        df_map = df_map[df_map['segment_id'].isin(active_ids)]

    if list(hardware) in ([1], [2]):
        df_map = df_map[df_map['hardware_version'] == hardware[0]]

    if street_type in STREET_TYPES:
        df_map = df_map[df_map['osm.highway'] == street_type]

    # A street stays selectable as long as it has data at all, even when the
    # selected period holds none of it.  Deriving the selectable streets from
    # the colours instead would let a narrow period drop the current street,
    # and the replacement would in turn change the period again.
    df_map['has_data'] = df_map.index.isin(SEGMENTS_WITH_DATA)

    # Mark segments without a ratio in this period as inactive and sort for the
    # legend order.  The category is rebuilt explicitly because a period
    # without any data joins an empty frame and loses the categorical dtype.
    df_map['map_line_color'] = pd.Categorical(df_map['map_line_color'],
                                              categories=[*RATIO_LABELS, INACTIVE], ordered=True)
    df_map = df_map.fillna({'map_line_color': INACTIVE}).sort_values(by=['map_line_color'])

    # Move the segment_id index into a column (avoids the ambiguity of two
    # segment_id columns in line_map since Plotly 6.0)
    df_map = df_map.drop('segment_id', axis=1).reset_index(level=0)
    df_map['segment_id'] = df_map['segment_id'].astype(str)
    df_map['preferred_street'] = df_map['id_street'].isin(PREFERRED_STREETS)
    return df_map


def pick_street(df_map, options):
    """Choose a replacement street when the current one leaves the selection."""
    preferred = [street for street in PREFERRED_STREETS if street in options]
    if preferred:
        return preferred[0]
    return random.choice(options) if options else INITIAL_STREET_ID


# --------------------------------------------------------------------------- #
# Figure helpers
# --------------------------------------------------------------------------- #
def traffic_labels():
    return {col: _(label) for col, label in TRAFFIC_TRACE_LABELS.items()}


def range_suffix(start_str, end_str, hour_range):
    return f' ({start_str} - {end_str}, {hour_range[0]} - {hour_range[1]} h)'


def rename_traffic_traces(fig, suffix='', columns=TRAFFIC_COLUMNS):
    for col in columns:
        fig.update_traces({'name': _(TRAFFIC_TRACE_LABELS[col]) + suffix}, selector={'name': col})


def apply_facet_layout(fig, street_name, segment_id, *, y_title=None, legend_title=None,
                       facet_note='', independent_y=True, independent_x=False):
    """Apply the shared styling of the faceted "selected street vs all" charts.

    Replaces the block of ``update_layout``/``for_each_annotation`` calls that
    was repeated almost verbatim for every chart.
    """
    fig.update_layout(plot_bgcolor=ADFC_palegrey, paper_bgcolor=ADFC_palegrey)
    if y_title:
        fig.update_layout(yaxis_title=y_title)
    if legend_title:
        fig.update_layout(legend_title_text=legend_title)
    if independent_y:
        fig.update_yaxes(matches=None)
    if independent_x:
        fig.update_xaxes(matches=None)
    fig.for_each_yaxis(lambda axis: axis.update(showticklabels=True))

    selected_label = street_name + _(' (segment:') + segment_id + facet_note + ')'

    def relabel(annotation):
        # Facet titles arrive as "street_selection=<value>"
        text = annotation.text.split('=')[-1]
        if text == street_name:
            text = selected_label
        elif text == ALL_STREETS:
            text = _(ALL_STREETS)
        annotation.update(text=text, font={'size': 14})

    fig.for_each_annotation(relabel)
    return fig


# --------------------------------------------------------------------------- #
# Module initialisation
# --------------------------------------------------------------------------- #
geo_df, json_df_features, conn = retrieve_data()

update_language(INITIAL_LANGUAGE)

# Overall data range, used to seed the date picker
with db_lock:
    data_start, data_end = conn.execute('SELECT MIN(date_local), MAX(date_local) FROM all_traffic').fetchone()

start_date = max(data_start, data_end - DEFAULT_RANGE).strftime(ISO_FORMAT)
end_date = data_end.strftime(ISO_FORMAT)

# "Active" means a camera delivered data within two weeks of the last measurement
TWO_WEEKS_AGO = (data_end - ACTIVE_WINDOW).strftime(ISO_FORMAT)

# Clip the seeded range to what the initial street actually has
# TODO: ensure initial street has data in the last two weeks
with request_cursor() as _cursor:
    _min, _max = street_date_range(_cursor, 'TRUE', [], INITIAL_STREET_ID)
    min_date, max_date = _min or start_date, _max or end_date
    start_date, end_date, _message, _missing = clamp_date_range(start_date, end_date, min_date, max_date)

    # Street options for the dropdown: only streets with usable, recent data
    id_street_options = [row[0] for row in _cursor.execute("""
        SELECT DISTINCT id_street
        FROM all_traffic
        WHERE uptime > 0.7 AND CAST(last_data_package_naive AS DATE) >= ?
        ORDER BY id_street
    """, [TWO_WEEKS_AGO]).fetchall()]

if not DEPLOYED:
    print('Prepare map...')

# Join the geometry coordinates with the per-segment OSM features
geo_df_map_info = geo_df.get_coordinates().join(geo_df[['segment_id']])
geo_df_map_info['segment_id'] = geo_df_map_info['segment_id'].astype(int)
geo_df_map_info.set_index('segment_id', drop=False, inplace=True)

json_df_features['segment_id'] = json_df_features['segment_id'].astype(int)
json_df_features.set_index('segment_id', inplace=True)

# TODO: move json_df_features to geopandas
df_map_base = geo_df_map_info.join(json_df_features)
# Remove rows w/o street names
df_map_base = df_map_base[df_map_base['osm.name'].notnull()]

#: Segment ids that hold data at all.  The map colours follow the selected
#: period, but a street only leaves the selection when it has no data ever, so
#: that narrowing a period cannot start a street/period feedback loop.
with db_lock:
    SEGMENTS_WITH_DATA = frozenset(
        row[0] for row in conn.execute('SELECT DISTINCT segment_id FROM all_traffic').fetchall())

#: Speed limit per segment, looked up once instead of scanning a map frame that
#: may have been filtered down by the time the chart callback needs it.
SEGMENT_MAXSPEED = {
    str(int(segment)): maxspeed
    for segment, maxspeed in zip(df_map_base.index, df_map_base['osm.maxspeed'])
}

del geo_df_map_info, json_df_features

if not DEPLOYED:
    print('Starting dash ...')

app = Dash(__name__,
           external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.BOOTSTRAP, '/assets/main.css'],
           meta_tags=[{'name': 'viewport', 'content': 'width=device-width, initial-scale=1'}])

app.title = 'Berlin-zaehlt'
app.layout = lambda: serve_layout(app, id_street_options, start_date, end_date, min_date, max_date)


# --------------------------------------------------------------------------- #
# Callbacks
# --------------------------------------------------------------------------- #
@app.callback(
    Output('url', 'href'),
    Input('language_selector', 'value'),
    prevent_initial_call=True,
)
def get_language(lang_code_dd):
    """Switch the catalogue and reload so the whole layout is re-rendered."""
    update_language(lang_code_dd)
    return '/'


@callback(
    Output('street_map', 'figure'),
    Output('hardware_version', 'value'),
    Output('street_name_dd', 'options'),
    Output('street_name_dd', 'value'),
    Output('nof_selected_segments', 'children'),
    Input('street_map', 'clickData'),
    Input('street_name_dd', 'value'),
    Input('street_type_dd', 'value'),
    Input('hardware_version', 'value'),
    Input('toggle_active_filter', 'value'),
    Input('toggle_map_style', 'value'),
    # Read as inputs instead of being served from update_graphs: a callback that
    # both takes the street selection and writes the date range would close a
    # dependency cycle with the map that colours itself from that range.
    Input('date_filter', 'start_date'),
    Input('date_filter', 'end_date'),
    Input('range_slider', 'value'),
)
def update_map(click_data, id_street, street_type_dd, hardware_version, toggle_active_filter,
               toggle_map_style, start_date, end_date, hour_range):
    trigger = ctx.triggered_id

    # Do not allow both hardware versions to be switched off
    if not hardware_version:
        hardware_version = [1, 2]

    period = selected_period(start_date, end_date, hour_range)
    df_map = map_data('filter_active_selected' in (toggle_active_filter or []),
                      tuple(sorted(hardware_version)), street_type_dd, period)

    # Streets that can be selected: everything the map shows that holds data
    street_options = sorted(df_map.loc[df_map['has_data'], 'id_street'].unique())

    if trigger == 'street_map' and click_data:
        street_name = click_data['points'][0]['hovertext']
        segment_id = click_data['points'][0]['customdata'][0]
        selected = df_map.loc[df_map['segment_id'] == segment_id]
        if selected.empty or not selected['has_data'].iloc[0]:
            raise PreventUpdate
        id_street = f'{street_name} ({segment_id})'
        zoom_factor = 13
    else:
        zoom_factor = 13 if trigger == 'street_name_dd' else 11

    # Keep the current street whenever it survives the filters, otherwise fall
    # back to a preferred one so the charts always have something to show
    if id_street not in street_options:
        id_street = pick_street(df_map, street_options)

    segment_id = id_street[-11:-1]
    selected = df_map.loc[df_map['segment_id'] == segment_id]
    if selected.empty:
        selected = df_map
    centre = {'lat': selected['y'].iloc[0], 'lon': selected['x'].iloc[0]}

    nof_selected_segments = _('Number of selected segments: ') + str(df_map['segment_id'].nunique())

    street_map = px.line_map(
        df_map, lat='y', lon='x',
        custom_data=['segment_id', 'hardware_version'],
        line_group='segment_id', hover_name='osm.name', color='map_line_color',
        color_discrete_map=MAP_COLOURS,
        hover_data={
            'map_line_color': False,
            'osm.highway': True,
            'osm.address.city': True,
            'osm.address.suburb': True,
            'osm.address.postcode': True,
            'hardware_version': True,
            'osm.maxspeed': True},
        labels={
            'segment_id': 'Segment',
            'osm.highway': _('Highway type'),
            'x': 'Lon',
            'y': 'Lat',
            'osm.address.city': _('City'),
            'osm.address.suburb': _('District'),
            'osm.address.postcode': _('Postal code'),
            'hardware_version': _('Hardware version'),
            'osm.maxspeed': _('Speed limit')},
        map_style=toggle_map_style or 'streets',
        center=centre, zoom=zoom_factor)

    street_map.update_traces(mode='lines+markers', line_width=5, opacity=0.7)
    for key in MAP_COLOURS:
        street_map.update_traces({'name': _(key)}, selector={'name': key})
    street_map.update_traces(visible='legendonly', selector={'name': _(INACTIVE)})

    street_map.update_layout(
        uirevision=True,
        autosize=True,
        margin={'l': 0, 'r': 0, 't': 0, 'b': 0},
        legend_title=_('Street color'),
        legend={'bgcolor': 'rgba(255,255,255,0.6)', 'yanchor': 'top', 'y': 0.99,
                'xanchor': 'right', 'x': 0.99},
        annotations=[{'text': MAP_ATTRIBUTION, 'showarrow': False, 'align': 'left',
                      'xref': 'paper', 'yref': 'paper', 'x': 0, 'y': 0,
                      'font': {'size': 10}}],
    )

    return street_map, hardware_version, street_options, id_street, nof_selected_segments


@callback(
    Output('selected_street_header', 'children'),
    Output('selected_street_header', 'style'),
    Output('street_id_text', 'children'),
    Output('date_range_text', 'children'),
    Output('date_filter', 'min_date_allowed'),
    Output('date_filter', 'max_date_allowed'),
    Output('date_range_text', 'style'),
    Output('pie_traffic', 'figure'),
    Output('line_abs_traffic', 'figure'),
    Output('bar_avg_traffic_hr', 'figure'),
    Output('bar_avg_traffic', 'figure'),
    Output('bar_perc_speed', 'figure'),
    Output('bar_v85', 'figure'),
    Output('bar_ranking', 'figure'),
    Input('radio_time_division', 'value'),
    Input('radio_time_unit', 'value'),
    Input('street_name_dd', 'value'),
    Input('street_type_dd', 'value'),
    Input('date_filter', 'start_date'),
    Input('date_filter', 'end_date'),
    Input('range_slider', 'value'),
    Input('toggle_uptime_filter', 'value'),
    Input('toggle_active_filter', 'value'),
    Input('hardware_version', 'value'),
    Input('radio_y_axis', 'value'),
    Input('language_selector', 'value'),
)
def update_graphs(radio_time_division, radio_time_unit, id_street, street_type_dd, start_date,
                  end_date, hour_range, toggle_uptime_filter, toggle_active_filter,
                  hardware_version, radio_y_axis, lang_code_dd):
    if not id_street:
        raise PreventUpdate

    lang = lang_code_dd if lang_code_dd in ('en', 'de') else INITIAL_LANGUAGE
    time_unit = resolve_column(TIME_UNIT_COLUMNS, radio_time_unit, lang, DEFAULT_TIME_UNIT)
    time_division = resolve_column(TIME_DIVISION_COLUMNS, radio_time_division, lang, DEFAULT_TIME_DIVISION)
    unit_division = resolve_column(TIME_DIVISION_COLUMNS,
                                   TIME_UNIT_TO_DIVISION.get(radio_time_unit, DEFAULT_TIME_DIVISION),
                                   lang, DEFAULT_TIME_DIVISION)
    y_axis = radio_y_axis if radio_y_axis in RANKING_COLUMNS else 'car_total'

    segment_id = id_street[-11:-1]
    street_name = id_street.split(' (')[0]
    street_id_text = _('Selected segment ID: ') + str(segment_id)

    traffic_label_map = traffic_labels()

    filter_sql, filter_params = build_filter(
        toggle_uptime_filter, toggle_active_filter, hardware_version, street_type_dd)

    start_date, end_date = to_iso(start_date), to_iso(end_date)

    with request_cursor() as cursor:
        min_date, max_date = street_date_range(cursor, filter_sql, filter_params, id_street)
        start_date, end_date, message, missing_data = clamp_date_range(
            start_date, end_date, min_date, max_date)
        min_date, max_date = min_date or start_date, max_date or end_date

        materialise_traffic(cursor, filter_sql, filter_params, start_date, end_date,
                            hour_range, id_street, street_name)

        # ---- Warnings about missing data -------------------------------- #
        start_date_str = format_str_date(start_date, ISO_FORMAT, DISPLAY_DATE_FORMAT)
        end_date_str = format_str_date(end_date, ISO_FORMAT, DISPLAY_DATE_FORMAT)
        min_date_str = format_str_date(min_date, ISO_FORMAT, DISPLAY_DATE_FORMAT)
        max_date_str = format_str_date(max_date, ISO_FORMAT, DISPLAY_DATE_FORMAT)
        suffix = range_suffix(start_date_str, end_date_str, hour_range)

        if missing_data:
            date_range_text = f'{message}, ' + _('available') + f': {min_date_str}' + _(' to ') + max_date_str
            warn_colour = ADFC_crimson if message == _('Dates out of range') else ADFC_orange
            selected_street_header_color = {'color': warn_colour}
            date_range_color = {'color': warn_colour}
        else:
            date_range_text = _('Pick date range:')
            selected_street_header_color = {'color': ADFC_green}
            date_range_color = {'color': 'black'}

        # ---- Pie chart --------------------------------------------------- #
        totals = cursor.execute(f"""
            SELECT {aggregate_columns(TRAFFIC_COLUMNS)}
            FROM traffic WHERE is_selected
        """).fetchone() or (0, 0, 0, 0)
        pie_df = pd.DataFrame({'type': [traffic_label_map[col] for col in TRAFFIC_COLUMNS],
                               'count': [value or 0 for value in totals]})
        pie_traffic = px.pie(pie_df, names='type', values='count', color='type',
                             color_discrete_map={traffic_label_map[col]: TRAFFIC_COLOURS[col]
                                                 for col in TRAFFIC_COLUMNS})
        pie_traffic.update_layout(margin={'l': 0, 'r': 0, 't': 0, 'b': 0}, showlegend=False)
        pie_traffic.update_traces(textposition='inside', textinfo='percent+label')

        # ---- Absolute traffic over time ---------------------------------- #
        df_line_abs = cursor.execute(f"""
            SELECT {time_division}, street_selection,
                   {aggregate_columns(TRAFFIC_COLUMNS)},
                   MIN(date_local) AS first_seen
            FROM traffic
            GROUP BY {time_division}, street_selection
            ORDER BY first_seen
        """).pl()

        facet_order = {'street_selection': [street_name, ALL_STREETS]}
        division_label = {time_division: _(TIME_DIVISION_LABELS.get(radio_time_division, 'Day'))}
        unit_label = {time_unit: _(TIME_UNIT_LABELS.get(radio_time_unit, 'Week'))}

        line_abs_traffic = px.scatter(
            df_line_abs, x=time_division, y=list(TRAFFIC_COLUMNS),
            facet_col='street_selection', facet_col_spacing=0.04,
            category_orders=facet_order, labels=division_label,
            color_discrete_map=TRAFFIC_COLOURS,
            title=_('Absolute traffic count') + suffix,
        ).update_traces(mode='lines+markers', connectgaps=False)
        rename_traffic_traces(line_abs_traffic)
        apply_facet_layout(line_abs_traffic, street_name, segment_id,
                           y_title=_('Absolute traffic count'), legend_title=_('Traffic Type'),
                           independent_x=True)

        # ---- Average traffic per hour ------------------------------------ #
        df_avg_hr = cursor.execute(f"""
            SELECT {time_unit}, street_selection,
                   {aggregate_columns(TRAFFIC_COLUMNS, 'AVG', decimals=1)},
                   MIN(date_local) AS first_seen
            FROM traffic
            GROUP BY {time_unit}, street_selection
            ORDER BY first_seen
        """).pl()

        bar_avg_traffic_hr = px.bar(
            df_avg_hr, x=time_unit, y=list(TRAFFIC_COLUMNS), barmode='stack',
            facet_col='street_selection', facet_col_spacing=0.04,
            category_orders=facet_order, labels=unit_label,
            color_discrete_map=TRAFFIC_COLOURS,
            title=_('Average traffic count per hour') + suffix)
        rename_traffic_traces(bar_avg_traffic_hr)
        bar_avg_traffic_hr.update_xaxes(dtick=1, tickformat='.0f')
        apply_facet_layout(bar_avg_traffic_hr, street_name, segment_id,
                           y_title=_('Average traffic count per hour'),
                           legend_title=_('Traffic Type'), independent_y=False)

        # ---- Average traffic per time unit ------------------------------- #
        # Sum within each division first (e.g. per calendar week), then average
        # those sums over the unit (e.g. per weekday).
        df_avg = cursor.execute(f"""
            WITH per_division AS (
                SELECT {unit_division}, {time_unit}, street_selection,
                       {aggregate_columns(TRAFFIC_COLUMNS)},
                       MIN(date_local) AS first_seen
                FROM traffic
                GROUP BY {unit_division}, {time_unit}, street_selection
            )
            SELECT {time_unit}, street_selection,
                   {aggregate_columns(TRAFFIC_COLUMNS, 'AVG', decimals=1)},
                   MIN(first_seen) AS first_seen
            FROM per_division
            GROUP BY {time_unit}, street_selection
            ORDER BY first_seen
        """).pl()

        unit_title = _('Average traffic count per ') + _(TIME_UNIT_LABELS.get(radio_time_unit, 'Week'))
        bar_avg_traffic = px.bar(
            df_avg, x=time_unit, y=list(TRAFFIC_COLUMNS), barmode='stack',
            facet_col='street_selection', facet_col_spacing=0.04,
            category_orders=facet_order, labels=unit_label,
            color_discrete_map=TRAFFIC_COLOURS,
            title=unit_title + suffix)
        rename_traffic_traces(bar_avg_traffic)
        bar_avg_traffic.update_xaxes(dtick=1, tickformat='.0f')
        apply_facet_layout(bar_avg_traffic, street_name, segment_id, y_title=unit_title,
                           legend_title=_('Traffic Type'))

        # ---- Car speed distribution -------------------------------------- #
        speed_avgs = ',\n                       '.join(
            f'ROUND(AVG({col}), 1) AS {col}' for col in SPEED_COLUMNS)
        speed_total = ' + '.join(SPEED_COLUMNS)
        speed_shares = ',\n                   '.join(
            f'ROUND({col} / total_speed * 100, 1) AS {col}' for col in SPEED_COLUMNS)

        df_speed = cursor.execute(f"""
            WITH grouped AS (
                SELECT {time_unit}, street_selection,
                       {speed_avgs},
                       MIN(date_local) AS first_seen
                FROM traffic
                GROUP BY {time_unit}, street_selection
            ),
            totals AS (
                SELECT *, {speed_total} AS total_speed
                FROM grouped
                WHERE ({speed_total}) > 0
            )
            SELECT {time_unit}, street_selection,
                   {speed_shares}
            FROM totals
            ORDER BY first_seen
        """).pl()

        maxspeed = str(SEGMENT_MAXSPEED.get(segment_id, '50'))
        if maxspeed == '30':
            speed_colour_map = SPEED_COLOUR_MAP_30
        elif maxspeed == "['50', '30']":
            speed_colour_map, maxspeed = SPEED_COLOUR_MAP_30, '30 / 50'
        else:
            speed_colour_map = SPEED_COLOUR_MAP_50

        bar_perc_speed = px.bar(
            df_speed, x=time_unit, y=list(SPEED_COLUMNS), barmode='stack',
            facet_col='street_selection', facet_col_spacing=0.04,
            category_orders=facet_order, labels=unit_label,
            color_discrete_map=speed_colour_map,
            title=_('Average car speed %') + suffix)
        for index, col in enumerate(SPEED_COLUMNS):
            bar_perc_speed.update_traces({'name': f'{index * 10} - {index * 10 + 10} km/h'},
                                         selector={'name': col})
        apply_facet_layout(bar_perc_speed, street_name, segment_id,
                           y_title=_('Average car speed %'), legend_title=_('Car speed'),
                           facet_note=f', max {maxspeed} km/h', independent_y=False)

        # ---- v85 ---------------------------------------------------------- #
        df_v85 = cursor.execute(f"""
            SELECT {time_unit}, street_selection,
                   ROUND(MEAN(v85), 1) AS v85,
                   MIN(date_local) AS first_seen
            FROM traffic
            GROUP BY {time_unit}, street_selection
            ORDER BY first_seen
        """).pl()

        bar_v85 = px.bar(df_v85, x=time_unit, y='v85', color='v85',
                         color_continuous_scale='temps',
                         facet_col='street_selection', facet_col_spacing=0.04,
                         category_orders=facet_order, labels=unit_label,
                         title=_('Speed cars v85') + suffix)
        bar_v85.update_xaxes(dtick=1, tickformat='.0f')
        bar_v85.update_yaxes(dtick=5, tickformat='.0f')
        bar_v85.update_coloraxes(colorbar_title_text=_('v85 in km/h'))
        apply_facet_layout(bar_v85, street_name, segment_id, y_title=_('v85 in km/h'),
                           independent_y=False)

        # ---- Ranking ------------------------------------------------------ #
        df_ranking = cursor.execute(f"""
            SELECT id_street,
                   {aggregate_columns(TRAFFIC_COLUMNS)}
            FROM traffic
            WHERE NOT is_selected
            GROUP BY id_street
            ORDER BY {y_axis} DESC
        """).fetch_df()

        # Shorten the segment ids in the tick labels to save horizontal space
        df_ranking['x-labels'] = df_ranking['id_street'].astype('string').str.replace('90000', '', regex=False)

        bar_ranking = px.bar(
            df_ranking, x='x-labels', y=y_axis, color=y_axis,
            color_continuous_scale='temps',
            hover_data={col: True for col in (*TRAFFIC_COLUMNS, 'id_street')},
            labels={**traffic_label_map, 'id_street': _('Street (segment id)')},
            title=_('Absolute traffic') + suffix)
        bar_ranking.update_layout(plot_bgcolor=ADFC_palegrey, paper_bgcolor=ADFC_palegrey,
                                  yaxis_title=_('Absolute count'), xaxis_title=None)
        #bar_ranking.update_xaxes(showticklabels=False)
        bar_ranking.update_coloraxes(colorbar_title_text=traffic_label_map[y_axis])

        # Point at the selected street, if it is part of the ranking at all
        selected_rows = df_ranking.index[df_ranking['id_street'] == id_street]
        if len(selected_rows):
            row = df_ranking.loc[selected_rows[0]]
            bar_ranking.add_annotation(
                x=row['x-labels'], y=row[y_axis],
                text=street_name + '<br>' + _(' (segment:') + segment_id + ')',
                showarrow=True, ax=0, ay=-40, arrowhead=2, arrowsize=2, arrowwidth=1,
                arrowcolor=ADFC_darkgrey, xanchor='left', font={'size': 14})

    return (street_name, selected_street_header_color, street_id_text, date_range_text,
            min_date, max_date, date_range_color,
            pie_traffic, line_abs_traffic, bar_avg_traffic_hr, bar_avg_traffic,
            bar_perc_speed, bar_v85, bar_ranking)


# --------------------------------------------------------------------------- #
# Period comparison
# --------------------------------------------------------------------------- #
@app.callback(
    Output('period_values_year', 'options'),
    Output('period_values_year', 'value'),
    Input('street_name_dd', 'value'),
    Input('date_filter', 'min_date_allowed'),
    Input('date_filter', 'max_date_allowed'),
    Input('toggle_uptime_filter', 'value'),
    Input('toggle_active_filter', 'value'),
    Input('hardware_version', 'value'),
    Input('street_type_dd', 'value'),
)
def update_period_year_values(id_street, min_date, max_date, toggle_uptime_filter,
                              toggle_active_filter, hardware_version, street_type_dd):
    if not id_street or not min_date or not max_date:
        raise PreventUpdate

    filter_sql, filter_params = build_filter(
        toggle_uptime_filter, toggle_active_filter, hardware_version, street_type_dd)

    with request_cursor() as cursor:
        years = [row[0] for row in cursor.execute(f"""
            SELECT DISTINCT year
            FROM all_traffic
            WHERE {filter_sql} AND id_street = ?
              AND date_local >= ? AND date_local <= ?
            ORDER BY year
        """, [*filter_params, id_street, to_iso(min_date), to_iso(max_date)]).fetchall()]

    return years, years


@app.callback(
    Output('period_values_others', 'options'),
    Input('period_values_year', 'value'),
    Input('period_type_others', 'value'),
    Input('street_name_dd', 'value'),
    Input('date_filter', 'min_date_allowed'),
    Input('date_filter', 'max_date_allowed'),
    Input('language_selector', 'value'),
    Input('toggle_uptime_filter', 'value'),
    Input('toggle_active_filter', 'value'),
    Input('hardware_version', 'value'),
    Input('street_type_dd', 'value'),
)
def update_period_other_values(period_values_year, period_type_others, id_street, min_date,
                               max_date, lang_code_dd, toggle_uptime_filter,
                               toggle_active_filter, hardware_version, street_type_dd):
    if not period_values_year or not id_street or not min_date or not max_date:
        return []

    lang = lang_code_dd if lang_code_dd in ('en', 'de') else INITIAL_LANGUAGE
    period_column = resolve_column(TIME_DIVISION_COLUMNS, period_type_others, lang, DEFAULT_PERIOD_TYPE)

    filter_sql, filter_params = build_filter(
        toggle_uptime_filter, toggle_active_filter, hardware_version, street_type_dd)

    placeholders = ', '.join('?' * len(period_values_year))
    # Never mutate the value list handed over by Dash
    params = [*filter_params, *period_values_year, id_street, to_iso(min_date), to_iso(max_date)]

    with request_cursor() as cursor:
        return [row[0] for row in cursor.execute(f"""
            SELECT {period_column}, MIN(date_local) AS first_seen
            FROM all_traffic
            WHERE {filter_sql}
              AND year IN ({placeholders})
              AND id_street = ?
              AND date_local >= ? AND date_local <= ?
            GROUP BY {period_column}
            ORDER BY first_seen
        """, params).fetchall()]


@app.callback(
    Output('line_avg_delta_traffic', 'figure'),
    Output('select_two', 'children'),
    Output('select_two', 'style'),
    Input('period_values_year', 'value'),
    Input('period_values_year', 'options'),
    Input('period_type_others', 'value'),
    Input('period_values_others', 'value'),
    Input('street_name_dd', 'value'),
    Input('date_filter', 'min_date_allowed'),
    Input('date_filter', 'max_date_allowed'),
    Input('language_selector', 'value'),
    Input('toggle_uptime_filter', 'value'),
    Input('toggle_active_filter', 'value'),
    Input('hardware_version', 'value'),
    Input('street_type_dd', 'value'),
)
def comparison_chart(period_values_year, period_options_year, period_type_others,
                     period_values_others, id_street, min_date, max_date, lang_code_dd,
                     toggle_uptime_filter, toggle_active_filter, hardware_version,
                     street_type_dd):
    if not id_street or not min_date or not max_date:
        raise PreventUpdate

    lang = lang_code_dd if lang_code_dd in ('en', 'de') else INITIAL_LANGUAGE
    segment_id = id_street[-11:-1]
    street_name = id_street.split(' (')[0]

    # Exactly two periods are needed; otherwise fall back to comparing the two
    # most recent years that are actually available for this street.
    if not period_values_others or len(period_values_others) != 2:
        select_two_text = _('Select (exactly) two periods to compare:')
        select_two_color = {'color': ADFC_orange}
        period_type_others = 'year'
        period_values_others = list(period_values_year or period_options_year or [])[-2:]
    else:
        select_two_text = _('Select two periods to compare:')
        select_two_color = {'color': 'black'}

    group_key, label_key = COMPARISON_GROUPING.get(period_type_others,
                                                   COMPARISON_GROUPING[DEFAULT_PERIOD_TYPE])
    group_by = resolve_column(TIME_UNIT_COLUMNS, group_key, lang, DEFAULT_TIME_UNIT)
    period_column = resolve_column(TIME_DIVISION_COLUMNS, period_type_others, lang, DEFAULT_PERIOD_TYPE)
    label = _(label_key)

    filter_sql, filter_params = build_filter(
        toggle_uptime_filter, toggle_active_filter, hardware_version, street_type_dd)

    if len(period_values_others) != 2:
        # Nothing sensible to compare - hand back an empty figure rather than
        # raising out of the callback.
        empty = px.line(title=_('Compare traffic periods'))
        empty.update_layout(plot_bgcolor=ADFC_palegrey, paper_bgcolor=ADFC_palegrey)
        return empty, select_two_text, select_two_color

    period_a, period_b = period_values_others
    delta_columns = [f'{col}_d' for col in TRAFFIC_COLUMNS]

    with request_cursor() as cursor:
        materialise_traffic(cursor, filter_sql, filter_params, to_iso(min_date), to_iso(max_date),
                            [0, 24], id_street, street_name, drop_speed=True)

        df_delta = cursor.execute(f"""
            WITH period_a AS (
                SELECT street_selection, {group_by},
                       {aggregate_columns(TRAFFIC_COLUMNS)},
                       MIN(date_local) AS first_seen_a
                FROM traffic WHERE {period_column} = ?
                GROUP BY street_selection, {group_by}
            ),
            period_b AS (
                SELECT street_selection, {group_by},
                       {aggregate_columns(TRAFFIC_COLUMNS, suffix='_d')},
                       MIN(date_local) AS first_seen_b
                FROM traffic WHERE {period_column} = ?
                GROUP BY street_selection, {group_by}
            )
            SELECT * EXCLUDE (first_seen_a, first_seen_b)
            FROM period_b FULL OUTER JOIN period_a USING ({group_by}, street_selection)
            ORDER BY COALESCE(first_seen_a, first_seen_b)
        """, [period_a, period_b]).fetch_df()

    line_avg_delta_traffic = px.line(
        df_delta, x=group_by, y=[*TRAFFIC_COLUMNS, *delta_columns],
        facet_col='street_selection', facet_col_spacing=0.04,
        category_orders={'street_selection': [street_name, ALL_STREETS]},
        labels={group_by: _(TIME_UNIT_LABELS.get(group_key, 'Day'))},
        color_discrete_map={**TRAFFIC_COLOURS,
                            **{f'{col}_d': colour for col, colour in TRAFFIC_COLOURS.items()}})

    for col in delta_columns:
        line_avg_delta_traffic.update_traces(selector={'name': col}, line={'dash': 'dash'})
    rename_traffic_traces(line_avg_delta_traffic, suffix=' A')
    for col in TRAFFIC_COLUMNS:
        line_avg_delta_traffic.update_traces(
            {'name': _(TRAFFIC_TRACE_LABELS[col]) + ' B'}, selector={'name': f'{col}_d'})

    line_avg_delta_traffic.update_layout(
        title_text=f"{_('Period')} A : {label} - {period_a} , {_('Period')} B (----): {label} - {period_b}")
    line_avg_delta_traffic.update_xaxes(dtick=1, tickformat='.0f')
    apply_facet_layout(line_avg_delta_traffic, street_name, segment_id,
                       y_title=_('Absolute traffic count'), legend_title=_('Traffic Type'),
                       independent_x=True)

    return line_avg_delta_traffic, select_two_text, select_two_color


if __name__ == '__main__':
    app.run(debug=False)
