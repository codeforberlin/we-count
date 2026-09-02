# Copyright (c) 2024-2026 Berlin zählt Mobilität
# SPDX-License-Identifier: MIT

# @file    layout.py
# @author  Egbert Klaassen
# @author  Michael Behrisch
# @date    2026-08-31

"""Dash layout for the Berlin zählt Mobilität dashboard.

The layout is mobile-first: every column declares an ``xs`` width (full width on
phones) and widens at the ``md``/``lg`` breakpoints.  Fixed pixel heights have
been replaced by the ``bzm-*`` CSS classes in ``assets/main.css`` so that graph
sizes can be tuned per device class from one place.
"""

from typing import Callable

import dash_bootstrap_components as dbc
from dash import Dash, html, dcc

# suppress warnings about "_" being undefined, see app.py
_: Callable[[str], str]


# --------------------------------------------------------------------------- #
# Palette
# --------------------------------------------------------------------------- #
ADFC_palegrey = '#F2F2F2'
ADFC_lightgrey = '#DEDEDE'
ADFC_middlegrey = '#A7A7A7'
ADFC_darkgrey = '#737373'
ADFC_green_L = '#25C996'
ADFC_green = '#1C9873'
ADFC_lightblue = '#95CBD8'
ADFC_lightblue_D = '#6DB7C9'
ADFC_cyan = '#61CBF4'
ADFC_skyblue = '#D7EDF2'
ADFC_blue = '#2C4B78'
ADFC_darkblue = '#331F45'
ADFC_yellow = '#EEDE72'
ADFC_orange_L = '#EDC773'
ADFC_orange = '#D78432'
ADFC_crimson = '#B44958'
ADFC_pink = '#EB9AAC'
ADFC_red = '#E07862'

ADFC_divider = '#53917E'


# --------------------------------------------------------------------------- #
# Defaults
# --------------------------------------------------------------------------- #
INITIAL_STREET_ID = 'Dresdener Straße (9000006667)'
INITIAL_LANGUAGE = 'de'
INITIAL_HOUR_RANGE = [0, 24]

TELRAAM_CANDIDATES_URL = ('https://telraam.net/en/candidates/'
                          'berlin-zaehlt-mobilitaet/berlin-zaehlt-mobilitaet')

#: Buttons removed from every analysis graph's mode bar.
_MODEBAR_REMOVE = ['select2d', 'lasso2d', 'zoomIn2d', 'zoomOut2d', 'autoScale2d']

#: Shared config for the analysis graphs.  ``responsive`` lets Plotly follow the
#: CSS-driven container size instead of the height baked into the figure.
GRAPH_CONFIG = {'responsive': True, 'displaylogo': False, 'modeBarButtonsToRemove': _MODEBAR_REMOVE}

#: The comparison graph keeps the box/lasso select buttons.
COMPARE_GRAPH_CONFIG = {'responsive': True, 'displaylogo': False,
                        'modeBarButtonsToRemove': ['zoomIn2d', 'zoomOut2d', 'autoScale2d']}

MAP_CONFIG = {'scrollZoom': True, 'displayModeBar': False, 'staticPlot': False, 'responsive': True}

PIE_CONFIG = {'responsive': True, 'displayModeBar': False}

info_icon = html.I(className='bi bi-info-circle-fill me-2')
email_icon = html.I(className='bi bi-envelope-at-fill me-2')
camera_icon = html.I(className='bi bi-camera-fill me-2')
arrow_right_icon = html.I(className='bi bi-arrow-bar-right')


# --------------------------------------------------------------------------- #
# Small building blocks
# --------------------------------------------------------------------------- #
def _info_icon(target_id):
    """Return the standard grey info icon used as a popover target."""
    return html.I(className='bi bi-info-circle-fill h6 ms-2 align-baseline',
                  id=target_id, style={'color': ADFC_middlegrey})


def _popover(target_id, text, placement='auto'):
    return dbc.Popover(dbc.PopoverBody(text), target=target_id,
                       trigger='hover focus', placement=placement)


def graph_panel(graph_id, config=None, class_name='bzm-graph', loading_type='default'):
    """Wrap a ``dcc.Graph`` in the loading spinner used throughout the page.

    Centralising this keeps spinner type, mode-bar configuration and sizing
    consistent for every chart instead of repeating the same nested markup.
    """
    return dcc.Loading(
        id=f'loading-icon_{graph_id}',
        type=loading_type,
        children=dcc.Graph(
            id=graph_id,
            figure={},
            config=GRAPH_CONFIG if config is None else config,
            className=class_name,
        ),
    )


def graph_row(graph_id, config=None, class_name='bzm-graph', heading=None, heading_extra=None):
    """A full-width row holding one graph, with an optional heading above it."""
    children = []
    if heading is not None:
        children.append(
            html.Span([html.H4(heading, className='my-3 me-2 d-inline-block')] + (heading_extra or []))
        )
    children.append(graph_panel(graph_id, config, class_name))
    return dbc.Row(dbc.Col(children, xs=12), class_name='g-2 p-1')


def labelled_control(label, control, **col_widths):
    """Stack a bold label on top of its control so it survives narrow screens."""
    return dbc.Col(
        [html.H6(label, className='fw-bold mb-1 text-nowrap'), control],
        class_name='bzm-control',
        **col_widths,
    )


def _switch(switch_id, label, value, popover_id, popover_text):
    """A single labelled toggle plus its explanatory popover."""
    return html.Div(
        [
            dbc.Checklist(
                id=switch_id,
                options=[{'label': label, 'value': value}],
                value=[value],
                inline=True,
                switch=True,
                class_name='d-inline-block mb-0',
            ),
            _info_icon(popover_id),
            _popover(popover_id, popover_text),
        ],
        className='d-inline-flex align-items-center me-3',
    )


def _partner_logo(href, src, title, height, last=False):
    style = {'padding': '0 10px', 'display': 'inline-flex', 'alignItems': 'center'}
    if not last:
        style['borderRight'] = '1px solid #ccc'
    return html.A(
        href=href, target='_blank', rel='noopener noreferrer', style=style,
        children=html.Img(src=src, title=title, alt=title, height=height, className='bzm-partner-logo'),
    )


# --------------------------------------------------------------------------- #
# Sections
# --------------------------------------------------------------------------- #
def _navbar(app):
    return dbc.NavbarSimple(
        children=[
            dbc.NavItem(dbc.NavLink(_('Project partners: '), href='#'),
                        class_name='d-none d-lg-block align-self-center'),
            _partner_logo('https://adfc-tk.de/wir-zaehlen/', app.get_asset_url('ADFC_logo.png'),
                          'Allgemeiner Deutscher Fahrrad-Club', '45px'),
            _partner_logo('https://dlr.de/ts/', app.get_asset_url('DLR_logo.png'),
                          'Das Deutsche Zentrum für Luft- und Raumfahrt', '50px'),
            _partner_logo(TELRAAM_CANDIDATES_URL, app.get_asset_url('Telraam.png'),
                          'Telraam - Citizen Science Project', '40px'),
            _partner_logo('https://codefor.de/projekte/wecount/', app.get_asset_url('CodeFor-berlin.svg'),
                          'Code for Berlin', '50px', last=True),
        ],
        brand='Berlin zählt Mobilität',
        brand_style={
            'fontWeight': 'bold',
            'color': ADFC_darkblue,
            'fontStyle': 'italic',
            'textShadow': '3px 2px lightblue',
        },
        color=ADFC_skyblue,
        dark=False,
        expand='lg',
        class_name='bzm-navbar flex-wrap',
    )


def _map_and_summary(id_street_options):
    return dbc.Row(
        [
            # Street map
            dbc.Col(
                dcc.Loading(
                    id='loading-icon_street_map',
                    children=dcc.Graph(id='street_map', figure={}, config=MAP_CONFIG, className='bzm-map'),
                ),
                xs=12, lg=8,
            ),

            # Language, street selection, pie chart and counters
            dbc.Col(
                [
                    dbc.Row(
                        [
                            dbc.Col(
                                [
                                    html.H6(_('Map info'), id='popover_map_info',
                                            className='text-start mb-0 mt-2',
                                            style={'color': ADFC_darkgrey}),
                                    _popover('popover_map_info', _(
                                        'Note: street colors represent bike/car ratios based on all data available '
                                        'and do not change with date- or hour selection. The map allows street '
                                        'segments to be selected by mouse-click. Upon selection, the map will zoom '
                                        'with the selected street in the center.'), placement='bottom'),
                                ],
                                xs=6, sm=8,
                            ),
                            dbc.Col(
                                dcc.Dropdown(
                                    id='language_selector',
                                    options=[
                                        {'label': '🇬🇧 ' + _('English'), 'value': 'en'},
                                        {'label': '🇩🇪 ' + _('Deutsch'), 'value': 'de'},
                                    ],
                                    value=INITIAL_LANGUAGE,
                                    persistence=True,
                                    persistence_type='local',
                                    clearable=False,
                                    searchable=False,
                                ),
                                xs=6, sm=4,
                            ),
                        ],
                        class_name='g-2 align-items-center',
                    ),

                    html.H4(_('Select street:'), className='my-2'),
                    dcc.Dropdown(id='street_name_dd', options=id_street_options,
                                 value=INITIAL_STREET_ID, clearable=False),

                    html.Span(
                        [
                            html.H4(_('Traffic type - selected street'), id='selected_street_header',
                                    style={'color': 'black'}, className='my-2 d-inline-block'),
                            _info_icon('popover_traffic_type'),
                            _popover('popover_traffic_type', _(
                                'Traffic type split of the currently selected street, based on currently '
                                'selected date and hour range.')),
                        ],
                    ),

                    graph_panel('pie_traffic', config=PIE_CONFIG, class_name='bzm-pie'),

                    html.H6(_('Selected segment ID: '), id='street_id_text',
                            className='mt-1 mb-0', style={'color': ADFC_darkgrey}),
                    html.H6(_('Number of selected segments: '), id='nof_selected_segments',
                            className='mt-0 mb-0', style={'color': ADFC_darkgrey}),
                ],
                xs=12, lg=4,
            ),
        ],
        class_name='g-2 mt-1 mb-3 text-start',
    )


def _filter_bar():
    return dbc.Row(
        [
            labelled_control(
                _('Map style:'),
                dcc.Dropdown(
                    id='toggle_map_style',
                    options=[{'label': _('Streets'), 'value': 'streets'},
                             {'label': _('OSM'), 'value': 'open-street-map'},
                             {'label': _('Carto'), 'value': 'carto-positron'},
                             {'label': _('Satellite'), 'value': 'satellite'}],
                    value='streets',
                    clearable=False,
                    searchable=False,
                    className='toggle_map_style',
                ),
                xs=6, md=3, lg=2,
            ),
            labelled_control(
                _('Street type:'),
                dcc.Dropdown(
                    id='street_type_dd',
                    options=[{'label': _('All'), 'value': 'all'},
                             {'label': _('Primary'), 'value': 'primary'},
                             {'label': _('Secondary'), 'value': 'secondary'},
                             {'label': _('Tertiary'), 'value': 'tertiary'},
                             {'label': _('Residential'), 'value': 'residential'}],
                    value='all',
                    clearable=False,
                    searchable=False,
                    className='street_type',
                ),
                xs=6, md=3, lg=2,
            ),
            dbc.Col(
                [
                    html.H6(_('Filters:'), className='fw-bold mb-1'),
                    html.Div(
                        [
                            _switch('toggle_uptime_filter', _(' Uptime > 70%'), 'filter_uptime_selected',
                                    'popover_filter_uptime', _(
                                        'A high uptime of >70% will always mean very good data. The first and last '
                                        'daylight hour of the day will always have lower uptimes. If uptimes during '
                                        'the day are below 0.5, that is usually a clear sign that something is '
                                        'probably wrong with the sensor.')),
                            _switch('toggle_active_filter', _(' Active only'), 'filter_active_selected',
                                    'popover_filter_active', _(
                                        'Active only means that only cameras that are or have been active during the '
                                        'last 14 days are included. Switching off this feature will include all '
                                        'cameras with data.')),
                            html.Div(
                                [
                                    dbc.Checklist(
                                        id='hardware_version',
                                        options=[{'label': _('V1 Sensor'), 'value': 1},
                                                 {'label': _('S2 Sensor'), 'value': 2}],
                                        value=[1, 2],
                                        inline=True,
                                        switch=True,
                                        class_name='d-inline-block mb-0',
                                    ),
                                    _info_icon('popover_hardware_version'),
                                    _popover('popover_hardware_version', _(
                                        "Click to show/hide cameras with hardware versions 1 and or 2. Switching off "
                                        "both, will re-enable both automatically. Note: the 'All streets' graphs "
                                        "below are based on all streets, regardless which camera hardware version "
                                        "is selected")),
                                ],
                                className='d-inline-flex align-items-center me-3',
                            ),
                        ],
                        className='d-flex flex-wrap align-items-center',
                    ),
                ],
                class_name='bzm-control',
                xs=12, md=6, lg=8,
            ),
            html.Hr(style={'border': 'none', 'borderTop': f'2px solid {ADFC_divider}', 'margin': '10px 0'}),
        ],
        class_name='g-2 px-1 rounded align-items-end',
        style={'backgroundColor': ADFC_skyblue},
    )


def _date_and_hour_bar(start_date, end_date, min_date, max_date):
    return dbc.Row(
        [
            dbc.Col(
                [
                    html.H6(_('Set hour range:'), className='fw-bold mb-1'),
                    dcc.RangeSlider(
                        id='range_slider',
                        min=0, max=24, step=1,
                        value=INITIAL_HOUR_RANGE,
                        allowCross=False,
                        className='mb-2',
                        tooltip={'always_visible': False, 'placement': 'bottom',
                                 'template': '{value}' + _(' Hour')},
                    ),
                ],
                class_name='bzm-control',
                xs=12, md=7,
            ),
            dbc.Col(
                [
                    html.H6(_('Pick date range:'), id='date_range_text', className='fw-bold mb-1'),
                    dcc.DatePickerRange(
                        id='date_filter',
                        updatemode='bothdates',
                        start_date=start_date,
                        end_date=end_date,
                        min_date_allowed=min_date,
                        max_date_allowed=max_date,
                        display_format='DD-MM-YYYY',
                        end_date_placeholder_text='DD-MM-YYYY',
                        number_of_months_shown=1,
                        minimum_nights=0,
                        clearable=False,
                        className='bzm-datepicker mb-2',
                    ),
                ],
                class_name='bzm-control',
                xs=12, md=5,
            ),
        ],
        class_name='g-2 px-1 rounded bzm-sticky',
        style={'backgroundColor': ADFC_skyblue},
    )


def _comparison_menu():
    return dbc.Row(
        [
            dbc.Col(
                [
                    dbc.Label(_('Select year scope:'), className='fw-bold mb-1 d-block'),
                    dcc.Dropdown(id='period_values_year', multi=True, options=['2025', '2026'],
                                 clearable=False, searchable=False, className='mb-2',
                                 labels={'select_all': _('Select All'),
                                         'selected_count': '{num_selected} ' + _('selected')}),
                ],
                class_name='bzm-control', xs=12, md=5,
            ),
            dbc.Col(
                [
                    dbc.Label(_('Select period type:'), className='fw-bold mb-1 d-block'),
                    dcc.Dropdown(
                        id='period_type_others',
                        options=[{'label': _('Year'), 'value': 'year'},
                                 {'label': _('Month'), 'value': 'year_month'},
                                 {'label': _('Week'), 'value': 'year_week'},
                                 {'label': _('Day'), 'value': 'date'}],
                        value='year', clearable=False, searchable=False, className='mb-2'),
                ],
                class_name='bzm-control', xs=12, md=3,
            ),
            dbc.Col(
                [
                    dbc.Label(_('Select two periods to compare:'), id='select_two',
                              className='fw-bold mb-1 d-block'),
                    dcc.Dropdown(id='period_values_others', value=['2025', '2026'], multi=True,
                                 clearable=False, searchable=False, className='mb-2'),
                ],
                class_name='bzm-control', xs=12, md=4,
            ),
        ],
        class_name='g-2 px-1 rounded bzm-sticky align-items-end',
        style={'backgroundColor': ADFC_lightblue},
    )


def _footer():
    return [
        dbc.Row(
            [
                dbc.Col(html.H4(_('Feedback and contact'), className='my-2'), xs=12),
                dbc.Col(
                    html.Ul([
                        html.Li([_('More information about the '),
                                 html.A('Berlin zählt Mobilität', href='https://adfc-tk.de/wir-zaehlen/',
                                        target='_blank', rel='noopener noreferrer'),
                                 _(' (BzM) initiative')]),
                        html.Li([_('Request a counter at the '),
                                 html.A(_('Citizen Science-Projekt'), href=TELRAAM_CANDIDATES_URL,
                                        target='_blank', rel='noopener noreferrer')]),
                        html.Li([_('Data protection around the '),
                                 html.A(_('Telraam camera'), href='https://telraam.net/home/blog/telraam-privacy',
                                        target='_blank', rel='noopener noreferrer'),
                                 _(' measurements')]),
                        html.Li([_('Open data source: '),
                                 html.A('Open Data Berlin',
                                        href='https://daten.berlin.de/datensaetze/berlin-zaehlt-mobilitaet',
                                        target='_blank', rel='noopener noreferrer')]),
                    ]),
                    xs=12, md=6,
                ),
                dbc.Col(
                    html.Ul([
                        html.Li(_('Dashboard development: ') + 'Egbert Klaassen' + _(' and ') + 'Michael Behrisch'),
                        html.Li([_('For dashboard improvement requests: '),
                                 html.A(_('email us'), href='mailto:kontakt@berlin-zaehlt.de')]),
                    ]),
                    xs=12, md=6,
                ),
            ],
            class_name='rounded text-black g-0 p-2 mb-3',
            style={'backgroundColor': ADFC_yellow},
        ),
        dbc.Row(
            dbc.Col(
                [
                    html.P(_('Disclaimer'), className='bzm-legal-heading'),
                    html.P(_(
                        'The content published in the offer has been researched with the greatest care. '
                        'Nevertheless, the Berlin Counts Mobility team cannot assume any liability for the '
                        'topicality, correctness or completeness of the information provided. All information is '
                        'provided without guarantee. liability claims against the Berlin zählt Mobilität team or '
                        'its supporting organizations derived from the use of this information are excluded. '
                        'Despite careful control of the content, the Berlin zählt Mobilität team and its supporting '
                        'organizations assume no liability for the content of external links. The operators of the '
                        'linked pages are solely responsible for their content. A constant control of the external '
                        'links is not possible for the provider. If there are indications or knowledge of legal '
                        'violations, the illegal links will be deleted immediately.'), className='bzm-legal-body'),
                    html.P(_('Copyright'), className='bzm-legal-heading'),
                    html.P(_(
                        'The layout and design of the offer as a whole as well as its individual elements are '
                        'protected by copyright. The same applies to the images, graphics and editorial '
                        'contributions used in detail as well as their selection and compilation. Further use and '
                        'reproduction are only permitted for private purposes. No changes may be made to it. '
                        'Public use of the offer may only take place with the consent of the operator.'),
                        className='bzm-legal-body'),
                ],
                xs=12,
            ),
            class_name='g-2 p-1',
        ),
    ]


# --------------------------------------------------------------------------- #
# Page
# --------------------------------------------------------------------------- #
def serve_layout(app: Dash, id_street_options, start_date, end_date, min_date, max_date):
    return dbc.Container(
        [
            _navbar(app),

            # Anchor for the language switch (reloads the page on change)
            dcc.Location(id='url', refresh=True),

            _map_and_summary(id_street_options),
            _filter_bar(),
            _date_and_hour_bar(start_date, end_date, min_date, max_date),

            # ---------------- Absolute traffic ---------------- #
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.H4(_('Absolute traffic'), className='my-3'),
                            dcc.RadioItems(
                                id='radio_time_division',
                                options=[{'label': _('Year'), 'value': 'year'},
                                         {'label': _('Month'), 'value': 'year_month'},
                                         {'label': _('Week'), 'value': 'year_week'},
                                         {'label': _('Day'), 'value': 'date'},
                                         {'label': _('Hour'), 'value': 'date_hour'}],
                                value='date',
                                inline=True,
                                className='bzm-radio',
                            ),
                        ],
                        xs=12, md=8,
                    ),
                    dbc.Col(
                        html.Span(
                            [
                                html.H6([_('Download graphs   '), info_icon], id='download_html_graphs',
                                        className='my-4 d-inline-block'),
                                _popover('download_html_graphs', _(
                                    'Hover over the top-right of a graph and click the camera symbol to download '
                                    'in png-format')),
                            ],
                            style={'color': ADFC_middlegrey},
                        ),
                        class_name='text-md-end', xs=12, md=4,
                    ),
                ],
                class_name='g-2 p-1',
            ),
            graph_row('line_abs_traffic'),

            # ---------------- Average traffic ---------------- #
            dbc.Row(
                dbc.Col(
                    [
                        html.H4(_('Average traffic'), className='my-3'),
                        dcc.RadioItems(
                            id='radio_time_unit',
                            options=[{'label': _('Yearly'), 'value': 'year'},
                                     {'label': _('Monthly'), 'value': 'month'},
                                     {'label': _('Weekly'), 'value': 'weekday'},
                                     {'label': _('Daily'), 'value': 'day'},
                                     {'label': _('Hourly'), 'value': 'hour'}],
                            value='weekday',
                            inline=True,
                            className='bzm-radio',
                        ),
                    ],
                    xs=12, md=8,
                ),
                class_name='g-2 p-1',
            ),
            graph_row('bar_avg_traffic_hr'),
            graph_row('bar_avg_traffic'),

            graph_row('bar_perc_speed', heading=_('Average car speed % - by time unit')),
            graph_row('bar_v85', heading=_('v85 car speed'),
                      heading_extra=[
                          _info_icon('popover_v85_speed'),
                          _popover('popover_v85_speed', _(
                              'The V85 is a widely used indicator in the world of mobility and road safety, as it '
                              'is deemed to be representative of the speed one can reasonably maintain on a road.')),
                      ]),

            # ---------------- Ranking ---------------- #
            dbc.Row(
                dbc.Col(
                    [
                        html.H4(_('Street ranking by traffic type'), className='my-3'),
                        dcc.RadioItems(
                            id='radio_y_axis',
                            options=[{'label': _('Pedestrians'), 'value': 'ped_total'},
                                     {'label': _('Bikes'), 'value': 'bike_total'},
                                     {'label': _('Cars'), 'value': 'car_total'},
                                     {'label': _('Heavy'), 'value': 'heavy_total'}],
                            value='car_total',
                            inline=True,
                            className='bzm-radio',
                        ),
                    ],
                    xs=12,
                ),
                class_name='g-2 p-1',
            ),
            graph_row('bar_ranking', class_name='bzm-graph bzm-graph--tall'),

            # ---------------- Period comparison ---------------- #
            _comparison_menu(),
            graph_row('line_avg_delta_traffic', config=COMPARE_GRAPH_CONFIG,
                      heading=_('Compare traffic periods'),
                      heading_extra=[
                          _info_icon('compare_traffic_periods'),
                          _popover('compare_traffic_periods', _(
                              'This chart allows four period-types to be compared: day, week, month or year. For '
                              'each of these, two periods can be compared (e.g. month vs. month or day vs. day, '
                              'etc.). Solid lines represent the first period, dashed lines represent the second '
                              'selected period. You can select the year range on the left to narrow down the '
                              'available periods shown on the right. You can select a period type from the dropdown '
                              'menu in the center. You can choose which periods to compare using the dropdown menu '
                              'located on the right side. If you select anything other than exactly two periods, '
                              'the graph will automatically use the year period type and display data for 2025 '
                              'and 2026.')),
                      ]),

            dcc.Store(id='intermediate-value'),

            *_footer(),
        ],
        style={'--Dash-Fill-Interactive-Strong': '#0d6efd'},
        fluid=True,
        class_name='dbc bzm-container',
    )
