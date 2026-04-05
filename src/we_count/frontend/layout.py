# Copyright (c) 2024-2025 Berlin zählt Mobilität
# SPDX-License-Identifier: MIT

# @file    layout.py
# @author  Egbert Klaassen
# @author  Michael Behrisch
# @date    2025-12-01

import dash_bootstrap_components as dbc
from dash import Dash, html, dcc
# suppress warnings, see app.py
from typing import Callable

_: Callable[[str], str]


# Initialize constants, variables and get data
ADFC_green = '#1C9873'
ADFC_green_L = '#25C996'
ADFC_palegrey = '#F2F2F2'
ADFC_lightgrey = '#DEDEDE'
ADFC_darkgrey = '#737373'
ADFC_middlegrey = '#A7A7A7'
ADFC_cyan = '#61CBF4'
ADFC_lightblue = '#95CBD8'
ADFC_lightblue_D = '#6DB7C9'
ADFC_skyblue = '#D7EDF2'
ADFC_blue = '#2C4B78'
ADFC_darkblue = '#331F45'
ADFC_orange = '#D78432'
ADFC_crimson = '#B44958'
ADFC_pink = '#EB9AAC'
ADFC_yellow = '#EEDE72'
ADFC_red = '#E07862'
ADFC_orange_L = '#EDC773'

info_icon = html.I(className='bi bi-info-circle-fill me-2')
email_icon = html.I(className='bi bi-envelope-at-fill me-2')
camera_icon = html.I(className='bi bi-camera-fill me-2')
arrow_right_icon = html.I(className='bi bi-arrow-bar-right')

INITIAL_STREET_ID = 'Dresdener Straße (9000006667)'
INITIAL_LANGUAGE = 'de'
INITIAL_HOUR_RANGE = [0, 24]


def serve_layout(app: Dash, id_street_options, start_date, end_date, min_date, max_date):
    return dbc.Container(
        [
        # Navigation bar
        dbc.NavbarSimple(
            children=[
                dbc.NavItem(dbc.NavLink(_("Project partners: "), href="#"), class_name='align-center'),
                html.A(href='https://adfc-tk.de/wir-zaehlen/', target='_blank',
                       children=[
                           html.Img(src=app.get_asset_url('ADFC_logo.png'), title='Allgemeiner Deutscher Fahrrad-Club', height="45px"),
                       ], style={'align-items': 'center', "border-right": "1px solid #ccc", "padding": "0 10px"}),
                html.A(href='https://dlr.de/ts/', target='_blank',
                       children=[
                           html.Img(src=app.get_asset_url('DLR_logo.png'), title='Das Deutsche Zentrum für Luft- und Raumfahrt', height="50px"),
                       ], style={"border-right": "1px solid #ccc", "padding": "0 10px"}),
                html.A(href='https://telraam.net/en/candidates/berlin-zaehlt-mobilitaet/berlin-zaehlt-mobilitaet', target='_blank',
                       children=[
                           html.Img(src=app.get_asset_url('Telraam.png'), title='Telraam - Citizen Science Project', height="40px"),
                       ], style={"border-right": "1px solid #ccc", "padding": "0 10px"}),
                html.A(href='https://codefor.de/projekte/wecount/', target='_blank',
                       children=[
                           html.Img(src=app.get_asset_url('CodeFor-berlin.svg'), title='Code for Berlin', height="50px"),
                       ], style={"padding": "0 15px"}),
            ],
            brand="Berlin zählt Mobilität",
            brand_style={'font-size': 36,'font-weight': 'bold', 'color': ADFC_darkblue, 'font-style': 'italic', 'text-shadow': '3px 2px lightblue', 'text-wrap': 'wrap'},
            color=ADFC_skyblue,
            dark=False,
            expand='lg',
        ),

        # Anchor for language switch
        dbc.Row([
            dcc.Location(id='url', refresh=True),
        ]),
        # Announcement - keep for potential use!
        # dbc.Row([
        #     dbc.Col(
        #         html.H5(_('Notice: we are moving server, until then, the maximum selectable date will be 04-02-2026.'), className='my-2', style={'background-color': ADFC_yellow, 'color': ADFC_crimson})
        #     ),
        # ]),

        dbc.Row([
            # Street Map
            dbc.Col([
                dcc.Loading(id="loading-icon_street_map", children=[html.Div(
                    dcc.Graph(id='street_map', figure={}, config={'ScrollZoom': True, 'displayModeBar': False, 'staticPlot': False, 'responsive': True} , className='bg-#F2F2F2', style={'height': 520}))]),
            ], sm=8),

            # Pie chart and infos
            dbc.Col([
                dbc.Row([
                    dbc.Col([
                        # Map info
                        html.H6(_('Map info'), id='popover_map_info', className='text-start', style={'color': ADFC_darkgrey}),
                        dbc.Popover(dbc.PopoverBody(
                            _('Note: street colors represent bike/car ratios based on all data available and do not change with date- or hour selection. The map allows street segments to be selected by mouse-click. Upon selection, the map will zoom with the selected street in the center.')),
                            target='popover_map_info', trigger='hover', placement='bottom'),
                        # dbc.RadioItems(
                        #     id='toggle_map_style',
                        #     options=[{'label': _('Default'), 'value': 'streets'},
                        #              {'label': _('Satellite'), 'value': 'satellite'},
                        #              {'label': _('OSM'), 'value': 'open-street-map'}],
                        #     value='streets',
                        #     switch=False,
                        #     inline=True,
                        #     style={'color': ADFC_darkgrey}
                        # ),
                    ], sm=8),
                    dbc.Col([
                        # Street drop down
                        dcc.Dropdown(
                            id='language_selector',
                            options=[
                                {'label': '🇬🇧' + ' ' + _('English'), 'value': 'en'},
                                {'label': '🇩🇪' + ' ' + _('Deutsch'), 'value': 'de'},
                            ],
                            value=INITIAL_LANGUAGE,
                            persistence = True,
                            persistence_type = 'local',
                            className='g-0',
                            clearable=False,
                        ),
                    ], sm=4),
                ]),
                html.H4(_('Select street:'), className='my-2'),
                dcc.Dropdown(id='street_name_dd',
                    options= id_street_options,
                    value= INITIAL_STREET_ID,
                    clearable=False,
                ),
                html.Span([
                    html.H4(_('Traffic type - selected street'), id='selected_street_header', style={'color': 'black'}, className='my-2 d-inline-block'),
                    html.I(className='bi bi-info-circle-fill h6 ms-1', id='popover_traffic_type', style={'align': 'top', 'color': ADFC_middlegrey}),
                    dbc.Popover(
                        dbc.PopoverBody(_('Traffic type split of the currently selected street, based on currently selected date and hour range.')),
                    target="popover_traffic_type", trigger="hover")
                ]),
                # Pie chart
                dcc.Loading(id="loading-icon_pie_traffic", children=[html.Div(
                    dcc.Graph(id='pie_traffic', figure={}))], type="default"),
                html.H6(_('Selected segment ID: '), id='street_id_text', className='mt-1 mb-0', style={'color': ADFC_darkgrey}),
                html.H6(_('Number of selected segments: '), id='nof_selected_segments', className='mt-0 mb-0', style={'color': ADFC_darkgrey}),
            ], sm=4),
        ], className= 'g-2 mt-1 mb-3 text-start'),

        ### Filters
        dbc.Row([
            dbc.Col([
                html.H6(_('Map style:'), className='ms-2 fw-bold d-inline'),
                dbc.RadioItems(
                    id='toggle_map_style',
                    options=[{'label': _('Default'), 'value': 'streets'},
                             {'label': _('OSM'), 'value': 'open-street-map'},
                             {'label': _('Carto'), 'value': 'carto-positron'},
                             {'label': _('Satellite'), 'value': 'satellite'}],
                    value='streets',
                    switch=False,
                    inline=True,
                    className='ms-2 d-inline-block',
                ),
            ], sm=5),
            dbc.Col([
                html.H6(_('Filters:'), className='ms-2 fw-bold d-inline'),
                html.Span([
                    dbc.Checklist(
                        id='toggle_uptime_filter',
                        options=[{'label': html.Div([_(' Uptime > 70%'), html.I(className='bi bi-info-circle-fill h6 ms-2', id='popover_filter_uptime', style={'color': ADFC_middlegrey})]),
                                  'value': 'filter_uptime_selected'}],
                        value=['filter_uptime_selected'],
                        inline=False,
                        switch=True,
                        className='ms-2 d-inline-block'
                    ),
                    dbc.Popover(
                        dbc.PopoverBody(
                            _('A high uptime of >70% will always mean very good data. The first and last daylight hour of the day will always have lower uptimes. If uptimes during the day are below 0.5, that is usually a clear sign that something is probably wrong with the sensor.')),
                        target='popover_filter_uptime', trigger="hover"),
                ]),
                html.Span([
                    dbc.Checklist(
                        id='toggle_active_filter',
                        options=[{'label': html.Div([_(' Active only'), html.I(className='bi bi-info-circle-fill h6 ms-2', id='popover_filter_active', style={'color': ADFC_middlegrey})]),
                                  'value': 'filter_active_selected'}],
                        value=['filter_active_selected'],
                        inline=True,
                        switch=True,
                        className='ms-2 d-inline-block'
                    ),
                    dbc.Popover(
                        dbc.PopoverBody(
                            _('Active only means that only cameras that are or have been active during the last 14 days are included. Switching off this feature will include all cameras with data.')),
                        target='popover_filter_active', trigger="hover"),
                ]),
                html.Span([
                    dbc.Checklist(
                        id='hardware_version',
                        options=[{'label': _('V1 Sensor'), 'value': 1}, {'label': html.Div([_('S2 Sensor'), html.I(className='bi bi-info-circle-fill h6 ms-2', id='popover_hardware_version', style={'color': ADFC_middlegrey})]),
                                                                         'value': 2}],
                        value=[1, 2],
                        inline=True,
                        switch=True,
                        className='ms-2 d-inline-block'
                    ),
                    dbc.Popover(
                        dbc.PopoverBody(
                            _("Click to show/hide cameras with hardware versions 1 and or 2. Switching off both, will re-enable both automatically. Note: the 'All streets' graphs below are based on all streets, regardless which camera hardware version is selected")),
                        target="popover_hardware_version", trigger="hover"),
                ]),
            ], sm=7),
            # Horizontal line
            html.Hr(style={
                "border": "none",
                "border-top": "2px solid #53917E",  # custom color
                "margin": "10px 0"
            }),
        ], className='g-2 rounded d-flex align-items-center', style={'background-color': ADFC_skyblue}),

        # Date and hour range selection
        dbc.Row([
            dbc.Col([
                dbc.Row([
                    html.H6(_('Set hour range:'), className='ms-2 fw-bold'),
                ]),
                # Hour slider
                dbc.Row([
                    dcc.RangeSlider(
                    id='range_slider',
                    min=0,
                    max=24,
                    step=1,
                    value = INITIAL_HOUR_RANGE,
                    className='align-items-bottom mb-2',
                    allowCross=False,
                    tooltip={'always_visible': False, 'placement' : 'bottom', 'template': '{value}' + _(" Hour")}),
                ]),
            ], sm=6),
            dbc.Col([
            ], sm=1),
            dbc.Col([
                html.H6(_('Pick date range:'), className='fw-bold text-nowrap', id='date_range_text'),
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
                    minimum_nights=0,
                    updatemode='bothdates',
                    className='align-bottom justify-center mb-2',
                ),
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
                        {'label': _('Month'), 'value': 'year_month'},
                        {'label': _('Week'), 'value': 'year_week'},
                        {'label': _('Day'), 'value': 'date'},
                        {'label': _('Hour'), 'value': 'date_hour'}
                    ],
                    value='date',
                    inline=True,
                    inputStyle={"margin-right": "5px", "margin-left": "20px"},
                ),
            ], sm=8),
            dbc.Col([
                html.Span([
                    html.H6([_('Download graphs   '), info_icon], id='download_html_graphs', className='my-4'),
                    dbc.Popover(
                        dbc.PopoverBody(_('Hover over the top-right of a graph and click the camera symbol to download in png-format')),
                        target="download_html_graphs", trigger="hover")
                ], style={'display': 'inline-block', 'color': ADFC_middlegrey}),
            ], className='text-end', sm=4),
        ], className='g-2 p-1'),
        dbc.Row([
            dbc.Col([
                dcc.Loading(id="loading-icon_line_abs_traffic", children=[html.Div(
                    dcc.Graph(id='line_abs_traffic', figure={}, config={'modeBarButtonsToRemove': ['select2d', 'lasso2d', 'zoomIn2d', 'zoomOut2d', 'autoScale2d']}),)])
            ], sm=12
            ),
        ], className='g-2 p-1'),
        # Average traffic
        dbc.Row([
            dbc.Col([
                # Radio time unit
                html.H4(_('Average traffic'), className='my-3'),

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
                dcc.Loading(id="loading-icon_bar_avg_traffic_hr", children=[html.Div(
                    dcc.Graph(id='bar_avg_traffic_hr', figure={}, config={'modeBarButtonsToRemove': ['select2d', 'lasso2d', 'zoomIn2d', 'zoomOut2d', 'autoScale2d']}),)])
            ], sm=12
            ),
        ], className='g-2 p-1'),
        dbc.Row([
            dbc.Col([
                dcc.Loading(id="loading-icon_bar_avg_traffic", children=[html.Div(
                    dcc.Graph(id='bar_avg_traffic', figure={}, config={'modeBarButtonsToRemove': ['select2d', 'lasso2d', 'zoomIn2d', 'zoomOut2d', 'autoScale2d']}),)])
            ], sm=12
            ),
        ], className='g-2 p-1'),
        dbc.Row([
            dbc.Col([
                html.H4(_('Average car speed % - by time unit'), className='my-3'),
                dcc.Loading(id="loading-icon_bar_perc_speed", children=[html.Div(
                    dcc.Graph(id='bar_perc_speed', figure={}, config={'modeBarButtonsToRemove': ['select2d', 'lasso2d', 'zoomIn2d', 'zoomOut2d', 'autoScale2d']}),)]),
            ], sm=12
            ),
        ], className='g-2 p-1'),
        dbc.Row([
            dbc.Col([
                html.Span([html.H4(_('v85 car speed'), className='my-3 me-2', style={'display': 'inline-block'}),
                           html.I(className='bi bi-info-circle-fill h6', id='popover_v85_speed',
                                  style={'display': 'inline-block', 'color': ADFC_middlegrey})]),
                dbc.Popover(
                    dbc.PopoverBody(
                        _('The V85 is a widely used indicator in the world of mobility and road safety, as it is deemed to be representative of the speed one can reasonably maintain on a road.')),
                    target='popover_v85_speed',
                    trigger='hover'
                ),
                dcc.Loading(id="loading-icon_map_bar_v85", children=[html.Div(
                    dcc.Graph(id='bar_v85', figure={}, config={'modeBarButtonsToRemove': ['select2d', 'lasso2d', 'zoomIn2d', 'zoomOut2d', 'autoScale2d']}),)])
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
                dcc.Loading(id="loading-icon_bar_ranking", children=[html.Div(
                    dcc.Graph(id='bar_ranking', figure={},config={'modeBarButtonsToRemove': ['select2d', 'lasso2d', 'zoomIn2d', 'zoomOut2d', 'autoScale2d']}),)], type="default"),
            ], sm=12
            ),
        ], className='g-2 p-1 mb-3'),


        ### Comparison graph
        # Menu
        dbc.Row([
            # Select year(s)
            dbc.Col([
                html.H6(_('Select year scope:'), className='ms-2 fw-bold header-fixed header-clamp'),
                dcc.Dropdown(
                    id='period_values_year',
                    multi=True,
                    options=['2025', '2026'],
                    className='ms-2 mb-2',
                    clearable=False
                ),
            ], className='col-flex', sm=6), #className='d-inline-block')
            # Select period type
            dbc.Col([
                html.H6(_('Select period type:'), className='ms-2 fw-bold header-fixed header-clamp'),
                dcc.Dropdown(
                    id='period_type_others',
                    options=[
                        {'label': _('Year'), 'value': 'year'},
                        {'label': _('Month'), 'value': _('year_month')},
                        {'label': _('Week'), 'value': 'year_week'},
                        {'label': _('Day'), 'value': 'date'}
                    ],
                    value='year',
                    className='ms-2 mb-2',
                    clearable=False
                ),
            ], className='col-flex', sm=2), #className='d-inline-block'),
            # Select two periods
            dbc.Col([
                html.H6(_('Select two periods to compare:'), id= 'select_two', className='ms-2 fw-bold header-fixed header-clamp'),
                dcc.Dropdown(
                    id='period_values_others',
                    value=['2025', '2026'],
                    multi=True,
                    className='ms-2 mb-2',
                    clearable=False
                ),
            ], className='col-flex',  sm=4), #className='d-inline-block')
        ], className='sticky-top rounded g-2 p-1 align-bottom', style={'background-color': ADFC_lightblue, 'opacity': 1.0}),

    # Comparison graph
        dbc.Row([
        html.Span(
            [html.H4(_('Compare traffic periods'), className='my-3 me-2', style={'display': 'inline-block'}),
             html.I(className='bi bi-info-circle-fill h6', id='compare_traffic_periods',
                    style={'display': 'inline-block', 'color': ADFC_middlegrey})]),
        dbc.Popover(
            dbc.PopoverBody(
                _('This chart allows four period-types to be compared: day, week, month or year. For each of these, two periods can be compared (e.g. month vs. month or day vs. day, etc.). Solid lines represent the first period, dashed lines represent the second selected period. You can select the year range on the left to narrow down the available periods shown on the right. You can select a period type from the dropdown menu in the center. You can choose which periods to compare using the dropdown menu located on the right side. If you select anything other than exactly two periods, the graph will automatically use the year period type and display data for 2025 and 2026.')),
            target='compare_traffic_periods',
            trigger='hover'
        ),
        ], className='g-2 p-1 mb-3'),
        dbc.Row([
            dbc.Col([
                dcc.Loading(id="loading-icon_line_avg_delta_traffic", children=[html.Div(
                    dcc.Graph(id='line_avg_delta_traffic', figure={},
                              config={'modeBarButtonsToRemove': ['zoomIn2d', 'zoomOut2d', 'autoScale2d']}))])
            ], sm=12),
        ], className='g-2 p-1 mb-3'),

        dcc.Store(id='intermediate-value'),

        ### Feedback and contact
        dbc.Row([
            # Section header
            dbc.Row([
                html.H4(_('Feedback and contact'), className='my-2'),
            ]),

            dbc.Row([
                # Links left
                dbc.Col([
                    html.Ul([
                        html.Li([_('More information about the '),
                            html.A('Berlin zählt Mobilität', href='https://adfc-tk.de/wir-zaehlen/', target="_blank"),_(' (BzM) initiative'),],
                           ),
                        html.Li([_('Request a counter at the '),
                                html.A(_('Citizen Science-Projekt'), href="https://telraam.net/en/candidates/berlin-zaehlt-mobilitaet/berlin-zaehlt-mobilitaet", target="_blank"),],
                                ),
                        html.Li([_('Data protection around the '),
                                html.A(_('Telraam camera'), href="https://telraam.net/home/blog/telraam-privacy", target="_blank"),_(' measurements'),],
                                ),
                        html.Li([_('Open data source: '),
                                 html.A('Open Data Berlin', href="https://daten.berlin.de/datensaetze/berlin-zaehlt-mobilitaet", target="_blank")],
                                ),
                    ]),
                ], sm=6),

                # Links right
                dbc.Col([
                    html.Ul([
                        html.Li([_('Dashboard development: ') + 'Egbert Klaassen' + _(' and ') + 'Michael Behrisch']),
                        html.Li([_('For dashboard improvement requests: '),
                                 html.A(_('email us'), href='mailto: kontakt@berlin-zaehlt.de')]),
                    ]),
                ], sm=6),
            ]),
        ], className='rounded text-black g-0 p-1 mb-3', style={'background-color': ADFC_yellow, 'opacity': 1.0}),

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
