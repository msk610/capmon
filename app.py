from typing import Optional, Tuple
import dash
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
from dash.dash import no_update
from structlog import get_logger
from config import Config
from utils.tasks import AsyncExecutionError
from helpers import (
    is_valid_data,
    get_current_data,
    generate_analysis_report,
    gen_forecast_graph_figure,
    gen_weekly_trend_graph_figure,
    gen_daily_trend_graph_figure
)

# setup app
app = dash.Dash(
    external_stylesheets=[dbc.themes.BOOTSTRAP]
)
app.title = 'Capmon'
# setup server
server = app.server
# load configuration
conf = Config()
# setup logger
logger = get_logger()

# setup app layout
app.layout = html.Div([
    # navbar #
    dbc.NavbarSimple(
        brand='Capmon',
        brand_href='#',
        color='dark',
        dark=True,
    ),
    dbc.Row([
        # start of form #
        # =================================================== #
        dbc.Col(
            dbc.FormGroup([
                html.Br(),
                # text box to get user query
                dbc.Label('Query:'),
                dbc.Input(
                    id='query-input',
                    placeholder='Type query here..',
                ),
                html.Br(),
                dbc.Label('Datasource:'),
                dcc.Dropdown(
                    id='dropdown',
                    options=conf.gen_source_options(),
                ),
                html.Br(),
                dbc.Label('Days of metrics to analyze:'),
                dcc.Slider(
                    min=7,
                    max=30,
                    step=None,
                    marks={
                        7: '7 ',
                        14: '14 ',
                        21: '21 ',
                        30: '30 ',
                    },
                    value=7,
                    id='lookback-slider',
                ),
                html.Br(),
                dbc.Label('Days of metrics to forecast:'),
                dcc.Slider(
                    min=7,
                    max=30,
                    step=None,
                    marks={
                        7: '7 ',
                        14: '14 ',
                        21: '21 ',
                        30: '30 ',
                    },
                    value=7,
                    id='forecast-slider',
                ),
                html.Br(),
                dbc.FormText(
                    'Scroll to see trends',
                    color='secondary'
                ),
                html.Br(),
                dbc.Spinner([
                    dbc.Button('Analyze', color='dark', id='submit-query'),
                    dbc.FormText('', color='secondary'),
                    html.Div(id='loading-output-1'),
                ]),
            ]),
            width={'size': 2, 'offset': 1},
            style={
                'float': 'left',
            }
        ),
        # end of form #
        # =================================================== #
        # #
        # start of graphs #
        # =================================================== #
        dbc.Col(
            # start of forecast graph #
            # =================================================== #
            dbc.Row([
                dcc.Graph(
                    id='forecast-graph',
                    figure={
                        'data': [
                            {'x': [], 'y': [], 'type': 'line', 'name': 'data'},
                        ],
                        'layout': {
                            'title': 'Forecast'
                        }
                    },
                    style={'height': '60%', 'width': '90%'}
                ),
                # end of forecast graph #
                # =================================================== #
                dbc.Row([
                    # start of weekly trend graph #
                    # =================================================== #
                    dbc.Col(
                        dcc.Graph(
                            id='weekly-graph',
                            figure={
                                'data': [
                                    {
                                        'x': [],
                                        'y': [],
                                        'type': 'line',
                                        'name': 'data'
                                    },
                                ],
                                'layout': {
                                    'title': 'Weekly Trends'
                                }
                            }),
                    ),
                    # end of weekly trend graph #
                    # =================================================== #
                    # #
                    # start of daily trend graph #
                    # =================================================== #
                    dbc.Col(
                        dcc.Graph(
                            id='daily-graph',
                            figure={
                                'data': [
                                    {
                                        'x': [],
                                        'y': [],
                                        'type': 'line',
                                        'name': 'data'
                                    },
                                ],
                                'layout': {
                                    'title': 'Daily Trends'
                                }
                            }),
                    ),
                    # end of daily trend graph #
                    # =================================================== #
                ])
            ]),
            width=9,
            style={
                'maxHeight': 600,
                'overflow-y': 'scroll',
            }
        ),
        # end of graphs #
        # =================================================== #
    ]),
])


def handle_query_error(
    message: str
) -> Tuple[
    object,
    object,
    object,
    dbc.Alert
]:
    """
    function to show clients errors for queries

    Parameters
    ----------
    message: str
        message is error message to show clients
    """
    return (
        no_update,
        no_update,
        no_update,
        dbc.Alert(
            'Error: ' + message,
            color='danger',
            fade=True,
            dismissable=True,
        )
    )


@app.callback(
    [
        Output('forecast-graph', 'figure'),
        Output('weekly-graph', 'figure'),
        Output('daily-graph', 'figure'),
        Output("loading-output-1", "children"),
    ],
    [
        Input('submit-query', 'n_clicks'),
        Input('dropdown', 'value'),
        Input('query-input', 'value'),
        Input('lookback-slider', 'value'),
        Input('forecast-slider', 'value')
    ]
)
def handle_query(
    clicks: Optional[int],
    source: Optional[str],
    query: Optional[str],
    lookback_days: int,
    forecast_days: int
) -> Tuple[
    object,
    object,
    object,
    dbc.Alert
]:
    """
    main function to handle user queries to the application

    Parameters
    ----------
    clicks: Optional[int] (default: None)
        clicks is the number of times analysis button was clicked
    source: Optional[str] (default: None)
        selected datasource
    query: Optional[str] (default: None)
        query to send to datasource
    lookback_days: int
        number of days of data to analyze
    forecast_days: int
        number of days to forecast for
    """
    # initial load will cause this to be none
    if clicks is None:
        raise dash.exceptions.PreventUpdate('no update necessary')
    # setup logger
    bound_logger = logger.bind(
        query=query,
        source_name=source,
        lookback_days=lookback_days,
        forecast_days=forecast_days,
    )
    bound_logger.info('recieved analysis query for capmon')
    # validate input
    valid, input_error = is_valid_data(
        source=source,
        query=query
    )
    if not valid:
        return handle_query_error(
            message=input_error
        )
    try:
        bound_logger.info('fetching query data')
        # get current data
        series = get_current_data(
            conf=conf,
            source_name=source,
            query=query,
            lookback_days=lookback_days,
        )
        bound_logger.info('running analysis for data')
        # generate forecast and trends
        report = generate_analysis_report(
            series=series,
            forecast_days=forecast_days,
        )
        bound_logger.info('setting up graphs')
        # setup graphs
        forecast_graph = gen_forecast_graph_figure(
            series=series,
            report=report,
        )
        if forecast_graph is None:
            forecast_graph = no_update
        weekly_graph = gen_weekly_trend_graph_figure(report=report)
        if weekly_graph is None:
            weekly_graph = no_update
        daily_graph = gen_daily_trend_graph_figure(report=report)
        if daily_graph is None:
            daily_graph = no_update
        return (
            forecast_graph,
            weekly_graph,
            daily_graph,
            dbc.Alert(
                'Finished analysis',
                color="success",
                fade=True,
                dismissable=True,
            )
        )
    except AsyncExecutionError as err:
        bound_logger.error(err.get_message())
        return handle_query_error(
            message=err.get_message()
        )


if __name__ == "__main__":
    logger.info(
        'starting application',
        host=conf.get_host(),
        debug=conf.get_debug(),
        conf_path=conf.get_conf_path(),
        port=conf.get_port(),
        workers=conf.get_workers(),
    )
    app.run_server(
        host=conf.get_host(),
        debug=conf.get_debug(),
        port=conf.get_port(),
    )
