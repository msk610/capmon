from typing import Iterable, Tuple
from metrics.common import Timeseries
from analysis.common import Report
from analysis.forecast import FBProphetForecaster
from config import Config


def is_valid_data(
    source: str,
    query: str,
) -> Tuple[bool, str]:
    """
    function to check if entered data can be processed

    Parameters
    ----------
    source: str
        selected datasource
    query: str
        query to send to datasource
    """
    valid_src = bool(source and source.strip())
    if not valid_src:
        return (False, 'no datasource selected')
    valid_query = bool(query and query.strip())
    if not valid_query:
        return (False, 'no query provided')
    return (True, None)


def get_current_data(
    conf: Config,
    source_name: str,
    query: str,
    lookback_days: int,
) -> Iterable[Timeseries]:
    """
    function to fetch lookback data from given datasource
    and query

    Parameters
    ----------
    config: Config
        config object for the application
    source: str
        selected datasource
    query: str
        query to send to datasource
    lookback_days: int
        number of days of data to analyze
    """
    source = conf.get_datasource(name=source_name)
    query = source.get_query_for_src(
        query=query,
        lookback_days=lookback_days
    )
    return query.execute_sync()


def generate_analysis_report(
    series: Iterable[Timeseries],
    forecast_days: int,
) -> Report:
    """
    function to generate forecasting and trend analysis
    reporting for given Timeseries data

    Parameters
    ----------
    series: Iterable[Timeseries]
        list of timeseries data to analyze
    forecast_days: int
        number of days to forecast for
    """
    reporter = FBProphetForecaster(
        series=series,
        forecast_days=forecast_days
    )
    return reporter.execute_sync()


def gen_forecast_graph_figure(
    series: Iterable[Timeseries],
    report: Report
) -> dict:
    """
    function to setup figure of graph to render
    forecasting data

    Parameters
    ----------
    series: Iterable[Timeseries]
        list of timeseries data to analyze
    report: Report
        analysis report object for the data
    """
    data = []
    all_series = []
    if not report.contains_forecasts():
        return None
    all_series.extend(report.get_forecasts())
    all_series.extend(series)
    for single in all_series:
        df = single.get_dataframe()
        if df is not None:
            line = {
                'x': df['ds'],
                'y': df['y'],
                'type': 'line',
                'name': single.get_name()
            }
            data.append(line)
    return {
        'data': data,
        'layout': {
            'title': 'Forecast',
            'xaxis': {
                'title': {
                    'text': 'Dates'
                }
            }
        }
    }


def gen_weekly_trend_graph_figure(
    report: Report
) -> dict:
    """
    function to setup figure of graph to render
    weekly trend data

    Parameters
    ----------
    series: Iterable[Timeseries]
        list of timeseries data to analyze
    report: Report
        analysis report object for the data
    """
    if not report.contains_daily_trends():
        return None
    trend = report.get_daily_trends()[0]
    trend_vals = trend.get_trend_vals()
    keys = [
        'Monday',
        'Tuesday',
        'Wednesday',
        'Thursday',
        'Friday',
        'Saturday',
        'Sunday'
    ]
    vals = []
    for key in keys:
        vals.append(trend_vals[key])
    data = [
        {
            'x': keys,
            'y': vals,
            'type': 'line',
            'name': 'hourly_trend'
        }
    ]
    return {
        'data': data,
        'layout': {
            'title': 'Weekly Trends',
            'xaxis': {
                'title': {
                    'text': 'Days of Week'
                }
            }
        }
    }


def gen_daily_trend_graph_figure(
    report: Report
) -> dict:
    """
    function to setup figure of graph to render
    weekly trend data

    Parameters
    ----------
    series: Iterable[Timeseries]
        list of timeseries data to analyze
    report: Report
        analysis report object for the data
    """
    if not report.contains_hourly_trends():
        return None
    trend = report.get_hourly_trends()
    trend_vals = trend.get_trend_vals()
    keys = []
    for i in range(24):
        if i < 10:
            keys.append(f'0{i}:00')
        else:
            keys.append(f'{i}:00')
    vals = []
    for i in range(24):
        vals.append(trend_vals[i])
    data = [
        {
            'x': keys,
            'y': vals,
            'type': 'line',
            'name': 'hourly_trend'
        }
    ]
    return {
        'data': data,
        'layout': {
            'title': 'Daily Trends',
            'xaxis': {
                'title': {
                    'text': 'Hours of Day'
                }
            }
        }
    }
