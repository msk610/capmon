import unittest
from datetime import timedelta
from math import sin, cos
import asyncio
from typing import Iterable
import numpy as np
from metrics.common import Timeseries
from analysis.common import Reporter, Report
from analysis.forecast import FBProphetForecaster


def rmse(
    predictions: Iterable[float],
    targets: Iterable[float],
) -> float:
    """function to generate"""
    return np.sqrt(((predictions - targets) ** 2).mean())


class ForecasterTests(unittest.TestCase):

    def setUp(self) -> None:
        """method executed before every test"""
        self.now = 1595823193
        self.days_forecast = 7
        self.series = [
            self.gen_abs_sin_series(),
            self.gen_abs_cos_series()
        ]
        self.error_threshold = 0.5

    def tearDown(self) -> None:
        """method executed after every test"""
        pass

    def gen_abs_sin_series(self) -> Timeseries:
        """
        helper method to generate a timeseries of sin function.
        it takes absolute value of the sin(x) function.
        this is to generate some data for forecasters
        """
        delta = int(timedelta(days=30).total_seconds())
        start = int(self.now - delta)
        interval = int(timedelta(hours=1).total_seconds())
        counter = 0
        data = {}
        for ts in range(start, self.now + 1, interval):
            data[ts] = abs(sin(counter))
            counter += 1
        return Timeseries(
            name='sin',
            values=data
        )

    def calculate_rmse_sin_series(
        self,
        sin_forecast: Timeseries
    ) -> float:
        """
        helper method to calculate rmse for sin series
        against the forecasted data
        """
        self.assertEqual(sin_forecast.get_name(), 'sin_forecast')
        predictions = []
        actual = []
        total = timedelta(days=30).total_seconds()
        current = int(total / timedelta(hours=1).total_seconds()) + 1
        vals = sin_forecast.get_raw_vals()
        for val in vals.values():
            actual.append(abs(sin(current)))
            predictions.append(val)
            current += 1
        predictions = np.array(predictions)
        actual = np.array(actual)
        return np.sqrt(((predictions - actual) ** 2).mean())

    def calculate_rmse_cos_series(
        self,
        cos_forecast: Timeseries
    ) -> float:
        """
        helper method to calculate rmse for cos series
        against the forecasted data
        """
        self.assertEqual(cos_forecast.get_name(), 'cos_forecast')
        predictions = []
        actual = []
        total = timedelta(days=30).total_seconds()
        current = int(total / timedelta(hours=1).total_seconds()) + 1
        vals = cos_forecast.get_raw_vals()
        for val in vals.values():
            actual.append(abs(cos(current)))
            predictions.append(val)
            current += 1
        predictions = np.array(predictions)
        actual = np.array(actual)
        return np.sqrt(((predictions - actual) ** 2).mean())

    def gen_abs_cos_series(self) -> Timeseries:
        """
        helper method to generate a timeseries of cos function.
        it takes absolute value of the cos(x) function.
        this is to generate some data for forecasters
        """
        delta = int(timedelta(days=30).total_seconds())
        start = int(self.now - delta)
        interval = int(timedelta(hours=1).total_seconds())
        counter = 0
        data = {}
        for ts in range(start, self.now + 1, interval):
            data[ts] = abs(cos(counter))
            counter += 1
        return Timeseries(
            name='cos',
            values=data
        )

    def verify_proper_forecast_series(self, forecasts: Iterable[Timeseries]):
        """helper method to verify proper future time values returned"""
        self.assertIsNotNone(forecasts)
        self.assertEqual(len(forecasts), 2)
        sec_in_days = int(timedelta(days=self.days_forecast).total_seconds())
        delta = int(timedelta(hours=1).total_seconds())
        dp_count = int(sec_in_days / delta)
        for forecast in forecasts:
            start = self.now + delta
            raw_vals = forecast.get_raw_vals()
            self.assertIsNotNone(raw_vals)
            self.assertEqual(len(raw_vals), dp_count)
            for t_val in raw_vals:
                self.assertEqual(t_val, start)
                start += delta

    def verify_report_data(self, report: Report):
        """helper method to verify proper report data returned"""
        # check report high level data
        self.assertTrue(report.contains_forecasts())
        self.assertTrue(report.contains_daily_trends())
        self.assertTrue(report.contains_hourly_trends())
        # check forecast data
        forecasts = report.get_forecasts()
        self.assertIsNotNone(forecasts)
        self.verify_proper_forecast_series(forecasts=forecasts)
        # check sin data
        sin_forecast = forecasts[0]
        self.assertIsNotNone(sin_forecast)
        err = self.calculate_rmse_sin_series(sin_forecast=sin_forecast)
        self.assertTrue(err < self.error_threshold)
        # check cos data
        cos_forecast = forecasts[1]
        self.assertIsNotNone(cos_forecast)
        err = self.calculate_rmse_cos_series(cos_forecast=cos_forecast)
        self.assertTrue(err < self.error_threshold)

    def gen_report_from_forecaster(
        self,
        reporter: Reporter
    ) -> Report:
        """
        helper method to generate report synchronously from
        a given reporter
        """
        return asyncio.run(reporter.report())

    def test_fbprophet_forecast(self) -> None:
        """
        method to test forecasting for FBProphetForecaster
        """
        # setup forecaster
        forecaster = FBProphetForecaster(
            series=self.series,
            forecast_days=self.days_forecast,
        )
        # generate report
        report = self.gen_report_from_forecaster(forecaster)
        # verify report data
        self.verify_report_data(report=report)


if __name__ == '__main__':
    unittest.main()
