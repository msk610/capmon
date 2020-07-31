import abc
from typing import Iterable, Optional, Tuple
from datetime import timedelta
from fbprophet import Prophet
import pandas as pd
from metrics.common import Timeseries
from analysis.common import (
    Reporter,
    Report,
    ReporterError,
    Trend
)


class Forecaster(Reporter, metaclass=abc.ABCMeta):
    """
    Forecaster analyzes Timeseries data and generates
    forecasts and trend analysis
    """

    async def report(self) -> Optional[Report]:
        """
        method generate report
        """
        return await self.forecast()

    @abc.abstractclassmethod
    async def forecast(self) -> Optional[Report]:
        """
        method to fetch result for the query
        """
        pass


class FBProphetForecaster(Forecaster):
    """
    ProphetForecaster uses fbprophet library to forecast
    and analyze Timeseries data
    """

    def __init__(
        self,
        series: Iterable[Timeseries],
        forecast_days: Optional[int] = 7,
    ) -> None:
        self._series = series
        delta = timedelta(days=forecast_days).total_seconds()
        self._periods = int(delta / 3600)

    async def forecast(self) -> Optional[Report]:
        """
        method to fetch result for the query
        """
        try:
            return await self._analyze()
        except Exception as e:
            raise ReporterError(
                reporter='FBProphetForecaster',
                error=str(e)
            )

    async def _analyze(self) -> Report:
        """helper method to perform analysis"""
        forecasts = []
        daily = []
        hourly = []
        for data in self._series:
            model = await self._build_model(data=data)
            future = await self._forecast_single(model=model)
            h, d = await self._process_trends_single(future=future)
            forecasts.append(Timeseries.from_df(
                name=data.get_name() + '_forecast',
                df=future,
                time_col='ds',
                val_col='yhat',
            ))
            daily.append(d)
            hourly.append(h)
        daily_agg = pd.concat(daily).groupby(level=0).mean()
        hourly_agg = pd.concat(hourly).groupby(level=0).mean()
        return Report(
            forecasts=forecasts,
            daily_trend=Trend(trend_vals=daily_agg.to_dict()),
            hourly_trend=Trend(trend_vals=hourly_agg.to_dict()),
        )

    async def _build_model(
        self,
        data: Timeseries
    ) -> Prophet:
        """helper method to build model for single metric"""
        model = Prophet()
        model.fit(data.get_dataframe())
        return model

    async def _forecast_single(
        self,
        model: Prophet
    ) -> pd.DataFrame:
        """helper method to build model and forecast for single metric"""
        future = model.make_future_dataframe(self._periods, 'H', False)
        return model.predict(future)

    async def _process_trends_single(
        self,
        future: pd.DataFrame
    ) -> Tuple[pd.Series, pd.Series]:
        """helper method to process trend data from single forecast"""
        daily = future.groupby(future['ds'].dt.day_name())['trend'].agg('sum')
        hourly = future.groupby(future['ds'].dt.hour)['trend'].agg('sum')
        return (hourly, daily)
