from typing import Dict, Optional, Iterable
import abc
import pandas as pd
from utils.tasks import AsyncTask, AsyncExecutionError


class Timeseries(object):
    """
    Timeseries represents historical metric data collected

    Parameters
    ----------
    name: str
        name of the time series metric
    values: Dic[int, float]
        dictionary of historical values with key as the unix timestamp
        of when the metric was collected as an integer and the value as
        the recorded value during the time as a float
   """

    def __init__(self, name: str, values: Dict[int, float]) -> None:
        self._raw_values = values
        self._name = name

    def get_name(self) -> str:
        """method to get name of metric"""
        return self._name

    def get_dataframe(self) -> Optional[pd.DataFrame]:
        """method to get dataframe of the timeseries"""
        if len(self._raw_values) > 0:
            df = pd.DataFrame(self._raw_values.items(), columns=['ds', 'y'])
            df['ds'] = pd.to_datetime(df['ds'], unit='s')
            return df
        return None

    def get_raw_vals(self) -> Dict[int, float]:
        """method to get raw values of the timeseries"""
        return self._raw_values

    @staticmethod
    def from_df(
        name: str,
        df: pd.DataFrame,
        time_col: Optional[str] = 'ds',
        val_col: Optional[str] = 'y',
    ):
        """
        method to generate Timeseries object from dataframe

        Parameters
        ----------
        name: str
            name of the Timeseries
        df: pd.Dataframe
            the dataframe to convert to timeseries
        time_col: str (default: ds)
            the column to fetch the time values from
        val_col: str (default: y)
            the column to fetch metric values from
        """
        dt = pd.Timestamp('1970-01-01')
        delta = pd.Timedelta('1s')
        df['unix'] = (df[time_col] - dt) // delta
        other_df = df.astype({'unix': int, val_col: float})
        raw_vals = dict(zip(other_df['unix'], other_df[val_col]))
        return Timeseries(
            name=name,
            values=raw_vals,
        )


class Query(AsyncTask, metaclass=abc.ABCMeta):
    """
    Query represents object to retrieve Timeseries data for
    a given query from given datasource
    """

    @abc.abstractclassmethod
    async def fetch_result(self) -> Optional[Iterable[Timeseries]]:
        """
        method to fetch result for the query
        """
        pass

    async def execute(self) -> Optional[object]:
        """method to execute async task"""
        return await self.fetch_result()


class QueryExecError(AsyncExecutionError):
    """
    QueryExecError is an AsyncExecutionError that is thrown
    when unable to fetch result for Query

    Parameters
    ----------
    datasource: str
        the datasource where the data was queries from
    query: str
        the query used to fetch data
    error: str
        an error message indicating the issue that occured
    """

    def __init__(
        self,
        datasource: str,
        query: str,
        error: str
    ) -> None:
        msg = f"QueryError -> {error} | Source: {datasource}, Query: {query}"
        self.message = msg
        super().__init__(self.message)
