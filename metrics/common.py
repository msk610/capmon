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
