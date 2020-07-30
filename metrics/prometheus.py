import time
from typing import Optional, Dict, Iterable
from utils.clients import AsyncRestClient, AsyncRestClientException
from metrics.common import Query, QueryExecError, Timeseries


class PrometheusQuery(Query):
    """
    PrometheusQuery is a Query to fetch Timeseries data from a
    prometheus data source
    """

    def __init__(
        self,
        query: str,
        source: Optional[str] = 'http://localhost:9090',
        lookback_days: Optional[int] = 7,
        step: Optional[str] = '1h'
    ) -> None:
        self._query = query
        self._days = lookback_days
        self._src = source
        self._step = step
        self._client = AsyncRestClient(base_url=self._src)
        self._range_uri = '/api/v1/query_range'

    async def fetch_result(self) -> Optional[Iterable[Timeseries]]:
        """
        method to fetch result for the query
        """
        end = int(time.time())
        start = end - (86400 * self._days)
        vals = await self._get_range_data(
            start=start,
            end=end,
        )
        series = []
        for name in vals:
            series.append(
                Timeseries(name=name, values=vals[name])
            )
        return series

    async def _get_range_data(
        self,
        start: int,
        end: int,
    ) -> Dict[str, Dict[int, float]]:
        """helper method to get range data from prometheus"""
        params = {
            'start': start,
            'end': end,
            'step': self._step,
            'query': self._query
        }
        try:
            res = await self._client.get(
                uri=self._range_uri,
                params=params
            )
            self._validate_range_result(res)
            data = {}
            for metric in res['data']['result']:
                name = metric['metric']['__name__']
                if name not in data:
                    data[name] = {}
                for val in metric['values']:
                    data[name][int(val[0])] = float(val[1])
            return data
        except AsyncRestClientException as e:
            msg = e.get_msg() + ' Unable to fetch data from source'
            self._throw_query_error(msg=msg)

    def _validate_range_result(self, result: dict) -> None:
        """helper method to validate response from prom range data query"""
        status = result.get('status', None)
        if status is None or status != 'success':
            self._throw_query_error(msg='Unable to make successful query')
        data = result.get('data', None)
        if data is None:
            self._throw_query_error(msg='Unable to parse result from source')
        data_res = data.get('result', None)
        if data_res is None or len(data_res) == 0:
            self._throw_query_error(msg='No results returned')

    def _throw_query_error(self, msg: str) -> None:
        """helper method to raise QueryExecError"""
        raise QueryExecError(
            datasource=self._src,
            query=self._query,
            error=msg
        )
