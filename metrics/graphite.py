from typing import Optional, Dict, Iterable
from utils.clients import AsyncRestClient, AsyncRestClientException
from metrics.common import Query, QueryExecError, Timeseries


class GraphiteQuery(Query):
    """
    GraphiteQuery is a Query to fetch Timeseries data from a
    graphite data source

    Parameters
    ----------
    query: str
        query to make to prometheus
    source: Optional[str] (default: http://localhost:8080)
        source to make query to
    lookback_days: Optional[int] (default: 7)
        number of days of data to analyze
    step: Optional[str] (default: 1h)
        resolution to use for data
    """

    def __init__(
        self,
        query: str,
        source: Optional[str] = 'http://localhost:8080',
        lookback_days: Optional[int] = 7,
        step: Optional[str] = '1h'
    ) -> None:
        self._query = query
        self._src = source
        self._step = step
        self._client = AsyncRestClient(base_url=self._src)
        self._range_uri = '/render'
        self._from = f'-{lookback_days}d'

    async def fetch_result(self) -> Optional[Iterable[Timeseries]]:
        """
        method to fetch result for the query
        """
        vals = await self._get_data()
        series = []
        for name in vals:
            series.append(
                Timeseries(name=name, values=vals[name])
            )
        return series

    async def _get_data(self) -> Dict[str, Dict[int, float]]:
        """helper method to get range data from graphite"""
        target = f'summarize({self._query},"{self._step}")'
        params = {
            'target': target,
            'format': 'json',
            'from': self._from,
        }
        try:
            res = await self._client.get(
                uri=self._range_uri,
                params=params
            )
            self._validate_range_result(res)
            data = {}
            for metric in res:
                name = metric['tags']['name']
                if name not in data:
                    data[name] = {}
                for val in metric['datapoints']:
                    if val[0] is not None:
                        data[name][int(val[1])] = float(val[0])
            return data
        except KeyError:
            self._throw_query_error(msg='Got bad data response')
        except ValueError:
            self._throw_query_error(msg='Got bad data response')
        except AsyncRestClientException as e:
            msg = e.get_msg() + ' Unable to fetch data from source'
            self._throw_query_error(msg=msg)

    def _validate_range_result(self, result: Iterable[dict]) -> None:
        """helper method to validate response from prom range data query"""
        if len(result) == 0:
            self._throw_query_error(msg='No results returned')

    def _throw_query_error(self, msg: str) -> None:
        """helper method to raise QueryExecError"""
        raise QueryExecError(
            datasource=self._src,
            query=self._query,
            error=msg
        )
