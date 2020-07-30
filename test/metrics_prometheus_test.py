import unittest
import mock
from typing import Optional, Iterable, Dict, Tuple
from urllib.parse import parse_qs
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop
from aiohttp import web
from metrics.common import QueryExecError, Timeseries
from metrics.prometheus import PrometheusQuery


class PrometheusQueryTest(AioHTTPTestCase):

    def get_source_url(self) -> str:
        """method to get prometheus source url"""
        return f'http://{self.server.host}:{self.server.port}'

    def gen_prom_range_response_with_error(self) -> dict:
        """
        helper method to return prometheus range query with error response
        """
        return {
            'status': 'error',
            'errorType': 'bad_data',
            'error': 'error: bad query'
        }

    def gen_prom_range_response_with_empty(self) -> dict:
        """
        helper method to return prometheus range query with empty
        result response
        """
        return {
            'status': 'success',
            'data': {
                'resultType': 'matrix',
                'result': []
            }
        }

    def gen_prom_range_response_with_nodata(self) -> dict:
        """
        helper method to return prometheus range query with no
        data response
        """
        return {
            'status': 'success',
            'data': {}
        }

    def gen_prom_range_response_with_single(self) -> dict:
        """
        helper method to return prometheus range query with single
        metric response
        """
        return {
            'status': 'success',
            'data': {
                'resultType': 'matrix',
                'result': [{
                    'metric': {
                        '__name__': 'example_metric_a',
                        'instance': 'localhost:9090',
                        'job': 'mock_prometheus_server'
                    },
                    'values': [
                        [1595823013.0, "6.0"],
                        [1595823073.0, "7.0"],
                        [1595823133.0, "9.0"],
                        [1595823193.0, "10.0"],
                    ]
                }]
            }
        }

    def gen_prom_range_response_with_multi(self) -> dict:
        """
        helper method to return prometheus range query with single
        metric response
        """
        return {
            'status': 'success',
            'data': {
                'resultType': 'matrix',
                'result': [
                    {
                        'metric': {
                            '__name__': 'example_metric_a',
                            'instance': 'localhost:9090',
                            'job': 'mock_prometheus_server'
                        },
                        'values': [
                            [1595823013.0, "3.0"],
                            [1595823073.0, "4.0"],
                            [1595823133.0, "6.0"],
                            [1595823193.0, "11.0"],
                        ]
                    },
                    {
                        'metric': {
                            '__name__': 'example_metric_b',
                            'instance': 'localhost:9090',
                            'job': 'mock_prometheus_server'
                        },
                        'values': [
                            [1595823013.0, "3.5"],
                            [1595823073.0, "4.5"],
                            [1595823133.0, "6.5"],
                            [1595823193.0, "11.5"],
                        ]
                    },
                ]
            }
        }

    def verify_series(
        self,
        ts: Optional[Timeseries],
        expected_name: str,
        expected_raw_vals: Dict[int, float],
        expected_df_vals: Iterable[Tuple[str, float]],
    ) -> None:
        """helper method to verify single timeseries result"""
        self.assertIsNotNone(ts)
        self.assertEqual(ts.get_name(), expected_name)
        raw_vals = ts.get_raw_vals()
        for key in expected_raw_vals:
            self.assertEqual(expected_raw_vals[key], raw_vals[key])
        df = ts.get_dataframe()
        self.assertIsNotNone(df)
        for i, val in enumerate(df.values):
            self.assertEqual(str(val[0]), expected_df_vals[i][0])
            self.assertEqual(val[1], expected_df_vals[i][1])

    def verify_multi_metric_matches(
        self,
        result: Optional[Iterable[Timeseries]]
    ) -> None:
        """helper method to verify single timeseries result matches"""
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 2)
        ts = result[0]
        expected_name = 'example_metric_a'
        expected_raw_vals = {
            1595823013: 3.0,
            1595823073: 4.0,
            1595823133: 6.0,
            1595823193: 11.0
        }
        expected_df_values = [
            ('2020-07-27 04:10:13', 3.0),
            ('2020-07-27 04:11:13', 4.0),
            ('2020-07-27 04:12:13', 6.0),
            ('2020-07-27 04:13:13', 11.0),
        ]
        self.verify_series(
            ts=ts,
            expected_name=expected_name,
            expected_raw_vals=expected_raw_vals,
            expected_df_vals=expected_df_values
        )
        ts = result[1]
        expected_name = 'example_metric_b'
        expected_raw_vals = {
            1595823013: 3.5,
            1595823073: 4.5,
            1595823133: 6.5,
            1595823193: 11.5
        }
        expected_df_values = [
            ('2020-07-27 04:10:13', 3.5),
            ('2020-07-27 04:11:13', 4.5),
            ('2020-07-27 04:12:13', 6.5),
            ('2020-07-27 04:13:13', 11.5),
        ]
        self.verify_series(
            ts=ts,
            expected_name=expected_name,
            expected_raw_vals=expected_raw_vals,
            expected_df_vals=expected_df_values
        )

    def verify_single_metric_matches(
        self,
        result: Optional[Iterable[Timeseries]]
    ) -> None:
        """helper method to verify single timeseries result matches"""
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 1)
        ts = result[0]
        expected_name = 'example_metric_a'
        expected_raw_vals = {
            1595823013: 6.0,
            1595823073: 7.0,
            1595823133: 9.0,
            1595823193: 10.0
        }
        expected_df_values = [
            ('2020-07-27 04:10:13', 6.0),
            ('2020-07-27 04:11:13', 7.0),
            ('2020-07-27 04:12:13', 9.0),
            ('2020-07-27 04:13:13', 10.0),
        ]
        self.verify_series(
            ts=ts,
            expected_name=expected_name,
            expected_raw_vals=expected_raw_vals,
            expected_df_vals=expected_df_values
        )

    async def get_application(self) -> web.Application:
        """method to setup test server for testing rest client"""
        # setup list of possible queries for test server
        queries = [
            'empty_res',
            'error_res',
            'empty_data',
            'single_data',
            'multi_data'
        ]
        # setup query to response mapping
        query_to_res = {
            'empty_res': self.gen_prom_range_response_with_empty(),
            'error_res': self.gen_prom_range_response_with_error(),
            'empty_data': self.gen_prom_range_response_with_nodata(),
            'single_data': self.gen_prom_range_response_with_single(),
            'multi_data': self.gen_prom_range_response_with_multi(),
        }
        # setup query to expected params mapping
        query_to_params = {
            'empty_res': {
                'start': '1595391193',
                'end': '1595823193',
                'step': '1h',
                'query': 'empty_res'
            },
            'error_res': {
                'start': '1595391193',
                'end': '1595823193',
                'step': '1h',
                'query': 'error_res'
            },
            'empty_data': {
                'start': '1595391193',
                'end': '1595823193',
                'step': '1h',
                'query': 'empty_data'
            },
            'single_data': {
                'start': '1595391193',
                'end': '1595823193',
                'step': '1h',
                'query': 'single_data'
            },
            'multi_data': {
                'start': '1595391193',
                'end': '1595823193',
                'step': '1h',
                'query': 'multi_data'
            },
        }

        async def handle_range_request(request: web.Request) -> web.Response:
            # test server range query request handler
            self.assertEquals(request.method, 'GET')
            params = request.query_string
            # verify params set
            self.assertIsNotNone(params)
            parsed_params = parse_qs(params)
            # verify query param set
            query = parsed_params.get('query', None)
            self.assertIsNotNone(query)
            query = query[0]
            # verify query one of the listed
            self.assertTrue(query in queries)
            # verify params for listed query
            for key in query_to_params[query]:
                self.assertEqual(
                    parsed_params[key][0],
                    query_to_params[query][key]
                )
            # return listed query response
            return web.json_response(data=query_to_res[query])

        # setup test server
        app = web.Application()
        # setup paths
        app.router.add_get('/api/v1/query_range', handle_range_request)
        return app

    def get_prom_query_for_query(self, query: str) -> PrometheusQuery:
        """
        helper method to get PrometheusQuery instance given query
        """
        src = self.get_source_url()
        instances = {
            'empty_res': PrometheusQuery(
                query='empty_res',
                source=src,
                lookback_days=5,
                step='1h'
            ),
            'error_res': PrometheusQuery(
                query='error_res',
                source=src,
                lookback_days=5,
                step='1h'
            ),
            'empty_data': PrometheusQuery(
                query='empty_data',
                source=src,
                lookback_days=5,
                step='1h'
            ),
            'single_data': PrometheusQuery(
                query='single_data',
                source=src,
                lookback_days=5,
                step='1h'
            ),
            'multi_data': PrometheusQuery(
                query='multi_data',
                source=src,
                lookback_days=5,
                step='1h'
            ),
        }
        return instances.get(query, None)

    @mock.patch('time.time', mock.MagicMock(return_value=1595823193))
    @unittest_run_loop
    async def test_query_single_metric(self) -> None:
        """test get result with single metric result"""
        query = self.get_prom_query_for_query('single_data')
        self.assertIsNotNone(query)
        res = await query.execute()
        self.verify_single_metric_matches(res)

    @mock.patch('time.time', mock.MagicMock(return_value=1595823193))
    @unittest_run_loop
    async def test_query_multi_metric(self) -> None:
        """test get result with multiple metric result"""
        query = self.get_prom_query_for_query('multi_data')
        self.assertIsNotNone(query)
        res = await query.execute()
        self.verify_multi_metric_matches(res)

    @mock.patch('time.time', mock.MagicMock(return_value=1595823193))
    @unittest_run_loop
    async def test_query_empty_result(self) -> None:
        """test get result with empty result"""
        query = self.get_prom_query_for_query('empty_res')
        self.assertIsNotNone(query)
        with self.assertRaises(QueryExecError):
            await query.execute()

    @mock.patch('time.time', mock.MagicMock(return_value=1595823193))
    @unittest_run_loop
    async def test_query_error_result(self) -> None:
        """test get result with error result"""
        query = self.get_prom_query_for_query('error_res')
        self.assertIsNotNone(query)
        with self.assertRaises(QueryExecError):
            await query.execute()

    @mock.patch('time.time', mock.MagicMock(return_value=1595823193))
    @unittest_run_loop
    async def test_query_nodata_result(self) -> None:
        """test get result with no data result"""
        query = self.get_prom_query_for_query('empty_data')
        self.assertIsNotNone(query)
        with self.assertRaises(QueryExecError):
            await query.execute()


if __name__ == '__main__':
    unittest.main()
