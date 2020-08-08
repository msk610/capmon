import unittest
from typing import Optional, Iterable, Dict, Tuple
from urllib.parse import parse_qs
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop
from aiohttp import web
from metrics.common import QueryExecError, Timeseries
from metrics.graphite import GraphiteQuery


class GraphiteQueryTest(AioHTTPTestCase):

    def get_source_url(self) -> str:
        """method to get graphite source url"""
        return f'http://{self.server.host}:{self.server.port}'

    def gen_response_with_empty(self) -> dict:
        """
        helper method to return graphite query with empty
        result response
        """
        return []

    def gen_response_with_single(self) -> dict:
        """
        helper method to return graphite query with single
        metric response
        """
        return [
            {
                'target': 'example_metric_a',
                'tags': {
                    'name': 'example_metric_a',
                },
                'datapoints': [
                    [6.0, 1595823013],
                    [7.0, 1595823073],
                    [9.0, 1595823133],
                    [10.0, 1595823193],
                    [None, 1595823293],
                ]
            }
        ]

    def gen_response_with_multi(self) -> dict:
        """
        helper method to return graphite query with single
        metric response
        """
        return [
            {
                'target': 'example_metric_a',
                'tags': {
                    'name': 'example_metric_a',
                },
                'datapoints': [
                    [3.0, 1595823013],
                    [4.0, 1595823073],
                    [6.0, 1595823133],
                    [11.0, 1595823193],
                    [None, 1595823293],
                ]
            },
            {
                'target': 'example_metric_b',
                'tags': {
                    'name': 'example_metric_b',
                },
                'datapoints': [
                    [3.5, 1595823013],
                    [4.5, 1595823073],
                    [6.5, 1595823133],
                    [11.5, 1595823193],
                    [None, 1595823293],
                ]
            }
        ]

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
            'summarize(empty.res,"1h")',
            'summarize(single.data,"1h")',
            'summarize(multi.data,"1h")',
        ]
        # setup query to response mapping
        query_to_res = {
            'summarize(empty.res,"1h")': self.gen_response_with_empty(),
            'summarize(single.data,"1h")': self.gen_response_with_single(),
            'summarize(multi.data,"1h")': self.gen_response_with_multi(),
        }
        # setup query to expected params mapping
        query_to_params = {
            'summarize(empty.res,"1h")': {
                'target': 'summarize(empty.res,"1h")',
                'format': 'json',
                'from': '-5d',
            },
            'summarize(single.data,"1h")': {
                'target': 'summarize(single.data,"1h")',
                'format': 'json',
                'from': '-5d',
            },
            'summarize(multi.data,"1h")': {
                'target': 'summarize(multi.data,"1h")',
                'format': 'json',
                'from': '-5d',
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
            query = parsed_params.get('target', None)
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
        app.router.add_get('/render', handle_range_request)
        return app

    def get_query_for_query(self, query: str) -> GraphiteQuery:
        """
        helper method to get GraphiteQuery instance given query
        """
        src = self.get_source_url()
        instances = {
            'empty.res': GraphiteQuery(
                query='empty.res',
                source=src,
                lookback_days=5,
                step='1h'
            ),
            'single.data': GraphiteQuery(
                query='single.data',
                source=src,
                lookback_days=5,
                step='1h'
            ),
            'multi.data': GraphiteQuery(
                query='multi.data',
                source=src,
                lookback_days=5,
                step='1h'
            ),
        }
        return instances.get(query, None)

    @unittest_run_loop
    async def test_query_single_metric(self) -> None:
        """test get result with single metric result"""
        query = self.get_query_for_query('single.data')
        self.assertIsNotNone(query)
        res = await query.execute()
        self.verify_single_metric_matches(res)

    @unittest_run_loop
    async def test_query_multi_metric(self) -> None:
        """test get result with multiple metric result"""
        query = self.get_query_for_query('multi.data')
        self.assertIsNotNone(query)
        res = await query.execute()
        self.verify_multi_metric_matches(res)

    @unittest_run_loop
    async def test_query_empty_result(self) -> None:
        """test get result with empty result"""
        query = self.get_query_for_query('empty.res')
        self.assertIsNotNone(query)
        with self.assertRaises(QueryExecError):
            await query.execute()


if __name__ == '__main__':
    unittest.main()
