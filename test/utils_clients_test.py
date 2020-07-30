import unittest
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop
from aiohttp import web
from utils.clients import AsyncRestClient, AsyncRestClientException


class AsyncRestClientTest(AioHTTPTestCase):

    def get_rest_client(self) -> AsyncRestClient:
        """method to get rest client"""
        url = f'http://{self.server.host}:{self.server.port}'
        return AsyncRestClient(base_url=url)

    async def get_application(self) -> web.Application:
        """method to setup test server for testing rest client"""
        # setup test api calls

        async def get_json(request: web.Request) -> web.Response:
            # test simple get with json response
            self.assertEquals(request.method, 'GET')
            return web.json_response(data={
                'name': 'example',
                'age': 32,
            })

        async def get_json_with_params(request: web.Request) -> web.Response:
            # test simple get with json response
            self.assertEquals(request.method, 'GET')
            # expect specific params for request
            self.assertEquals(request.query_string, 'p1=1&p2=example')
            return web.json_response(data={
                'correct': True,
            })

        async def get_text(request: web.Request) -> web.Response:
            # test simple get with text response
            self.assertEquals(request.method, 'GET')
            return web.Response(text='Hello World')

        # setup test server
        app = web.Application()
        # setup paths
        app.router.add_get('/getjson', get_json)
        app.router.add_get('/getjsonparams', get_json_with_params)
        app.router.add_get('/gettext', get_text)
        return app

    @unittest_run_loop
    async def test_get_json(self) -> None:
        """test simple json get request"""
        client = self.get_rest_client()
        res = await client.get('/getjson')
        self.assertEquals(res['name'], 'example')
        self.assertEquals(res['age'], 32)

    @unittest_run_loop
    async def test_get_json_with_params(self) -> None:
        """test simple json get request with params"""
        client = self.get_rest_client()
        res = await client.get(
            uri='/getjsonparams',
            params={
                'p1': 1,
                'p2': 'example'
            }
        )
        self.assertTrue(res['correct'])

    @unittest_run_loop
    async def test_get_json_for_text_response(self) -> None:
        """test simple json get request with params"""
        client = self.get_rest_client()
        with self.assertRaises(AsyncRestClientException):
            await client.get('/gettext')


if __name__ == '__main__':
    unittest.main()
