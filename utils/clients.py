from typing import Optional, Mapping
import aiohttp


class AsyncRestClientException(Exception):
    """
    AsyncRestClientException is thrown when AsyncRestClient
    is unable to successfully make a request to external
    API

    Parameters
    ----------
    base: str
        base url request was made to
    uri: str
        the uri to request was made to
    method: str
        the http method used for request
    error: str
        custom error message for exception
    """

    def __init__(
        self,
        base: str,
        uri: str,
        method: str,
        error: str
    ) -> None:
        msg = (
            f'AsyncRestClientException: {error}. Base URL: {base}. '
            f'URI: {uri}. METHOD: {method}'
        )
        self.message = msg
        super().__init__(self.message)


class AsyncRestClient(object):
    """
    AsyncRestClient helps fetch data from external RESTful
    APIs asynchronously

    Parameters
    ----------
    base_url: str
        base url to make requests to
    """

    def __init__(self, base_url: str) -> None:
        self._base = base_url
        self._client_exceptions = (
            aiohttp.ClientResponseError,
            aiohttp.ClientConnectionError,
            aiohttp.ClientPayloadError,
            aiohttp.ServerTimeoutError,
        )

    async def get(
        self,
        uri: str,
        params: Optional[Mapping[str, str]] = None
    ) -> dict:
        """
        method to make a get request

        Parameters
        ----------
        uri: str
            the uri to make request to
        params: Optional[Mapping[str, str]] (default None)
            the parameters to pass to request

        returns json response
        """
        async with aiohttp.ClientSession() as session:
            try:
                return await self._get(
                    session=session,
                    uri=uri,
                    params=params
                )
            except self._client_exceptions:
                raise AsyncRestClientException(
                    base=self._base,
                    uri=uri,
                    method='GET',
                    error='unable to fetch data',
                )

    async def _get(
        self,
        session: aiohttp.ClientSession,
        uri: str,
        params: Optional[Mapping[str, str]] = None
    ) -> dict:
        """helper method for get method"""
        url = self._base + uri
        async with session.get(url, params=params) as response:
            return await response.json()
