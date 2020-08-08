from enum import Enum
from distutils.util import strtobool
import os
from typing import Dict, Iterable
import yaml
from metrics.common import Query
from metrics.prometheus import PrometheusQuery
from metrics.graphite import GraphiteQuery


class InvalidConfigError(Exception):
    """
    InvalidConfigError is thrown when malformed or invalid
    config is provided
    """


class DatasourceType(Enum):
    """
    DatasourceType is type of supported datasource for the
    application
    """
    # PROMETHEUS is prometheus data source
    PROMETHEUS = 'prometheus'
    GRAPHITE = 'graphite'

    @staticmethod
    def from_str(src_type: str):
        """
        method to get DatasourceType given string

        Parameters
        ----------
        src_type: str
            src_type is source type string value provided
        """
        for sourcetype in DatasourceType:
            if sourcetype.value == src_type:
                return sourcetype
        raise InvalidConfigError('unsupported datasource type')


class Datasource(object):
    """
    Datasource defines a data source/database to fetch
    data from for analysis
    """

    def __init__(
        self,
        name: str,
        source: str,
        source_type: DatasourceType,
    ) -> None:
        self._name = name
        self._source = source
        self._type = source_type

    def get_type(self) -> DatasourceType:
        """method to get datasource type"""
        return self._type

    def get_query_for_src(
        self,
        query: str,
        lookback_days: int,
    ) -> Query:
        """method to get Query object for source"""
        if self._type == DatasourceType.PROMETHEUS:
            return PrometheusQuery(
                query=query,
                source=self._source,
                lookback_days=lookback_days
            )
        else:
            return GraphiteQuery(
                query=query,
                source=self._source,
                lookback_days=lookback_days
            )


class Config(object):
    """
    Config contains configuration for the application
    """

    def __init__(self) -> None:
        self._load_settings()
        self._mapping = self._load_config()

    def get_datasource(self, name: str) -> Datasource:
        """method to get datasource by name"""
        return self._mapping[name]

    def get_debug(self) -> bool:
        """method to check if debug is enabled"""
        return self._debug

    def get_port(self) -> int:
        """method to get port to run in"""
        return self._port

    def get_host(self) -> str:
        """method to get host"""
        return self._host

    def get_workers(self) -> int:
        """method to get worker count"""
        return self._workers

    def get_conf_path(self) -> str:
        """method to get conf path to read conf from"""
        return self._conf_path

    def gen_source_options(self) -> Iterable[Dict[str, str]]:
        """
        method to generate options for all the datasources
        provided by user in the config file
        """
        options = []
        for name in self._mapping:
            ds = self._mapping[name]
            option = {
                'label': f'{name} [{ds.get_type().value}]',
                'value': name
            }
            options.append(option)
        return options

    def _load_settings(self) -> None:
        """
        helper method to load config settings from env. If env
        var not set then use defaults
        """
        try:
            self._port = int(os.getenv(
                'CAPMON_PORT',
                8050,
            ))
            self._workers = int(os.getenv(
                'CAPMON_WORKERS',
                2,
            ))
            self._debug = strtobool(os.getenv(
                'CAPMON_DEBUG',
                'no',
            ))
            self._host = os.getenv(
                'CAPMON_HOST',
                '0.0.0.0',
            )
            current_dir = os.path.dirname(os.path.realpath(__file__))
            self._conf_path = os.getenv(
                'CAPMON_CONFIG_PATH',
                current_dir + '/config.yml'
            )
        except ValueError as e:
            raise InvalidConfigError('unable to parse env: ' + str(e))

    def _load_config(self) -> Dict[str, Datasource]:
        """helper method to load config"""
        try:
            mapping = {}
            with open(self._conf_path) as config_file:
                config = yaml.full_load(config_file)
                for datasource in config['datasources']:
                    if datasource['name'] in mapping:
                        raise InvalidConfigError('repreated datasource name')
                    mapping[datasource['name']] = Datasource(
                        name=datasource['name'],
                        source=datasource['source'],
                        source_type=DatasourceType.from_str(
                            src_type=datasource['type'],
                        )
                    )
            return mapping
        except Exception as e:
            raise InvalidConfigError('unable to load config: ' + str(e))
