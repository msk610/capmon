from distutils.util import strtobool
import os
from typing import Dict, Iterable
import yaml
from metrics.common import Query
from metrics.prometheus import PrometheusQuery


class InvalidConfigError(Exception):
    """
    InvalidConfigError is thrown when malformed or invalid
    config is provided
    """


class Datasource(object):
    """
    Datasource defines a data source/database to fetch
    data from for analysis
    """

    def __init__(
        self,
        name: str,
        source: str,
        source_type: str,
    ) -> None:
        self._name = name
        self._source = source
        self._type = source_type
        self._validate()

    def get_type(self) -> str:
        """method to get datasource type"""
        return self._type

    def get_query_for_src(
        self,
        query: str,
        lookback_days: int,
    ) -> Query:
        """method to get Query object for source"""
        return PrometheusQuery(
            query=query,
            source=self._source,
            lookback_days=lookback_days
        )

    def _validate(
        self
    ) -> None:
        """helper method to validate data source"""
        supported_types = [
            'prometheus',
        ]
        if self._type not in supported_types:
            msg = f'invalid source type for {self._name}: {self._type}'
            raise InvalidConfigError(msg)


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
        # return self._debug
        return True

    def get_port(self) -> int:
        """method to get port to run in"""
        return self._port

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
            option = {
                'label': f'{name} [{self._mapping[name].get_type()}]',
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
                8050
            ))
            self._debug = strtobool(os.getenv(
                'CAPMON_DEBUG',
                'no'
            ))
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
                        source_type=datasource['type']
                    )
            return mapping
        except Exception as e:
            raise InvalidConfigError('unable to load config: ' + str(e))
