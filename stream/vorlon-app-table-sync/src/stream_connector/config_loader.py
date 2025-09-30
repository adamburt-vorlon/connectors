import os
from pathlib import Path
import uuid

import yaml
from pycti import get_config_variable


class ConfigConnector:
    def __init__(self):
        """
        Initialize the connector with necessary configurations
        """

        # Load configuration file
        self.load = self._load_config()
        self._initialize_configurations()

    @staticmethod
    def _load_config() -> dict:
        """
        Load the configuration from the YAML file
        :return: Configuration dictionary
        """
        config_file_path = Path(__file__).parents[1].joinpath("config.yml")
        config = (
            yaml.load(open(config_file_path), Loader=yaml.FullLoader)
            if os.path.isfile(config_file_path)
            else {}
        )

        return config

    def _initialize_configurations(self) -> None:
        """
        Connector configuration variables
        :return: None
        """
        # OpenCTI configurations

        # Connector extra parameters
        self.pg_conn_str = get_config_variable(
            "CONNECTOR_PG_CONN_STR",
            ["connector_template", "pg_conn_str"],
            self.load
        )
        
        self.mongo_conn_str = get_config_variable(
            "CONNECTOR_MONGO_CONN_STR",
            ["connector_template", "mongo_conn_str"],
            self.load
        )
        
        self.service_namespace = get_config_variable(
            "SERVICE_NAMESPACE",
            ["connector_template", "service_namespace"],
            self.load
        )
        
        self.service_namespace = uuid.UUID(self.service_namespace)
