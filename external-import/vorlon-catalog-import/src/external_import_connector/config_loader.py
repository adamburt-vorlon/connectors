import os
from pathlib import Path
from datetime import datetime
import json
import pytz
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
        self.get_last_run()

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

    def get_last_run(self):
        if os.path.exists(self.last_run_file):
            with open(self.last_run_file, "r") as fp:
                try:
                    json_data = json.load(fp)
                except:
                    json_data = {}
                try:
                    self.last_run_services = datetime.fromtimestamp(json_data.get('services')).astimezone(pytz.UTC)
                except:
                    pass
                try:
                    self.last_run_endpoints = datetime.fromtimestamp(json_data.get('endpoints')).astimezone(pytz.UTC)
                except:
                    pass
                try:
                    self.last_run_scopes = datetime.fromtimestamp(json_data.get('scopes')).astimezone(pytz.UTC)
                except:
                    pass
    
    def set_last_run(self, dt: datetime):
        with open(self.last_run_file, "w") as pf:
            json.dump({
                "services": self.last_run_services.timestamp(),
                "endpoints": self.last_run_endpoints.timestamp(),
                "scopes": self.last_run_scopes.timestamp()
            },pf)

    def _initialize_configurations(self) -> None:
        """
        Connector configuration variables
        :return: None
        """
        # OpenCTI configurations
        self.last_run_file = "last_run.json"
        
        self.connector_name = get_config_variable(
            "CONNECTOR_NAME",
            ["connector", "name"],
            self.load,
            default="Catalog Data Import"
        )
        
        self.duration_period = get_config_variable(
            "CONNECTOR_DURATION_PERIOD",
            ["connector", "duration_period"],
            self.load,
        )

        # Connector extra parameters
        self.api_base_url = get_config_variable(
            "CONNECTOR_TEMPLATE_API_BASE_URL",
            ["connector_template", "api_base_url"],
            self.load,
        )

        self.api_key = get_config_variable(
            "CONNECTOR_TEMPLATE_API_KEY",
            ["connector_template", "api_key"],
            self.load,
        )

        self.tlp_level = get_config_variable(
            "CONNECTOR_TEMPLATE_TLP_LEVEL",
            ["connector_template", "tlp_level"],
            self.load,
            default="clear",
        )
        
        self.mongo_conn_str = get_config_variable(
            "MONGO_CONN_STR",
            ["connector_template", "mongo_conn_str"],
            self.load,
            default="",
        )
        
        self.scope_namespace = get_config_variable(
            "SCOPE_NAMESPACE",
            ["connector_template", "scope_namespace"],
            self.load,
            default="",
        )
        
        self.endpoint_namespace = get_config_variable(
            "ENDPOINT_NAMESPACE",
            ["connector_template", "endpoint_namespace"],
            self.load,
            default="",
        )
        
        self.service_namespace = get_config_variable(
            "SERVICE_NAMESPACE",
            ["connector_template", "service_namespace"],
            self.load,
            default="",
        )
        
        self.last_run_services = datetime(1970,1,1).astimezone(pytz.UTC)
        self.last_run_endpoints = datetime(1970,1,1).astimezone(pytz.UTC)
        self.last_run_scopes = datetime(1970,1,1).astimezone(pytz.UTC)
