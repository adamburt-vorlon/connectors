import os
from pathlib import Path

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
        self.api_key = get_config_variable(
            "IPREGISTRY_API_KEY",
            ["connector_template", "ipregistry_api_key"],
            self.load
        )

        self.indicator_namespace = get_config_variable(
            "INDICATOR_NAMESPACE",
            ["connector_template", "indicator_namespace"],
            self.load
        )
        
        self.max_tlp = get_config_variable(
            "MAX_TLP",
            ["connector_template", "max_tlp"],
            self.load
        )
        
        self.is_abuser_score = get_config_variable(
            "IS_ABUSER_SCORE",
            ["connector_template", "is_abuser_score"],
            self.load,
            default=0
        )
        
        self.is_anonymous_score = get_config_variable(
            "IS_ANONYMOUS_SCORE",
            ["connector_template", "is_anonymous_score"],
            self.load,
            default=0
        )
        
        self.is_attacker_score = get_config_variable(
            "IS_ATTACKER_SCORE",
            ["connector_template", "is_attacker_score"],
            self.load,
            default=0
        )
        
        self.is_bogon_score = get_config_variable(
            "IS_BOGON_SCORE",
            ["connector_template", "is_bogon_score"],
            self.load,
            default=0
        )
        
        self.is_cloud_provider_score = get_config_variable(
            "IS_CLOUD_PROVIDER_SCORE",
            ["connector_template", "is_cloud_provider_score"],
            self.load,
            default=0
        )
        
        self.is_proxy_score = get_config_variable(
            "IS_PROXY_SCORE",
            ["connector_template", "is_proxy_score"],
            self.load,
            default=0
        )
        
        self.is_relay_score = get_config_variable(
            "IS_RELAY_SCORE",
            ["connector_template", "is_relay_score"],
            self.load,
            default=0
        )
        
        self.is_threat_score = get_config_variable(
            "IS_THREAT_SCORE",
            ["connector_template", "is_threat_score"],
            self.load,
            default=0
        )
        
        self.is_tor_score = get_config_variable(
            "IS_TOR_SCORE",
            ["connector_template", "is_tor_score"],
            self.load,
            default=0
        )
        
        self.is_tor_exit_node_score = get_config_variable(
            "IS_TOR_EXIT_NODE_SCORE",
            ["connector_template", "is_tor_exit_node_score"],
            self.load,
            default=0
        )
        
        self.is_vpn_score = get_config_variable(
            "IS_VPN_SCORE",
            ["connector_template", "is_vpn_score"],
            self.load,
            default=0
        )
        
        self.suspicious_score = get_config_variable(
            "SUSPICIOUS_SCORE",
            ["connector_template", "suspicious_score"],
            self.load,
            default=0
        )
        
        self.malicious_score = get_config_variable(
            "MALICIOUS_SCORE",
            ["connector_template", "malicious_score"],
            self.load,
            default=0
        )
        
        self.apply_score = get_config_variable(
            "APPLY_SCORE_TO_OBSERVABLE",
            ["connector_template", "applt_score_to_observable"],
            self.load,
            default=True
        )
        
        self.batch_size = get_config_variable(
            "BATCH_SIZE",
            ["connector_template", "batch_size"],
            self.load,
            default=1024
        )
        
        self.malicious_label = "IPRegistry:malicious"
        self.suspicious_label = "IPRegistry:suspicious"
        self.clean_label = "IPRegistry:clean"
        self.marking_clean = None
        self.marking_suspicious = None
        self.marking_malicious = None
