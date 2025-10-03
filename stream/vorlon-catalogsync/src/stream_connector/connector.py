import json
from pymongo import MongoClient
from datetime import datetime, timedelta
import pytz

from pycti import OpenCTIConnectorHelper

from .config_loader import ConfigConnector

DEFAULT_SERVICE_PROPERTIES = {
    "app_secret_creation_date": False,
    "app_secret_creator": False,
    "app_secret_expiration_date": False,
    "app_secret_last_modified": False,
    "app_secret_lastused_date": False,
    "app_secret_lastused_person": False,
    "app_secret_metafields": [],
    "app_secret_modifier": False,
    "app_secret_permissions": False,
    "app_secret_via_audit_log": False,
    "audit_log_application": False,
    "audit_log_collection": "Full",
    "audit_log_full_uri": False,
    "audit_log_intent": False,
    "audit_log_method": False,
    "audit_log_restrictions": "",
    "audit_log_secret": False,
    "audit_log_src_ip": False,
    "audit_log_token": False,
    "audit_log_user": False,
    "can_generate_keys": False,
    "certification": {
      "c5": False,
      "csa": False,
      "dpa": False,
      "hipaa": False,
      "iso27001": False,
      "iso27017": False,
      "iso27018": False,
      "pcidss": False,
      "soc2": False,
      "soc3": False
    },
    "collect_secrets": False,
    "collect_users": False,
    "collector_collection_endpoints": [],
    "collector_collection_permissions": [],
    "exposes_admin_functions": False,
    "exposes_credentials": False,
    "exposes_financial": False,
    "exposes_pii": False,
    "exposes_secrets": False,
    "exposes_sensitive_data": False,
    "has_public_ips": False,
    "impact_level": 3,
    "is_beta": False,
    "is_lean": False,
    "licence_requirements": "",
    "most_secure_auth": "OAUTH",
    "observable": False,
    "secret_collection_types": [],
    "secret_collects_permissions": False,
    "secret_creation_dates": False,
    "secret_creator": False,
    "secret_expiration_date": False,
    "secret_lastused_date": False,
    "secret_lastused_person": False,
    "supported_authentication": [],
    "supports_expirations": False,
    "supports_ip_restrictions": False,
    "supports_permissions": False,
    "token_secret_collects_permissions": False,
    "token_secret_creation_date": False,
    "token_secret_creator": False,
    "token_secret_expiration_date": False,
    "token_secret_last_modified": False,
    "token_secret_lastused_date": False,
    "token_secret_lastused_person": False,
    "token_secret_metafields": [],
    "token_secret_modifier": False,
    "token_secret_via_audit_log": False,
    "traffic_dst_metafields": [],
    "traffic_src_metafields": [],
    "user_secret_creation_date": False,
    "user_secret_creator": False,
    "user_secret_expiration_date": False,
    "user_secret_last_modified": False,
    "user_secret_lastused_date": False,
    "user_secret_lastused_person": False,
    "user_secret_metafields": [],
    "user_secret_modifier": False,
    "user_secret_permissions": False,
    "user_secret_via_audit_log": False
  }


class ConnectorCatalogSync:
    """
    Specifications of the Stream connector

    This class encapsulates the main actions, expected to be run by any stream connector.
    Note that the attributes defined below will be complemented per each connector type.
    This type of connector has the capability to listen to live streams from the OpenCTI platform.
    It is highly useful for creating connectors that can react and make decisions in real time.
    Actions on OpenCTI will apply the changes to the third-party connected platform
    ---

    Attributes
        - `config (ConfigConnector())`:
            Initialize the connector with necessary configuration environment variables

        - `helper (OpenCTIConnectorHelper(config))`:
            This is the helper to use.
            ALL connectors have to instantiate the connector helper with configurations.
            Doing this will do a lot of operations behind the scene.

    ---

    Best practices
        - `self.helper.connector_logger.[info/debug/warning/error]` is used when logging a message

    """

    def __init__(self, config: ConfigConnector, helper: OpenCTIConnectorHelper):
        """
        Initialize the Connector with necessary configurations
        """

        # Load configuration file and connection helper
        self.config = config
        self.helper = helper
        self.label_extension = "extension-definition--f93e2c80-4231-4f9a-af8b-95c9bd566a82"

    def check_stream_id(self) -> None:
        """
        In case of stream_id configuration is missing, raise Value Error
        :return: None
        """
        if (
            self.helper.connect_live_stream_id is None
            or self.helper.connect_live_stream_id == "ChangeMe"
        ):
            raise ValueError("Missing stream ID, please check your configurations.")

    def create_software(self, obs_data):
        return
        # Check all fields are present, otherwise delete
        standard_id = obs_data.get('id')
        swid = obs_data.get('swid')
        name = obs_data.get('name')
        description = obs_data.get('description')
        
        if not swid or not name:
            return
        
        mongo: MongoClient = MongoClient(self.config.mongo_conn_str)
        catalog = mongo.catalog
        mongo_services = catalog.service
        
        # Check for existing services
        existing = [x for x in mongo_services.find({"_id": swid})]
        if existing:
            self.helper.api.stix_cyber_observable.delete(standard_id)
            return
        
        # Otherwise create the service
        labels = self.get_labels(obs_data)
        categories = [x for x in labels if x not in ["service", "application", "hidden", "observable"]]
        these_properties = {k: v for k, v in DEFAULT_SERVICE_PROPERTIES.items()}
        mongo_services.insert_one({
            "_id": swid,
            "name": name,
            "description": description if description else "",
            "categories": categories,
            "updated_at": datetime.now(pytz.UTC),
            "hidden": True if "hidden" in labels else False,
            "properties": these_properties
        })
    
    def get_relationship_objects(self, obs_data: dict) -> tuple[dict]:
        source = {}
        target = {}
        source_id = obs_data.get('source_ref')
        target_id = obs_data.get('target_ref')
        source = self.helper.api.stix_cyber_observable.read(
            id = source_id
        )
        target = self.helper.api.stix_cyber_observable.read(
            id = target_id
        )        
        return source, target
    
    def get_type_labels_value(self, obs_data: dict) -> tuple[str]:
        type = None
        labels = []
        value = ""
        if obs_data:
            type = obs_data.get('entity_type')
            labels = [x.get('value') for x in obs_data.get('objectLabel', []) if x.get('value')]
            value = obs_data.get('observable_value')
        return type, labels, value
    
    def create_directory(self, obs_data):
        pass
    
    def create_text(self, obs_data):
        pass
    
    def create_relationship(self, obs_data):
        relationship_id = obs_data.get('id')
        source_obs, target_obs = self.get_relationship_objects(obs_data)
        source_type, source_labels, source_value = self.get_type_labels_value(source_obs)
        target_type, target_labels, target_value = self.get_type_labels_value(target_obs)
        
        # Switch source / destination of reqired
        if source_type == "Text":
            source_obs, target_obs = target_obs, source_obs
        
        source_type, source_labels, source_value = self.get_type_labels_value(source_obs)
        target_type, target_labels, target_value = self.get_type_labels_value(target_obs)
        source_method = source_obs.get('path_enc')
        target_method = target_obs.get('path_enc')
        
        mongo_client = MongoClient(self.config.mongo_conn_str)
        catalog = mongo_client.catalog
        mongo_endpoints = catalog.endPoint
        matching_endpoints = [x for x in mongo_endpoints.find({"path": source_value, "method": source_method})]
        if len(matching_endpoints) == 1:
            matching_endpoint = matching_endpoints[0]
            endpoint_id = matching_endpoint.get('_id')
            properties = matching_endpoint.get('properties', {})
            permissions: list = properties.get('permissions', [])
            if target_value not in permissions:
                permissions.append(target_value)
                
                # Perform the update
                try:
                    update = mongo_endpoints.update_one(
                        {
                            "_id": endpoint_id
                        },
                        {
                            "$set": {
                                "properties.permissions": permissions,
                                "updated_at": datetime.now(pytz.UTC)
                            }
                        }
                    )
                    # If it was not successful, delete the relationship
                    if update.matched_count < 1:
                        self.helper.api.stix_core_relationship.delete(id=relationship_id)
                except Exception as err:
                    pass
                
        mongo_client.close()
    
    def update_software(self, obs_data):
        pass
    
    def update_directory(self, obs_data):
        pass
    
    def update_text(self, obs_data):
        pass

    def update_relationship(self, obs_data):
        source_obs, target_obs = self.get_relationship_objects(obs_data)
        pass
    
    def delete_software(self, obs_data):
        pass
    
    def delete_directory(self, obs_data):
        pass
    
    def delete_text(self, obs_data):
        pass
    
    def delete_relationship(self, obs_data):
        source_obs, target_obs = self.get_relationship_objects(obs_data)
        source_type, source_labels, source_value = self.get_type_labels_value(source_obs)
        target_type, target_labels, target_value = self.get_type_labels_value(target_obs)
        
        # Switch source / destination of reqired
        if source_type == "Text":
            source_obs, target_obs = target_obs, source_obs
        
        source_type, source_labels, source_value = self.get_type_labels_value(source_obs)
        target_type, target_labels, target_value = self.get_type_labels_value(target_obs)
        source_method = source_obs.get('path_enc')
        target_method = target_obs.get('path_enc')
        
        mongo_client = MongoClient(self.config.mongo_conn_str)
        catalog = mongo_client.catalog
        mongo_endpoints = catalog.endPoint
        matching_endpoints = [x for x in mongo_endpoints.find({"path": source_value, "method": source_method})]
        if len(matching_endpoints) == 1:
            matching_endpoint = matching_endpoints[0]
            endpoint_id = matching_endpoint.get('_id')
            properties = matching_endpoint.get('properties', {})
            permissions: list = properties.get('permissions', [])
            if target_value in permissions:
                permissions.remove(target_value)
                try:
                    update = mongo_endpoints.update_one(
                        {
                            "_id": endpoint_id
                        },
                        {
                            "$set": {
                                "properties.permissions": permissions,
                                "updated_at": datetime.now(pytz.UTC)
                            }
                        }
                    )
                    if update.modified_count < 1:
                        self.helper.api.stix_core_relationship.create(
                            stix_id=obs_data.get('id'),
                            relationship_type=obs_data.get('relationship_type'),
                            created=obs_data.get('created'),
                            modified=obs_data.get('modified'),
                            confidence=obs_data.get('confidence'),
                            start_time=obs_data.get('start_time'),
                            stop_time=obs_data.get('stop_time'),
                            from_id=obs_data.get('source_ref'),
                            to_id=obs_data.get('target_ref')
                        )
                except Exception as err:
                    pass
        mongo_client.close()

    def get_labels(self, obs_data: dict) -> list[str]:
        label_extensions = obs_data.get(self.label_extension, {})
        labels = label_extensions.get('labels', [])
        return labels
    
    def valid_relationship(self, obs_data, user_id: str) -> bool:
        valid = False
        
        # Check the user is not the catalog service itself
        if user_id == self.config.catalog_user_id:
            return False
        
        # Check the source and destination
        source_obs, target_obs = self.get_relationship_objects(obs_data)
        if source_obs and target_obs:
            source_type, source_labels, source_value = self.get_type_labels_value(source_obs)
            target_type, target_labels, target_value = self.get_type_labels_value(target_obs)
            if (source_type == "Directory" and "endpoint" in source_labels and target_type == "Text" and "scope" in target_labels) or (target_type == "Directory" and "endpoint" in target_labels and source_type == "Text" and "scope" in source_labels):
                valid = True
        return valid
    
    def process_message(self, msg) -> None:
        """
        Main process if connector successfully works
        The data passed in the data parameter is a dictionary with the following structure as shown in
        https://docs.opencti.io/latest/development/connectors/#additional-implementations
        :param msg: Message event from stream
        :return: string
        """
        try:
            self.check_stream_id()
        except Exception:
            raise ValueError("Cannot process the message")

        # =========================================== #
        # The catalog sync only handles relationships #
        # =========================================== #

        data = json.loads(msg.data)
        origin = data.get('origin', {})
        user_id = origin.get('user_id', '')
        obs_data = data.get('data', {})
        obs_type = obs_data.get('type')
        event = msg.event
        labels = self.get_labels(obs_data)
        
        # Handle creation
        if msg.event == "create":
            if obs_type == "relationship":
                if self.valid_relationship(obs_data, user_id):
                    self.helper.connector_logger.info("[CREATE]")
                    self.create_relationship(obs_data)
            # elif obs_type == "software":
            #     if "service" in labels:
            #         self.create_software(obs_data)
            # elif obs_type == "directory":
            #     if "endpoint" in labels:
            #         self.create_directory(obs_data)
            # elif obs_type == "text":
            #     if "scope" in labels:
            #         self.create_text(obs_data)
        
        # Handle update
        if msg.event == "update":
            if obs_type == "relationship":
                if self.valid_relationship(obs_data, user_id):
                    self.helper.connector_logger.info("[UPDATE]")
                    self.update_relationship(obs_data)
            # elif obs_type == "software":
            #     if "service" in labels:
            #         self.update_software(obs_data)
            # elif obs_type == "directory":
            #     if "endpoint" in labels:
            #         self.update_directory(obs_data)
            # elif obs_type == "text":
            #     if "scope" in labels:
            #         self.update_text(obs_data)

        # Handle delete
        if msg.event == "delete":
            if obs_type == "relationship":
                if self.valid_relationship(obs_data, user_id):
                    self.helper.connector_logger.info("[DELETE]")
                    self.delete_relationship(obs_data)
            # elif obs_type == "software":
            #     if "service" in labels:
            #         self.delete_software(obs_data)
            # elif obs_type == "directory":
            #     if "endpoint" in labels:
            #         self.delete_directory(obs_data)
            # elif obs_type == "text":
            #     if "scope" in labels:
            #         self.delete_text(obs_data)


    def run(self) -> None:
        """
        Run the main process in self.helper.listen() method
        The method continuously monitors messages from the platform
        The connector have the capability to listen a live stream from the platform.
        The helper provide an easy way to listen to the events.
        """
        self.helper.listen_stream(message_callback=self.process_message)
