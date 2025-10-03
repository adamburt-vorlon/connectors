import sys
import json
from datetime import datetime, timezone
import pytz
from external_import_connector.utils import *
from stix2 import Relationship, Software, Directory
import uuid

from pycti import OpenCTIConnectorHelper

from .client_api import ConnectorClient
from .config_loader import ConfigConnector
from .converter_to_stix import ConverterToStix


class ConnectorCatalogImport:
    """
    Specifications of the external import connector

    This class encapsulates the main actions, expected to be run by any external import connector.
    Note that the attributes defined below will be complemented per each connector type.
    This type of connector aim to fetch external data to create STIX bundle and send it in a RabbitMQ queue.
    The STIX bundle in the queue will be processed by the workers.
    This type of connector uses the basic methods of the helper.

    ---

    Attributes
        - `config (ConfigConnector())`:
            Initialize the connector with necessary configuration environment variables

        - `helper (OpenCTIConnectorHelper(config))`:
            This is the helper to use.
            ALL connectors have to instantiate the connector helper with configurations.
            Doing this will do a lot of operations behind the scene.

        - `converter_to_stix (ConnectorConverter(helper))`:
            Provide methods for converting various types of input data into STIX 2.1 objects.

    ---

    Best practices
        - `self.helper.api.work.initiate_work(...)` is used to initiate a new work
        - `self.helper.schedule_iso()` is used to encapsulate the main process in a scheduler
        - `self.helper.connector_logger.[info/debug/warning/error]` is used when logging a message
        - `self.helper.stix2_create_bundle(stix_objects)` is used when creating a bundle
        - `self.helper.send_stix2_bundle(stix_objects_bundle)` is used to send the bundle to RabbitMQ
        - `self.helper.set_state()` is used to set state

    """

    def __init__(self, config: ConfigConnector, helper: OpenCTIConnectorHelper):
        """
        Initialize the Connector with necessary configurations
        """

        # Load configuration file and connection helper
        self.config = config
        self.helper = helper
        self.client = ConnectorClient(self.helper, self.config)
        self.converter_to_stix = ConverterToStix(self.helper, self.config)

    def process_items(self, stix_objects):
        if len(stix_objects):
            stix_objects_bundle = self.helper.stix2_create_bundle(stix_objects)
            bundles_sent = self.helper.send_stix2_bundle(
                stix_objects_bundle,
                work_id=self.helper.work_id,
                cleanup_inconsistent_bundle=True,
            )

            self.helper.connector_logger.info(
                "Sending STIX objects to OpenCTI...",
                {"bundles_sent": {str(len(bundles_sent))}},
            )

    def _collect_intelligence(self) -> list:
        """
        Collect intelligence from the source and convert into STIX object
        :return: List of STIX objects
        """
        stix_objects = []
        
        last_run_now = datetime.now(pytz.UTC)

        # ===========================
        # === Add your code below ===
        # ===========================

        # Set namespace items
        service_namespace = uuid.UUID(self.config.service_namespace)
        endpoints_namespace = uuid.UUID(self.config.endpoint_namespace)
        scopes_namespace = uuid.UUID(self.config.scope_namespace)
        
        # Create mongo connection
        mongo: MongoClient = mongodb_connect(self.config.mongo_conn_str)
        catalog = mongo.catalog
        
        # Create connections for each collection
        mongo_services = catalog.service
        mongo_endpoints = catalog.endPoint
        mongo_scopes = catalog.scope
        
        all_services = mongo_services.find({})

        
        # Parse all services
        for service in all_services:
            mapped_endpoints_to_scopes = {}
            service_id = service.get('_id')
            service_name = service.get('name')
            service_deterministic_uuid = uuid.uuid5(service_namespace, service_id)
            service_description = service.get('description')
            service_updated_at: datetime = service.get('updated_at', datetime(1970,1,1)).astimezone(pytz.UTC)
            properties = service.get('properties', {})
            applicable_labels = service.get('categories', [])
            applicable_labels.append("service")
            if service.get('hidden', False):
                applicable_labels.append("hidden")
            if properties.get('observable', False):
                applicable_labels.append("observable")
            
            # Create the service observable if it is within date
            if service_updated_at > self.config.last_run_services:
                service_obj = {
                    "type": "Software",
                    "id": f"software--{service_deterministic_uuid}",
                    "name": service_name,
                    "swid": service_id,
                    "description": service_description if service_description else None,
                    "x_opencti_main_observable": True,
                    "labels": applicable_labels
                }
                stix_objects.append(service_obj)
                
            # Collect the endpoints
            service_endpoints = mongo_endpoints.find({
                "service": service_id,
                "updated_at": {"$gt": self.config.last_run_endpoints}
            })
            for endpoint in service_endpoints:
                path = endpoint.get('path', '')
                method = endpoint.get('method', '')
                endpoint_deterministic_uuid = uuid.uuid5(endpoints_namespace, f"{service_id}{path}{method}")
                description = endpoint.get('description', '')
                properties = endpoint.get('properties', {})
                permissions = properties.get('permissions', [])
                
                # Create the endpoint observable
                endpoint_obj = {
                    "type": "Directory",
                    "id": f"directory--{endpoint_deterministic_uuid}",
                    "path": path,
                    "path_enc": method,
                    "description": description,
                    "x_opencti_main_observable": True,
                    "labels": ["endpoint", service_id]
                }
                stix_objects.append(endpoint_obj)
                if permissions:
                    mapped_endpoints_to_scopes.update({
                        f"directory--{endpoint_deterministic_uuid}": permissions
                    })
                
                # Create the relationship to the service
                er = Relationship(
                    source_ref=f"software--{service_deterministic_uuid}",
                    target_ref=f"directory--{endpoint_deterministic_uuid}",
                    relationship_type="related-to"
                )
                
                stix_objects.append(er)
            
            # Collect all scopes for the service
            all_scopes = mongo_scopes.find({
                "service_id": service_id,
                "updated_at": {"$gt": self.config.last_run_scopes}
            })
            for scope in all_scopes:
                scope_id = scope.get('scope_id', '')
                scope_name = scope.get('scope_name', '')
                scope_description = scope.get('scope_description', '')
                scope_deterministic_uuid = uuid.uuid5(scopes_namespace, f"{scope_id}:{service_id}")
                applicable_labels = ["scope", service_id]
                access_sensitive = scope.get('access_sensitive', False)
                admin_capabilities = scope.get('admin_capabilities', False)
                access_pii = scope.get('access_pii', False)
                is_read = scope.get('is_read', False)
                is_write = scope.get('is_write', False)
                
                score = 0
                if access_sensitive:
                    score += 10
                    applicable_labels.append("access_sensitive")
                if admin_capabilities:
                    score += 45
                    applicable_labels.append("admin_capabilities")
                if access_pii:
                    score += 20
                    applicable_labels.append("access_pii")
                if is_read:
                    score += 5
                    applicable_labels.append("is_read")
                if is_write:
                    score += 10
                    applicable_labels.append("is_write")
                
                # Create the scope observable
                scope_obj = {
                    "type": "Text",
                    "id": f"text--{scope_deterministic_uuid}",
                    "value": scope_name,
                    "description": scope_description,
                    "score": score,
                    "x_opencti_main_observable": True,
                    "x_opencti_author": service_id,
                    "labels": applicable_labels
                }
                stix_objects.append(scope_obj)
                
                # Create the relationship
                sr = Relationship(
                    source_ref=f"software--{service_deterministic_uuid}",
                    target_ref=f"text--{scope_deterministic_uuid}",
                    relationship_type="related-to"
                )
                stix_objects.append(sr)
                
                if mapped_endpoints_to_scopes:
                    for det_id, endpoint_scopes in mapped_endpoints_to_scopes.items():
                        if scope_id in endpoint_scopes:
                            esp = Relationship(
                                source_ref=f"text--{scope_deterministic_uuid}",
                                target_ref=det_id,
                                relationship_type="related-to"
                            )
                            stix_objects.append(esp)
        
        # Set the last run
        self.config.set_last_run(last_run_now)

        # ===========================
        # === Add your code above ===
        # ===========================

        # Ensure consistent bundle by adding the author and TLP marking
        if len(stix_objects):
            stix_objects.append(self.converter_to_stix.author)
            stix_objects.append(self.converter_to_stix.tlp_marking)

        return stix_objects

    def process_message(self) -> None:
        """
        Connector main process to collect intelligence
        :return: None
        """
        self.helper.connector_logger.info(
            "[CONNECTOR] Starting connector...",
            {"connector_name": self.helper.connect_name},
        )

        try:
            # Get the current state
            now = datetime.now()
            current_timestamp = int(datetime.timestamp(now))
            current_state = self.helper.get_state()

            if current_state is not None and "last_run" in current_state:
                last_run = current_state["last_run"]

                self.helper.connector_logger.info(
                    "[CONNECTOR] Connector last run",
                    {"last_run_datetime": last_run},
                )
            else:
                self.helper.connector_logger.info(
                    "[CONNECTOR] Connector has never run..."
                )

            # Friendly name will be displayed on OpenCTI platform
            friendly_name = self.config.connector_name

            # Initiate a new work
            work_id = self.helper.api.work.initiate_work(
                self.helper.connect_id, friendly_name
            )

            self.helper.connector_logger.info(
                "[CONNECTOR] Running connector...",
                {"connector_name": self.helper.connect_name},
            )

            # Performing the collection of intelligence
            # ===========================
            # === Add your code below ===
            # ===========================
            stix_objects = self._collect_intelligence()

            if len(stix_objects):
                stix_objects_bundle = self.helper.stix2_create_bundle(stix_objects)
                bundles_sent = self.helper.send_stix2_bundle(
                    stix_objects_bundle,
                    work_id=work_id,
                    cleanup_inconsistent_bundle=True,
                )

                self.helper.connector_logger.info(
                    "Sending STIX objects to OpenCTI...",
                    {"bundles_sent": {str(len(bundles_sent))}},
                )
            # ===========================
            # === Add your code above ===
            # ===========================

            # Store the current timestamp as a last run of the connector
            self.helper.connector_logger.debug(
                "Getting current state and update it with last run of the connector",
                {"current_timestamp": current_timestamp},
            )
            current_state = self.helper.get_state()
            current_state_datetime = now.strftime("%Y-%m-%d %H:%M:%S")
            last_run_datetime = datetime.fromtimestamp(
                current_timestamp, tz=timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%S")
            if current_state:
                current_state["last_run"] = current_state_datetime
            else:
                current_state = {"last_run": current_state_datetime}
            self.helper.set_state(current_state)

            message = (
                f"{self.helper.connect_name} connector successfully run, storing last_run as "
                + str(last_run_datetime)
            )

            self.helper.api.work.to_processed(work_id, message)
            self.helper.connector_logger.info(message)

        except (KeyboardInterrupt, SystemExit):
            self.helper.connector_logger.info(
                "[CONNECTOR] Connector stopped...",
                {"connector_name": self.helper.connect_name},
            )
            sys.exit(0)
        except Exception as err:
            self.helper.connector_logger.error(str(err))

    def run(self) -> None:
        """
        Run the main process encapsulated in a scheduler
        It allows you to schedule the process to run at a certain intervals
        This specific scheduler from the pycti connector helper will also check the queue size of a connector
        If `CONNECTOR_QUEUE_THRESHOLD` is set, if the connector's queue size exceeds the queue threshold,
        the connector's main process will not run until the queue is ingested and reduced sufficiently,
        allowing it to restart during the next scheduler check. (default is 500MB)
        It requires the `duration_period` connector variable in ISO-8601 standard format
        Example: `CONNECTOR_DURATION_PERIOD=PT5M` => Will run the process every 5 minutes
        :return: None
        """
        self.helper.schedule_iso(
            message_callback=self.process_message,
            duration_period=self.config.duration_period,
        )
