import sys
from datetime import datetime, timezone
from stix2 import Software, Relationship, URL
import uuid
import json

from pycti import OpenCTIConnectorHelper, CustomObservableText

from .client_api import ConnectorClient
from .config_loader import ConfigConnector
from .converter_to_stix import ConverterToStix


class ConnectorMSInternalApps:
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
    
    def get_existing_scopes(self) -> list:
        return_list = []
        existing_indicators = self.helper.api.stix_cyber_observable.list(
            filters={
                "mode": "and",
                "filters":[
                    {
                        "key": "entity_type",
                        "values": ["Text"]
                    },
                    {
                        "key": "objectLabel",
                        "values": ["scope"]
                    }
                ],
                "filterGroups":[]
            },
            withPagination=True
        )
        has_more = True
        while has_more:
            pagination = existing_indicators.get('pagination', {})
            has_more = pagination.get('hasNextPage', False)
            entities = existing_indicators.get('entities', [])
            for entity in entities:
                return_list.append(entity.get('standard_id'))
            if has_more:
                existing_indicators = self.helper.api.indicator.list(
                    filters={
                        "mode": "and",
                        "filters":[
                            {
                                "key": "entity_type",
                                "values": ["Text"]
                            },
                            {
                                "key": "objectLabel",
                                "values": ["scope"]
                            }
                        ],
                        "filterGroups":[]
                    },
                    withPagination=True,
                    after=pagination.get('endCursor')
                )
        return return_list

    def _collect_intelligence(self) -> list:
        """
        Collect intelligence from the source and convert into STIX object
        :return: List of STIX objects
        """
        stix_objects = []

        # ===========================
        # === Add your code below ===
        # ===========================
        
        # Namespace to use
        NAMESPACE = uuid.UUID(self.config.uuid_namespace)
        
        # Vendor to use
        vendor = "microsoft365"
        
        # Log all scopes to prevent creating multiple times
        global_scopes = []#self.get_existing_scopes()
        global_redirect_uris = []
        
        
        # Get entities from external sources
        entities: dict = self.client.get_entities()
        apps = entities.get('apps', {})
        rids = entities.get('resourceidentifiers', {})

        for app_id, app_data in apps.items():
            app_name = app_data.get('name')
            redirect_uris = sorted(app_data.get('redirect_uris', []))
            
            # Create main software observable
            software = json.loads(Software(
                name=app_name,
                swid=app_id,
                vendor=vendor
            ).serialize())
            software["x_opencti_labels"] = [vendor, "application"]
            stix_objects.append(software)
            
            # Create observables for the redirect URIs
            for uri in redirect_uris:
                url_deterministic_uuid = uuid.uuid5(NAMESPACE, f"{uri}:{vendor}")
                if url_deterministic_uuid not in global_redirect_uris:
                    url = {
                        "type": "url",
                        "id": f"url--{url_deterministic_uuid}",
                        "value": uri,
                        "x_opencti_main_observable": True,
                        "x_opencti_author": "microsoft365",
                        "x_opencti_labels": ["redirect", vendor]
                    }
                    stix_objects.append(url)
                    global_redirect_uris.append(url_deterministic_uuid)
                else:
                    url = {
                        "id": f"url--{url_deterministic_uuid}"
                    }
                
                # Create the relationship
                url_relationship = Relationship(
                    source_ref=url.get('id'),
                    relationship_type="related-to",
                    target_ref=software.get('id')
                )
                stix_objects.append(url_relationship)
            
            
            # Aggregate all scopes
            scopes = []
            for scope_dest, scope_data in app_data.get('scopes', {}).items():
                if scope_data and isinstance(scope_data, list):
                    for scope_item in scope_data:
                        if scope_item not in scopes:
                            scopes.append(scope_item)
            scopes = sorted(scopes)
            
            for scope in scopes:
                
                scope_deterministic_uuid = uuid.uuid5(NAMESPACE, f"{scope}:{vendor}")
                if scope_deterministic_uuid not in global_scopes:
                    
                    # Create the scope observable if it does not exist
                    text_obj = {
                        "type": "Text",
                        "id": f"text--{scope_deterministic_uuid}",
                        "value": scope,
                        "x_opencti_main_observable": True,
                        "x_opencti_author": vendor,
                        "labels": ["scope", vendor]
                    }
                    stix_objects.append(text_obj)
                    global_scopes.append(scope_deterministic_uuid)
                else:
                    text_obj = {
                        "id": f"text--{scope_deterministic_uuid}"
                    }
                
                # Create the relationship
                scope_relationship = Relationship(
                    source_ref=text_obj.get('id'),
                    relationship_type="related-to",
                    target_ref=software.get('id')
                )
                stix_objects.append(scope_relationship)

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
            friendly_name = "Microsoft Internal Applications"

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
