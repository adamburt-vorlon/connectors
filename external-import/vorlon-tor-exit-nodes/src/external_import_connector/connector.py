import sys
from stix2 import Indicator, IPv4Address, Relationship, IPv6Address
from datetime import datetime, timezone, timedelta
import pytz

from pycti import OpenCTIConnectorHelper

from .client_api import ConnectorClient
from .config_loader import ConfigConnector
from .converter_to_stix import ConverterToStix


class ConnectorTorExitNodes:
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
    
    def get_existing_indicators(self) -> dict:
        return_list = {}
        existing_indicators = self.helper.api.indicator.list(
            filters={
                "mode": "and",
                "filters":[
                    {
                        "key": ["entity_type"],
                        "values": "Indicator"
                    },
                    {
                        "key": "objectLabel",
                        "values": ["tor-exit-node"]
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
                return_list.update({
                    entity.get('pattern'): {
                        "standard_id": entity.get('standard_id', '')
                    }
                })
            if has_more:
                existing_indicators = self.helper.api.indicator.list(
                    filters={
                        "mode": "and",
                        "filters":[
                            {
                                "key": ["entity_type"],
                                "values": "Indicator"
                            },
                            {
                                "key": "objectLabel",
                                "values": ["tor-exit-node"]
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
        tor_exit_node_label = "tor-exit-node"

        # ===========================
        # === Add your code below ===
        # ===========================

        # Get entities from external sources
        entities = self.client.get_entities()
        
        # Get all existing indicators that have the tor-exit-nodes label
        existing_indicators: dict = self.get_existing_indicators()

        # Convert into STIX2 object and add it on a list
        valid_from = datetime.now(pytz.UTC)
        valid_to = (valid_from + timedelta(hours=1))
        self.helper.log_info(f"Found {len(entities)} entities")
        for entity in entities:
            ip = entity.get('value', '')
            if ip:
                ip_type = ""
                if self.converter_to_stix._is_ipv4(ip):
                    ip_type = "ipv4-addr"
                    observable = IPv4Address(value=ip)
                elif self.converter_to_stix._is_ipv6(ip):
                    ip_type = "ipv6-addr"
                    observable = IPv6Address(value=ip)
                
                pattern=f"[{ip_type}:value = '{ip}']"
                if pattern in existing_indicators:
                    indicator = Indicator(
                        id=existing_indicators[pattern].get('standard_id'),
                        name=existing_indicators[pattern].get('name'),
                        valid_from=valid_from,
                        valid_until=valid_to,
                        pattern_type="stix",
                        pattern=f"[{ip_type}:value = '{ip}']",
                        indicator_types=["anonymization"]
                    )
                else:
                    indicator = Indicator(
                        name=ip,
                        pattern_type="stix",
                        pattern=f"[{ip_type}:value = '{ip}']",
                        indicator_types=["anonymization"],
                        valid_from=valid_from,
                        valid_until=valid_to,
                        labels=[tor_exit_node_label],
                        custom_properties={
                            "x_created_by": tor_exit_node_label,
                        }
                    )
                
                relationship = Relationship(
                    source_ref=indicator.id,
                    target_ref=observable.id,
                    relationship_type="related-to",
                    start_time=valid_from,
                    stop_time=valid_to,
                    labels=[tor_exit_node_label]
                )
                stix_objects.append(indicator)
                stix_objects.append(observable)
                stix_objects.append(relationship)
        
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
