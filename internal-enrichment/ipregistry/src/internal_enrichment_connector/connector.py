from pycti import OpenCTIConnectorHelper, StixCoreRelationship
import os
import json
from datetime import datetime, timedelta
import pytz
import uuid
from stix2 import DomainName, Relationship, Identity, AutonomousSystem, Location, Indicator, MarkingDefinition
from .client_api import ConnectorClient
from .config_loader import ConfigConnector
from .converter_to_stix import ConverterToStix
from ipregistry import IpregistryClient, IpInfo, ApiResponse, ApiError


class ConnectorIPRegsitry:
    """
    Specifications of the internal enrichment connector

    This class encapsulates the main actions, expected to be run by any internal enrichment connector.
    Note that the attributes defined below will be complemented per each connector type.
    This type of connector aim to enrich a data (Observables) created or modified in the OpenCTI core platform.
    It will create a STIX bundle and send it in a RabbitMQ queue.
    The STIX bundle in the queue will be processed by the workers.
    This type of connector uses the basic methods of the helper.
    Ingesting a bundle allow the connector to be compatible with the playbook automation feature.


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
        - `self.helper.connector_logger.[info/debug/warning/error]` is used when logging a message
        - `self.helper.stix2_create_bundle(stix_objects)` is used when creating a bundle
        - `self.helper.send_stix2_bundle(stix_objects_bundle)` is used to send the bundle to RabbitMQ

    """

    def __init__(self, config: ConfigConnector, helper: OpenCTIConnectorHelper):
        """
        Initialize the Connector with necessary configurations
        """

        # Load configuration file and connection helper
        self.config = config
        self.helper = helper
        self.client = ConnectorClient(self.helper, self.config)
        self.converter_to_stix = ConverterToStix(self.helper)
        self.ipr_client: IpregistryClient = IpregistryClient(config.api_key)

        # Define variables
        self.author = Identity(
            name="IPRegistry",
            identity_class="organization",
            description="IPRegistry",
            sectors=["technology"]
        )
        self.tlp = None
        self.stix_objects_list = []
    
    def handle_ips(self, json_data: dict) -> list:
        
        all_ips = [v.get('obs_value') for k, v in json_data.items()]
                
        try:
            all_ip_data: ApiResponse = self.ipr_client.batch_lookup_ips(all_ips)
        except ApiError as err:
            self.helper.api.work.to_received(
                self.helper.work_id,
                err.message
            )
            return []
        for item in all_ip_data.data:
            ip = item.ip
            matching_observer = [k for k,v in json_data.items() if v.get('obs_value') == ip]
            if matching_observer:
                json_data[matching_observer[0]].update({
                    "ip_data": item
                })
        for obs_id, obs_data in json_data.items():
            
            ip_details: IpInfo = obs_data.get('ip_data')
            stix_entity = obs_data.get('observable')
            opencti_labels = stix_entity.get('x_opencti_labels', [])
            full_observable = self.helper.api.stix_cyber_observable.read(filters={
                "mode": "and",
                "filters": [
                    {
                        "key": "standard_id",
                        "values": [obs_id]
                    }
                ],
                "filterGroups": []
            })
            current_markings = []
            if full_observable:
                current_markings = full_observable.get('objectMarking', [])
                current_markings[:] = [x.get('standard_id') for x in current_markings if x.get('definition_type')== "IPRegistry"]
            
            connection = ip_details.connection
            company = ip_details.company
            location = ip_details.location
            security = ip_details.security
            
            stix_asn = None
            stix_domain = None
            stix_organization = None
            stix_company = None
            stix_location = None
            
            # Handle connection data
            if connection:
                
                # Create the asn
                if connection.asn:
                    stix_asn = AutonomousSystem(
                        number=connection.asn
                    )
                
                # Create the domain
                if connection.domain:
                    stix_domain = DomainName(
                        value=connection.domain
                    )
                
                # Create the organization
                if connection.organization:
                    stix_organization = Identity(
                        name=connection.organization,
                        identity_class="organization"
                    )            
                
            # Handle company data
            if company:
                if company.name:
                    stix_company = Identity(
                        name=company.name,
                        identity_class="organization", 
                        sectors=[company.type]
                    )
            
            # Handle location data
            if location:
                if location.city:
                    stix_location = Location(
                        latitude=location.latitude,
                        longitude=location.longitude,
                        region=location.region.name,
                        country=location.country.code,
                        city=location.city,
                        postal_code=location.postal
                    )
            
            # Populate stix objects
            if stix_asn:
                self.stix_objects_list.append(stix_asn)
                self.stix_objects_list.append(Relationship(
                    id=StixCoreRelationship.generate_id(
                        "belongs-to", obs_id, stix_asn.id
                    ),
                    relationship_type="belongs-to",
                    source_ref=obs_id,
                    target_ref=stix_asn.id
                ))
            
            if stix_domain:
                self.stix_objects_list.append(stix_domain)
                self.stix_objects_list.append(Relationship(
                    source_ref=obs_id,
                    target_ref=stix_domain.id,
                    relationship_type="related-to"
                ))
            
            if stix_organization:
                self.stix_objects_list.append(stix_organization)
                self.stix_objects_list.append(Relationship(
                    source_ref=obs_id,
                    target_ref=stix_organization.id,
                    relationship_type="related-to"
                ))
            
            if stix_company:
                self.stix_objects_list.append(stix_company)
                self.stix_objects_list.append(Relationship(
                    source_ref=obs_id,
                    target_ref=stix_company.id,
                    relationship_type="related-to"
                ))
            
            if stix_location:
                self.stix_objects_list.append(stix_location)
                self.stix_objects_list.append(Relationship(
                    source_ref=obs_id,
                    target_ref=stix_location.id,
                    relationship_type="related-to"
                ))

                if stix_company:
                    self.stix_objects_list.append(Relationship(
                        id=StixCoreRelationship.generate_id(
                            "located-at", stix_company.id, stix_location.id
                        ),
                        source_ref=stix_company.id,
                        target_ref=stix_location.id,
                        relationship_type="located-at"
                    ))
            
            score_map = {
                "IPRegistry:is_abuser": {
                    "check_object": security.is_abuser,
                    "score": self.config.is_abuser_score
                },
                "IPRegistry:is_anonymous": {
                    "check_object": security.is_anonymous,
                    "score": self.config.is_anonymous_score
                },
                "IPRegistry:is_attacker": {
                    "check_object": security.is_attacker,
                    "score": self.config.is_attacker_score
                },
                "IPRegistry:is_bogon": {
                    "check_object": security.is_bogon,
                    "score": self.config.is_bogon_score
                },
                "IPRegistry:is_cloud_provider": {
                    "check_object": security.is_cloud_provider,
                    "score": self.config.is_cloud_provider_score
                },
                "IPRegistry:is_proxy": {
                    "check_object": security.is_proxy,
                    "score": self.config.is_proxy_score
                },
                "IPRegistry:is_relay": {
                    "check_object": security.is_relay,
                    "score": self.config.is_relay_score
                },
                "IPRegistry:is_threat": {
                    "check_object": security.is_threat,
                    "score": self.config.is_threat_score
                },
                "IPRegistry:is_tor": {
                    "check_object": security.is_tor,
                    "score": self.config.is_tor_score
                },
                "IPRegistry:is_tor_exit": {
                    "check_object": security.is_tor_exit,
                    "score": self.config.is_tor_exit_node_score
                },
                "IPRegistry:is_vpn": {
                    "check_object": security.is_vpn,
                    "score": self.config.is_vpn_score
                }
            }
            
            score = 0
            
            labels_to_add = []
            labels_to_remove = []
            markings_to_add = []
            markings_to_remove = []
            
            # Determine which labels to apply
            for label, data in score_map.items():
                if data.get('check_object'):
                    labels_to_add.append(label)
                    score += data.get('score')
                else:
                    if label in opencti_labels:
                        labels_to_remove.append(label)
            
            score = 100 if score > 100 else score
                        
            # Determine malicious or suspicious
            if score >= self.config.malicious_score:
                labels_to_add.append(self.config.malicious_label)
                labels_to_remove.append(self.config.suspicious_label)
                labels_to_remove.append(self.config.clean_label)
                markings_to_add.append(self.config.marking_malicious)
                markings_to_remove.append(self.config.marking_suspicious)
                markings_to_remove.append(self.config.marking_clean)
            elif score >= self.config.suspicious_score:
                labels_to_add.append(self.config.suspicious_label)
                labels_to_remove.append(self.config.clean_label)
                labels_to_remove.append(self.config.malicious_label)
                markings_to_add.append(self.config.marking_suspicious)
                markings_to_remove.append(self.config.marking_malicious)
                markings_to_remove.append(self.config.marking_clean)
            else:
                labels_to_add.append(self.config.clean_label)
                labels_to_remove.append(self.config.suspicious_label)
                labels_to_remove.append(self.config.malicious_label)
                markings_to_add.append(self.config.marking_clean)
                markings_to_remove.append(self.config.marking_suspicious)
                markings_to_remove.append(self.config.marking_malicious)
            
            # Apply all pending labels
            for label in labels_to_remove:
                if label in opencti_labels:
                    self.helper.api.stix_cyber_observable.remove_label(
                        id=obs_id,
                        label_name=label
                    )
            for label in labels_to_add:
                if label not in opencti_labels:
                    self.helper.api.stix_cyber_observable.add_label(
                        id=obs_id,
                        label_name=label
                    )
            
            # Apply all pending markings
            for marking in markings_to_remove:
                if marking in current_markings:
                    self.helper.api.stix_cyber_observable.remove_marking_definition(
                        id=obs_id,
                        marking_definition_id=marking
                    )
            for marking in markings_to_add:
                if marking not in current_markings:
                    self.helper.api.stix_cyber_observable.add_marking_definition(
                        id=obs_id,
                        marking_definition_id=marking
                    )
                        
            # Create the indicator
            namespace = uuid.UUID(self.config.indicator_namespace)
            new_indicator_id = uuid.uuid5(namespace, f"IPRegistry-{ip}")
            new_indicator_id_str = f"indicator--{new_indicator_id}"
            
            new_indicator = {
                "type": "indicator",
                "id": new_indicator_id_str,
                "name": f"IPRegistry - {ip}",
                "pattern_type": "stix",
                "pattern": f"[{ip_details.type.lower()}-addr:value = '{ip}']",
                "x_opencti_main_observable_type": f"{ip_details.type}-Addr",
                "x_opencti_score": score,
                "x_opencti_labels": labels_to_add,
                "detection": True,
                "object_marking_refs": markings_to_add
            }
            self.stix_objects_list.append(new_indicator)
            self.stix_objects_list.append(Relationship(
                source_ref=obs_id,
                target_ref=new_indicator_id_str,
                relationship_type="related-to"
            ))
            
            # Create the external reference
            external_reference = self.helper.api.external_reference.create(
                source_name="IPRegistry",
                url=f"https://ipregistry.co/{ip}",
                external_id=ip
            )
            self.helper.api.stix_cyber_observable.add_external_reference(
                id=obs_id,
                external_reference_id=external_reference.get('standard_id')
            )
            
            # Apply the score if necessary
            if self.config.apply_score:
                self.helper.api.stix_cyber_observable.create(
                    simple_observable_id=obs_id,
                    x_opencti_score=score,
                    update=True
                )
        
        return self.stix_objects_list

    def _collect_intelligence(self, json_data: dict) -> list:
        """
        Collect intelligence from the source and convert into STIX object
        :return: List of STIX objects
        """
        self.helper.connector_logger.info("[CONNECTOR] Starting enrichment...")

        # ===========================
        # === Add your code below ===
        # ===========================
        entities = self.handle_ips(json_data)
        return entities

        # EXAMPLE
        # === Get entities from external sources based on entity value
        # entities = self.client.get_entity(value)

        # === Create the author
        # self.author = self.converter.create_author()

        # === Convert into STIX2 object and add it to the stix_object_list
        # entity_to_stix = self.converter_to_stix.create_obs(value,obs_id)
        # self.stix_object_list.append(entity_to_stix)

        # return self.stix_objects_list

        # ===========================
        # === Add your code above ===
        # ===========================

    def entity_in_scope(self, data) -> bool:
        """
        Security to limit playbook triggers to something other than the initial scope
        :param data: Dictionary of data
        :return: boolean
        """
        scopes = self.helper.connect_scope.lower().replace(" ", "").split(",")
        entity_split = data["entity_id"].split("--")
        entity_type = entity_split[0].lower()

        if entity_type in scopes:
            return True
        else:
            return False

    def extract_and_check_markings(self, tlp: str) -> bool:
        """
        Extract TLP, and we check if the variable "max_tlp" is less than
        or equal to the markings access of the entity from OpenCTI
        If this is true, we can send the data to connector for enrichment.
        :param opencti_entity: Dict of observable from OpenCTI
        :return: Boolean
        """
        if tlp:
            valid_max_tlp = self.helper.check_max_tlp(self.tlp, self.config.max_tlp)
        else:
            valid_max_tlp = True
        
        return valid_max_tlp

    def check_and_build_markings(self):
        all_markings = self.helper.api.marking_definition.list()
        ip_registry_markings = [x for x in all_markings if x.get('definition_type') == "IPRegistry"]
        for marking in ip_registry_markings:
            if marking.get('definition') == "IPRegistry:clean":
                self.config.marking_clean = marking.get('standard_id')
            if marking.get('definition') == "IPRegistry:suspicious":
                self.config.marking_suspicious = marking.get('standard_id')
            if marking.get('definition') == "IPRegistry:malicious":
                self.config.marking_malicious = marking.get('standard_id')
        
        if not self.config.marking_clean:
            md = {
                "definition_type": "IPRegistry",
                "definition": "IPRegistry:clean",
                "x_opencti_color": "#2bcc45"
            }
            clean_md = self.helper.api.marking_definition.create(
                **md
            )
            self.config.marking_clean = clean_md.get('id')
        
        if not self.config.marking_suspicious:
            md = {
                "definition_type": "IPRegistry",
                "definition": "IPRegistry:suspicious",
                "x_opencti_color": "#f89d3a"
            }
            suspicious_md = self.helper.api.marking_definition.create(
                **md
            )
            self.config.marking_suspicious = suspicious_md.get('id')
        
        if not self.config.marking_malicious:
            md = {
                "definition_type": "IPRegistry",
                "definition": "IPRegistry:malicious",
                "x_opencti_color": "#fa1818"
            }
            malicious_md = self.helper.api.marking_definition.create(
                **md
            )
            self.config.marking_malcious = malicious_md.get('id')

    def process_message(self, data: dict) -> str:
        """
        Get the observable created/modified in OpenCTI and check which type to send for process
        The data passed in the data parameter is a dictionary with the following structure as shown in
        https://docs.opencti.io/latest/development/connectors/#additional-implementations
        :param data: dict of data to process
        :return: string
        """
        try:
            cache_file_path = os.path.join("./data", "cached_items.json")
            self.check_and_build_markings()
                        
            # Test the TLP max level
            opencti_entity = data["enrichment_entity"]
            tlp = "TLP:CLEAR"
            for marking_definition in opencti_entity["objectMarking"]:
                if marking_definition["definition_type"] == "TLP":
                    tlp = marking_definition["definition"]
            valid = self.extract_and_check_markings(tlp)
            if not valid:
                info_msg = f"[FAILED] {tlp} is higher than {self.config.max_tlp}, will not process"
                return info_msg


            # To enrich the data, you can add more STIX object in stix_objects
            self.stix_objects_list = []
            observable = data["stix_entity"]

            # Extract information from entity data
            obs_standard_id = observable["id"]
            obs_value = observable["value"]
            obs_type = observable["type"]

            info_msg = (
                "[CONNECTOR] Processing observable for the following entity type: "
            )
            self.helper.connector_logger.info(info_msg, {"type": {obs_type}})

            if self.entity_in_scope(data):
                
                # Performing the collection of intelligence and enrich the entity
                # ===========================
                # === Add your code below ===
                # ===========================
                
                # If there is a batch size greater than 1 defined, then use
                # the batch storage                    
                
                stix_objects = None
                
                # Check to see if the indicator already exists
                existing_indicator = self.helper.api.indicator.read(
                    filters={
                        "mode": "and",
                        "filters": [
                            {
                                "key": "name",
                                "values": [f"{obs_value} (IPRegistry)"]
                            }
                        ],
                        "filterGroups": []
                    }
                )
                if existing_indicator:
                    info_msg = "[CONNECTOR] Observer within TTL, skipping lookup"
                    return info_msg
                
                json_data = {}
                if os.path.exists(cache_file_path):
                    with open(cache_file_path, "r") as fp:
                        try:
                            json_data = json.load(fp)
                        except:
                            json_data = {}
                
                # Add to the cache
                now = datetime.now(pytz.UTC)
                if obs_standard_id not in json_data:
                    json_data[obs_standard_id] = {
                        "obs_value": obs_value,
                        "observable": observable,
                        "date_added": now.timestamp()
                    }
                
                # If we have reached our batch size, then process them all
                if len(json_data) > self.config.batch_size:
                    stix_objects = self._collect_intelligence(json_data)
                
                # If there is a defined max wait period, check it
                elif self.config.max_wait_period > 0:
                    all_timestamps = [v.get('date_added') for k, v in json_data.items()]
                    latest_timestamp = sorted(all_timestamps)[0]
                    max_datetime = (datetime.fromtimestamp(latest_timestamp) + timedelta(minutes=self.config.max_wait_period)).timestamp()
                    if max_datetime < now.timestamp():
                        stix_objects = self._collect_intelligence(json_data)
                    else:
                        info_msg = "[CONNECTOR] Observer lookup added to cache"
                        with open(cache_file_path, "w") as pf:
                            json.dump(json_data, pf)
                        return info_msg
                
                # Otherwise save and continue
                else:
                    info_msg = "[CONNECTOR] Observer lookup added to cache"
                    with open(cache_file_path, "w") as pf:
                        json.dump(json_data, pf)
                    return info_msg
                
                # Overwrite the cache file with empty data
                with open(cache_file_path, "w") as pf:
                    json.dump({}, pf)
                
                # Add the author
                if stix_objects is not None and len(stix_objects):
                    stix_objects.append(self.author)
                    return self._send_bundle(stix_objects)
                else:
                    info_msg = "[CONNECTOR] No information found"
                    return info_msg

                # ===========================
                # === Add your code above ===
                # ===========================
            else:
                if not data.get("event_type"):
                    # If it is not in scope AND entity bundle passed through playbook, we should return the original bundle unchanged
                    self._send_bundle(self.stix_objects_list)
                else:
                    # self.helper.connector_logger.info(
                    #     "[CONNECTOR] Skip the following entity as it does not concern "
                    #     "the initial scope found in the config connector: ",
                    #     {"entity_id": opencti_entity["entity_id"]},
                    # )
                    raise ValueError(
                        f"Failed to process observable, {opencti_entity['entity_type']} is not a supported entity type."
                    )
        except Exception as err:
            # Handling other unexpected exceptions
            return self.helper.connector_logger.error(
                "[CONNECTOR] Unexpected Error occurred", {"error_message": str(err)}
            )

    def _send_bundle(self, stix_objects: list) -> str:
        stix_objects_bundle = self.helper.stix2_create_bundle(stix_objects)
        bundles_sent = self.helper.send_stix2_bundle(stix_objects_bundle)

        info_msg = (
            "Sending " + str(len(bundles_sent)) + " stix bundle(s) for worker import"
        )
        return info_msg

    def run(self) -> None:
        """
        Run the main process in self.helper.listen() method
        The method continuously monitors a message queue associated with a specific connector
        The connector have to listen a specific queue to get and then enrich the information.
        The helper provide an easy way to listen to the events.
        """
        self.helper.listen(message_callback=self.process_message)
