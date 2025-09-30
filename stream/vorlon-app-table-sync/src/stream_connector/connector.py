import json
from stream_connector.utils import *
import re
from stix2 import parse_observable, Software
from datetime import datetime
import pytz
import uuid

from pycti import OpenCTIConnectorHelper

from .config_loader import ConfigConnector


class ConnectorAppTableStream:
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
        self.re_map = {
            "add_verified_label": re.compile(r"^adds `verified` in `Label`"),
            "remove_verified_label": re.compile(r"^removes `verified` in `Label`"),
            "change_vendor": re.compile(r"^replaces `.+` in `Vendor`"),
        }

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
    
    def service_is_valid(self, service_id: str) -> bool:
        ret_val = False
        if service_id == '':
            return True
        mongo = mongodb_connect(self.config.mongo_conn_str)
        catalog = mongo.catalog
        services = catalog.service
        filtered_services = services.find({
            "_id": service_id
        })
        all_filtered_services = [x for x in filtered_services]
        if all_filtered_services:
            ret_val = True
        else:
            ret_val = False
        mongo.close()
        # service_uuid = uuid.uuid5(self.config.service_namespace, service_id)
        # id = f"software--{service_uuid}"
        # matches = self.helper.api.stix_cyber_observable.list(
        #     filters={
        #         "mode": "and",
        #         "filters":[
        #             {
        #                 "key": "id",
        #                 "values": [id]
        #             }
        #         ],
        #         "filterGroups":[]
        #     },
        # )
        # if matches:
        #     ret_val = True
        # else:
        #     ret_val = False
        return ret_val

    def create_app(self, data: dict):
        swid = data.get('swid','')
        name = data.get('name','')
        vendor = data.get('vendor','')
        obs_id = data.get('id')
        if self.service_is_valid(vendor):
            pg = postgres_connect(self.config.pg_conn_str)
            cursor = pg.cursor()
            
            # Get labels and created date
            extensions = data.get('extensions', {})
            extension_core = extensions.get('extension-definition--ea279b3e-5c71-4632-ac08-831c66a786ba', {})
            created_at = extension_core.get('created_at', '')
            if created_at:
                date_created = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%S.%fZ")
                date_created_int = int(date_created.timestamp() * 10)
            else:
                date_created_int = int(datetime.now(pytz.UTC).timestamp() * 10)
            extension_labels = extensions.get('extension-definition--f93e2c80-4231-4f9a-af8b-95c9bd566a82', {})
            labels = extension_labels.get('labels')
            if "verified" in labels:
                verified = True
            else:
                verified = False
            labels[:] = [x for x in labels if x not in ['application', 'verified']]
            if len(labels) == 1:
                label = labels[0]
            else:
                label = ''

            # Check if the app already exists
            query = f"SELECT Id FROM app_service WHERE app_id = '{swid}'"
            if label:
                query = f"{query} AND platform_id = '{label}'"
            try:
                cursor.execute(query)
                results = cursor.fetchall()
            except:
                return
            if results:
                return
            
            try:
                create_query = f"INSERT INTO app_service (app_id, platform_id, app_name, service_id, date_created, vendor_verified, ignore) VALUES ('{swid}', '{label}', '{name}', '{vendor}', {date_created_int}, {verified}, False);"
                cursor.execute(create_query)
                pg.commit()
            except:
                pg.rollback()
            pg.close()
        else:
            self.helper.connector_logger.warning(f"Invalid service ID '{vendor}', removing entry")
            self.helper.api.stix_cyber_observable.delete(id=obs_id)
    
    def update_app(self, event_type: str, data: dict):
        swid = data.get('swid','')
        name = data.get('name','')
        vendor = data.get('vendor','')
        obs_id = data.get('id')
        pg = postgres_connect(self.config.pg_conn_str)
        cursor = pg.cursor()
        if event_type == "change_vendor":
            if not self.service_is_valid(vendor):
                self.helper.api.stix_cyber_observable.add_label(
                    id=obs_id,
                    label_name="invalid_vendor"
                )
            else:
                self.helper.api.stix_cyber_observable.remove_label(
                    id=obs_id,
                    label_name="invalid_vendor"
                )
            query = f"UPDATE app_service SET service_id = '{vendor}' WHERE app_id = '{swid}' AND app_name = '{name}';"
        elif event_type == "add_verified_label":
            query = f"UPDATE app_service SET vendor_verified = true WHERE app_id = '{swid}' AND app_name = '{name}';"
        elif event_type == "remove_verified_label":
            query = f"UPDATE app_service SET vendor_verified = false WHERE app_id = '{swid}' AND app_name = '{name}';"
        try:
            cursor.execute(query)
            pg.commit()
        except:
            pg.rollback()
        pg.close()
    
    def delete_app(self, data: dict):
        swid = data.get('swid','')
        name = data.get('name','')
        pg = postgres_connect(self.config.pg_conn_str)
        cursor = pg.cursor()
        query = f"DELETE FROM app_service WHERE app_id = '{swid}' AND app_name = '{name}'"
        try:
            cursor.execute(query)
            pg.commit()
        except:
            pg.rollback()
        pg.close()

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

            # data = json.loads(msg.data)["data"]
        except Exception:
            raise ValueError("Cannot process the message")

        # Performing the main process
        # ===========================
        # === Add your code below ===
        # ===========================

        data = json.loads(msg.data)
        
        # Handle creation
        if msg.event == "create":
            self.create_app(data.get('data', {}))
            self.helper.connector_logger.info("[CREATE]")

        # Handle update
        if msg.event == "update":
            event_type = ""
            
            # Determine the update type
            for event, event_re in self.re_map.items():
                if event_re.match(data.get('message', '')):
                    event_type = event
                    break
            if event_type:
                self.update_app(event_type, data.get('data', {}))
                self.helper.connector_logger.info("[UPDATE]")

        # Handle delete
        if msg.event == "delete":
            self.delete_app(data.get('data', {}))
            self.helper.connector_logger.info("[DELETE]")

        # ===========================
        # === Add your code above ===
        # ===========================

    def run(self) -> None:
        """
        Run the main process in self.helper.listen() method
        The method continuously monitors messages from the platform
        The connector have the capability to listen a live stream from the platform.
        The helper provide an easy way to listen to the events.
        """
        self.helper.listen_stream(message_callback=self.process_message)
