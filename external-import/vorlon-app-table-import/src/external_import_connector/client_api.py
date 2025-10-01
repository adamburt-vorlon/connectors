import requests
from external_import_connector.utils import *


class ConnectorClient:
    def __init__(self, helper, config):
        """
        Initialize the client with necessary configurations
        """
        self.helper = helper
        self.config = config

        # Define headers in session and update when needed
        headers = {"Bearer": ""}
        self.session = requests.Session()
        self.session.headers.update(headers)

    def get_entities(self) -> dict:
        """
        Get app table entries
        """
        try:
            
            pg = postgres_connect(self.config.pg_conn_string)
            cursor = pg.cursor()
            query = f"SELECT * FROM app_service WHERE date_created > {self.config.last_run}"
            if self.config.only_unassigned:
                query = f"{query} AND service_id = '' OR service_id is null"
            cursor.execute(query)
            all_data = cursor.fetchall()
            pg.close()
            return [
                {
                    "id": x[0],
                    "app_id": x[1],
                    "platform_id": x[2],
                    "app_name": x[3],
                    "service_id": x[4],
                    "date_created": x[5],
                    "vendor_verified": x[6]
                } for x in all_data
            ]
            

        except Exception as err:
            self.helper.connector_logger.error(err)
