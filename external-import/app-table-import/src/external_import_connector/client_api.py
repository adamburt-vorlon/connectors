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
        headers = {"Bearer": self.config.api_key}
        self.session = requests.Session()
        self.session.headers.update(headers)

    def _request_data(self, api_url: str, params=None):
        """
        Internal method to handle API requests
        :return: Response in JSON format
        """
        try:
            response = self.session.get(api_url, params=params)

            self.helper.connector_logger.info(
                "[API] HTTP Get Request to endpoint", {"url_path": api_url}
            )

            response.raise_for_status()
            return response

        except requests.RequestException as err:
            error_msg = "[API] Error while fetching data: "
            self.helper.connector_logger.error(
                error_msg, {"url_path": {api_url}, "error": {str(err)}}
            )
            return None

    def get_entities(self) -> dict:
        """
        Get app table entries
        """
        try:
            
            pg = postgres_connect(self.config.pg_conn_string)
            cursor = pg.cursor()
            query = "SELECT * FROM app_service"
            if self.config.only_unassigned:
                query = f"{query} WHERE service_id = '' OR service_id is null"
            cursor.execute(query)
            
            all_data = cursor.fetchall()
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
