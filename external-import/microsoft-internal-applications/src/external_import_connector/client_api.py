import requests


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

    def get_entities(self, params=None) -> dict:
        """
        Get all software IDs from the website
        """
        try:
            # ===========================
            # === Add your code below ===
            # ===========================
            
            data = {}

            try:
                response = requests.get(self.config.api_base_url)
            except Exception as err:
                self.helper.connector_logger.error(f"There was an error connecting {err}")
            if response.status_code == 200:
                data = response.json()
            
            return data

            # return response.json()
            # ===========================
            # === Add your code above ===
            # ===========================

        except Exception as err:
            self.helper.connector_logger.error(err)
