import requests
from requests import Response
from loguru import logger
from typing import Dict
from lec02.client.decorator.handle_http_client_error import handle_http_client_error


class HttpClient:

    def __init__(self, base_url: str, token: str):
        self.base_url = base_url
        self.token = token

    @handle_http_client_error
    def fetch_json_data(self, params: Dict | None = None) -> Dict | None:
        if not params:
            params = {}

        logger.debug(f"Fetching data from {self.base_url} with params: {params}.")

        response: Response = requests.get(
            self.base_url,
            params=params,
            headers={"Authorization": f"{self.token}"}
        )

        response.raise_for_status()

        return response.json()
