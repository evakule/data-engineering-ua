import os
from typing import Iterable
from loguru import logger
from pathlib import Path
from typing import Tuple, Dict, List
from lec02.client.sales_http_client import HttpClient
from lec02.io.json_io import write_json_data, read_json_file
from lec02.io.avro_io import write_data_to_avro_file


class SalesService:

    def __init__(self):
        self.file_storage_path = 'file_storage'
        self.http_token: str = os.environ.get("AUTH_TOKEN")

        if self.http_token is None:
            raise RuntimeError("No token provided for http client.")

        self.http_base_url: str = 'https://fake-api-vycpfa6oca-uc.a.run.app/sales'

        self.http_client: HttpClient = HttpClient(self.http_base_url, self.http_token)

        self.sales_schema = {
            'doc': 'Simple sales schema.',
            'name': 'Sales',
            'namespace': 'Additional actions not required.',
            'type': 'record',
            'fields': [
                {'name': 'client', 'type': 'string'},
                {'name': 'price', 'type': 'long'},
                {'name': 'product', 'type': 'string'},
                {'name': 'purchase_date', 'type': 'string'},
            ],
        }

    def save_sales_data_locally(self, payload: dict) -> Tuple[str, Dict]:
        raw_dir: str = payload.get("raw_dir")
        date: str = payload.get("date")
        page: int = payload.get("page")

        if date is None or raw_dir is None:
            raise RuntimeError("All params are required to fetch the data.")

        data: dict = self.http_client.fetch_json_data(params={'date': date, 'page': page})

        file_name: str = f"sales_{date}_{page}.json"

        file_path: str = write_json_data(data, self.file_storage_path, raw_dir, file_name)

        return file_path, data

    def from_raw_to_avro(self, payload: dict) -> List[str]:
        result = []

        raw_dir: str = payload.get("raw_dir")
        stg_dir: str = payload.get("stg_dir")

        if raw_dir is None or stg_dir is None:
            raise RuntimeError('All params are required to save data in avro format.')

        base_raw_directory = f"{self.file_storage_path}/{raw_dir}"

        raw_file_names = self._get_json_file_names(base_raw_directory)

        for raw_file in raw_file_names:
            data: dict = read_json_file(f"{base_raw_directory}/{raw_file}")

            avro_file_name: str = raw_file.replace(".json", ".avro")

            file_path: str = write_data_to_avro_file(
                self.sales_schema, self.file_storage_path, stg_dir, data, avro_file_name
            )

            result.append(file_path)

        return result

    @staticmethod
    def _get_json_file_names(raw_dir_path: str) -> Iterable[str]:
        try:
            base = Path(raw_dir_path)
            for file in base.iterdir():
                if file.is_file():
                    yield file.name
        except FileNotFoundError as e:
            logger.error(f'Files not found in {raw_dir_path}. Try to create some files first.')
            raise e
