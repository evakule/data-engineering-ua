import json
from loguru import logger
from pathlib import Path


def write_json_data(
        data: dict,
        file_storage_path: str,
        raw_dir: str,
        file_name: str,
) -> str:
    out_path: Path = Path(f"{file_storage_path}/{raw_dir}")
    out_path.mkdir(parents=True, exist_ok=True)

    file_path: Path = out_path / file_name

    logger.info(f'Writing data to {out_path}/{file_name}')

    try:
        with open(file_path, 'w+') as f:
            json.dump(data, f)
    except OSError as e:
        logger.error(f"Failed to write file: {file_path}")
        raise e

    return str(file_path)


def read_json_file(path: str) -> dict:
    try:
        with open(path, 'r') as file:
            return json.load(file)
    except FileNotFoundError as e:
        logger.error(f"Failed to read file: {path}")
        raise e

