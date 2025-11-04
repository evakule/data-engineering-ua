from pathlib import Path
from loguru import logger
from fastavro import writer, parse_schema


def write_data_to_avro_file(
        schema: dict,
        file_storage_path: str,
        stg_dir: str,
        data: dict,
        file_name: str
) -> str:
    parsed_schema = parse_schema(schema)

    out_path: Path = Path(f"{file_storage_path}/{stg_dir}")
    out_path.mkdir(parents=True, exist_ok=True)

    file_path: Path = out_path / file_name

    logger.info(f'Writing data to {out_path}/{file_name}')

    try:
        with open(file_path, 'wb') as out:
            writer(out, parsed_schema, data)
    except OSError as e:
        logger.error(f"Failed to write data to file: {file_name}")
        raise e

    return str(file_path)
