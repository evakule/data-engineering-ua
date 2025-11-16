from loguru import logger
from flask import Blueprint, jsonify, request
from typing import List

from lec02.service.sales_service import SalesService

sales_bp_raw = Blueprint("save_to_raw", __name__, url_prefix="/")
sales_bp_stg = Blueprint("save_to_stg", __name__, url_prefix="/")

sales_service = SalesService()


@sales_bp_stg.errorhandler(FileNotFoundError)
def handle_not_found(e):
    return jsonify({"Error": str(e)}), 400


@sales_bp_raw.post("/to_raw_locally")
def save_raw_data_locally():
    logger.info(f"Received request to save raw data locally.")

    payload: dict = request.get_json()

    file_path, data = sales_service.save_sales_data_locally(payload)

    logger.info(f"Saved raw data locally: {file_path}")

    return jsonify(data), 201


@sales_bp_stg.post("/to_stg_locally")
def save_to_stg_locally():
    logger.info(f"Received request to save raw data in avro format.")

    payload: dict = request.get_json()

    file_paths: List[str] = sales_service.from_raw_to_avro(payload)

    return jsonify({'Data saved to: ': file_paths}), 201
