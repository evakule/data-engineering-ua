from flask import Flask
from threading import Thread
from werkzeug.serving import make_server
from loguru import logger
import time


from lec02.controller.sales_controller import sales_bp_raw, sales_bp_stg


def create_raw_app(name) -> Flask:
    flask_app = Flask(name)
    flask_app.register_blueprint(sales_bp_raw)
    return flask_app


def create_stg_app(name) -> Flask:
    flask_app = Flask(name)
    flask_app.register_blueprint(sales_bp_stg)
    return flask_app


class ServerThread(Thread):
    def __init__(self, app: Flask, host: str, port: int):
        super().__init__(daemon=True)
        self.server = make_server(host, port, app)
        self.ctx = app.app_context()
        self.ctx.push()

    def run(self):
        self.server.serve_forever()

    def shutdown(self):
        self.server.shutdown()


if __name__ == '__main__':
    raw_app = create_raw_app("raw_app")
    stg_app = create_stg_app("stg_app")

    raw_server = ServerThread(raw_app, "localhost", 8081)
    stg_server = ServerThread(stg_app, "localhost", 8082)

    logger.info('Starting server raw_app')
    raw_server.start()

    logger.info('Starting server stg_app')
    stg_server.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        raw_server.shutdown()
        stg_server.shutdown()
