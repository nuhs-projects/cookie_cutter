from flask import Flask, request
import threading
import sys
import json

import logging

logger = logging.getLogger(__name__)

host_name = "0.0.0.0"
port = 8118
app = Flask(__name__)


@app.route("/")
@app.route("/healthcheck")
def main():
    return json.dumps({"success": True}), 200, {"ContentType": "application/json"}


@app.route("/shutdown")
def shutdown():
    logger.info("SHUTTING DOWN")
    func = request.environ.get("werkzeug.server.shutdown")
    if func is not None:
        func()
    else:
        raise Exception("Cannot shutdown")
    logger.info("hello world")


# if __name__ == "__main__":
def start_web_service():
    logger.info("Starting web service")

    threading.Thread(
        target=lambda: app.run(
            host=host_name, port=port, debug=False, use_reloader=False
        ),
        daemon=True,
    ).start()
    logger.info("Web service has started")
