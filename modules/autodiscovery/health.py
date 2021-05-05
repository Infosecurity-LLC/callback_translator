import json
from uuid import uuid4

from flask import Blueprint, current_app

health_bp = Blueprint(__name__, "health")


@health_bp.route("/health", methods=["GET"])
def health_endpoint():
    if not current_app.config.get("SERVICE_ID"):
        current_app.config["SERVICE_ID"] = str(uuid4())
    response_body = {
        "version": current_app.config.get("VERSION", "1"),
        "service_id": current_app.config.get("SERVICE_ID"),
        "description": current_app.config.get("DESCRIPTION", ""),
        'details': {}
    }

    return json.dumps(response_body), 200, {"Content-Type": "application/health+json"}
