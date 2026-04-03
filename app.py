from __future__ import annotations

import base64
import os
from typing import Any

import requests
from flask import Flask, jsonify, request
from flask_caching import Cache
from flask_cors import CORS

from frost_sync.config import load_settings, require_frost_client_id
from frost_sync.web import create_blueprint


FROST_OBS_URL = "https://frost.met.no/observations/v0.jsonld"
FROST_AVAILABLE_URL = "https://frost.met.no/observations/availableTimeSeries/v0.jsonld"
FROST_SOURCES_URL = "https://frost.met.no/sources/v0.jsonld"


def create_app() -> Flask:
    settings = load_settings()
    app = Flask(__name__)
    CORS(app)

    app.config["CACHE_TYPE"] = os.getenv("CACHE_TYPE", "SimpleCache")
    app.config["CACHE_DEFAULT_TIMEOUT"] = int(os.getenv("CACHE_DEFAULT_TIMEOUT", "300"))
    cache = Cache(app)

    client_id = require_frost_client_id(settings)
    auth_header = "Basic " + base64.b64encode(f"{client_id}:".encode()).decode()
    user_agent = os.getenv(
        "FROST_USER_AGENT",
        "SVV-VertiGIS-Workflow/1.0 (your.email@example.com)",
    )
    route_prefix = os.getenv("FROST_ROUTE_PREFIX", "/weather").rstrip("/") or "/weather"

    app.register_blueprint(create_blueprint(), url_prefix=route_prefix)

    @app.get("/health")
    def health() -> Any:
        return {"status": "ok"}

    @app.get("/frost")
    @cache.cached(timeout=600, query_string=True)
    def frost() -> Any:
        allowed_params = [
            "sources",
            "elements",
            "referencetime",
            "timeresolutions",
            "levels",
            "fields",
            "timeoffsets",
            "performancecategories",
            "exposurecategories",
        ]

        params = {key: request.args.get(key) for key in allowed_params if request.args.get(key)}
        if not {"sources", "elements", "referencetime"} <= params.keys():
            return (
                jsonify(
                    {
                        "error": "Missing required parameters. Must include: sources, elements, referencetime."
                    }
                ),
                400,
            )

        response = requests.get(
            FROST_OBS_URL,
            headers=_frost_headers(auth_header, user_agent),
            params=params,
            timeout=settings.request_timeout_seconds,
        )
        if response.status_code != 200:
            return _error_response(response)

        frost_data = response.json()
        requested_elements = [element.strip() for element in params["elements"].split(",")]
        result = []

        for point in frost_data.get("data", []):
            timestamp = point.get("referenceTime")
            entry: dict[str, Any] = {"timestamp": timestamp}

            for observation in point.get("observations", []):
                element_id = observation.get("elementId")
                if element_id in requested_elements:
                    entry[element_id] = {
                        "value": observation.get("value"),
                        "resolution": observation.get("timeResolution"),
                        "offset": observation.get("timeOffset"),
                        "qualityCode": observation.get("qualityCode"),
                    }

            if any(isinstance(value, dict) for value in entry.values()):
                result.append(entry)

        return jsonify(result)

    @app.get("/frost_available")
    @cache.cached(timeout=3600, query_string=True)
    def frost_available() -> Any:
        source = request.args.get("sources")
        if not source:
            return jsonify({"error": "Missing required parameter: sources"}), 400

        response = requests.get(
            FROST_AVAILABLE_URL,
            headers=_frost_headers(auth_header, user_agent),
            params={"sources": source},
            timeout=settings.request_timeout_seconds,
        )
        if response.status_code != 200:
            return _error_response(response)

        frost_data = response.json()
        return jsonify([item["elementId"] for item in frost_data.get("data", []) if item.get("elementId")])

    @app.get("/sources")
    @cache.cached(timeout=600, query_string=True)
    def frost_sources() -> Any:
        source = request.args.get("sources")
        if not source:
            return jsonify({"error": "Missing required parameter: sources"}), 400

        response = requests.get(
            FROST_SOURCES_URL,
            headers=_frost_headers(auth_header, user_agent),
            params={"ids": source},
            timeout=settings.request_timeout_seconds,
        )
        if response.status_code != 200:
            return _error_response(response)

        return jsonify(response.json())

    return app


def _frost_headers(auth_header: str, user_agent: str) -> dict[str, str]:
    return {
        "Authorization": auth_header,
        "User-Agent": user_agent,
        "Accept": "application/json",
    }


def _error_response(response: requests.Response) -> tuple[Any, int]:
    return (
        jsonify(
            {
                "error": "Frost API error",
                "status": response.status_code,
                "details": response.text,
            }
        ),
        response.status_code,
    )


app = create_app()
