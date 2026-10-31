"""Header authentication for the proxy's public HTTP entry points."""
from __future__ import annotations

import hmac

from flask import Blueprint, Flask, jsonify, request


PUBLIC_ENDPOINTS = frozenset({
    "health", "latest_geojson", "latest_compact_geojson",
    "latest_fme_geojson", "latest_7d_geojson",
})


def install_api_key_auth(target: Flask | Blueprint, api_key: str | None) -> None:
    if not api_key:
        raise RuntimeError("Missing required environment variable PROXY_API_KEY")
    expected = api_key.encode("utf-8")

    def is_public_request() -> bool:
        # OPTIONS is CORS preflight only and returns no protected data.
        return request.method == "OPTIONS" or (
            request.method in {"GET", "HEAD"}
            and (request.endpoint or "").rsplit(".", 1)[-1] in PUBLIC_ENDPOINTS
        )

    @target.before_request
    def check_api_key():
        if is_public_request():
            return None
        supplied = request.headers.get("X-API-Key", "").encode("utf-8")
        if not hmac.compare_digest(supplied, expected):
            return jsonify({"error": "Unauthorized"}), 401
        return None

    @target.after_request
    def protect_response_cache(response):
        if not is_public_request():
            # Authentication runs before views and their caches. Do not allow
            # shared/browser HTTP caches to reuse responses without the header.
            response.headers["Cache-Control"] = "private, no-store"
            response.headers.pop("Expires", None)
            response.vary.add("X-API-Key")
        return response
