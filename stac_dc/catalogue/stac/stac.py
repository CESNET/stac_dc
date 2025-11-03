import httpx
import json
import logging
import random
import time

from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin

from env import env
from .exceptions import *


class STAC:
    _username: str
    _password: str
    _stac_token: str | None = None
    _api_token_valid_until: datetime = datetime.fromtimestamp(0, tz=timezone.utc)

    def __init__(self, username: str, password: str, stac_host: str, logger=None):
        if stac_host is None:
            raise STACHostNotSpecified()

        self._stac_host = stac_host
        self._username = username
        self._password = password
        self._logger = logger or logging.getLogger(env.get_app__name())

    # ------------------------
    # Public API
    # ------------------------

    def register_item(self, json_data: str | dict, dataset: str) -> str:
        self._ensure_token()
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}

        if isinstance(json_data, dict):
            payload = json_data
        else:
            payload = json.loads(json_data)

        response = self._send_request(
            f"/collections/{dataset}/items",
            payload=payload,
            headers=headers,
            method="POST",
        )

        if response.status_code != 200:
            raise STACRequestNotOK(status_code=response.status_code)

        content = response.json()
        errors = content.get("errors")
        if errors:
            error = errors[0]
            if error["code"] == 409:
                feature_id = error["error"].split(" ")[1]
                self.delete_stac_item(dataset, feature_id)
                return self.register_item(payload, dataset)

            raise Exception(f"{error['error']}")

        feature_id = content["features"][0]["featureId"]
        self._logger.info(f"STAC item registered: {feature_id}")
        return feature_id

    def delete_stac_item(self, dataset: str, feature_id: str):
        self._logger.info(f"Deleting STAC item {feature_id} from dataset {dataset}")
        headers = {'Accept': 'application/json'}
        response = self._send_request(
            f"/collections/{dataset}/items/{feature_id}",
            headers=headers,
            method="DELETE",
        )
        if response.status_code != 200:
            raise STACRequestNotOK(status_code=response.status_code)

    # ------------------------
    # Private helpers
    # ------------------------

    def _ensure_token(self):
        if self._api_token_valid_until < datetime.now(tz=timezone.utc):
            self._login()

    def _login(self):
        if not self._username or not self._password:
            raise STACCredentialsNotProvided()

        url = urljoin(self._stac_host, "auth")
        response = httpx.post(url, auth=(self._username, self._password), timeout=10)

        if response.status_code != 200:
            raise STACRequestNotOK(status_code=response.status_code)

        content = response.json()
        token = content.get("token")
        if not token:
            raise STACTokenNotObtainedError()

        self._stac_token = token
        self._api_token_valid_until = datetime.now(tz=timezone.utc) + timedelta(hours=12)
        self._logger.info("Authenticated with STAC API.")

    def _send_request(self, endpoint: str, payload=None, headers=None, method="GET", retries=5) -> httpx.Response:
        headers = headers or {}
        payload = payload or {}
        url = urljoin(self._stac_host, endpoint)
        headers['Authorization'] = f"Bearer {self._stac_token}"

        for attempt in range(1, retries + 1):
            try:
                self._logger.debug(f"{method} {url} (attempt {attempt})")
                response = httpx.request(method, url, json=payload, headers=headers, timeout=15)
                return response

            except httpx.RequestError as e:
                wait = random.uniform(3, 6)
                self._logger.warning(f"Request failed ({e}), retrying in {wait:.1f}s...")
                time.sleep(wait)

        raise STACRequestTimeout(retry=retries, max_retries=retries)
