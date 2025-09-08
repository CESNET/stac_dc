import json
import random
import time
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urljoin

import httpx
import logging

from env import env
from .exceptions import *

from stac_dc.dataset_worker.aoi import AOI
from stac_dc.catalogue import Catalogue
from stac_dc.dataset_worker import DatasetWorker


class STAC(Catalogue):
    _base_dir_path: Path
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
        self._base_dir_path = Path(__file__).resolve().parent
        self._logger = logger or logging.getLogger(env.get_app__name())

        super().__init__(logger=self._logger)

    def register_item(
            self,
            worker: DatasetWorker, dataset: str, day: date, aoi: AOI, assets: list[dict]
    ) -> tuple[str, str, str]:
        feature_json = self._prepare_feature(worker, dataset, day, aoi, assets)
        feature_id = self.register_stac_item(feature_json, dataset)
        return feature_id, feature_json, "json"

    # ------------------------
    # Feature JSON Handling
    # ------------------------
    def _load_feature_json(self, dataset: str) -> dict:
        feature_path = self._base_dir_path / "json" / f"[feature]{dataset}.json"
        with open(feature_path) as f:
            return json.load(f)

    def _populate_feature_dict(
            self, feature_dict: dict, worker: DatasetWorker, day: date, aoi: AOI, assets: list[dict]
    ) -> dict:
        feature = feature_dict['features'][0]
        feature['id'] = worker.get_id(day)
        feature['bbox'] = aoi.get_bbox()
        feature['geometry']['coordinates'] = aoi.get_polygon()
        feature['properties'].update({
            'start_datetime': f"{day}T00:00:00Z",
            'end_datetime': f"{day}T23:59:59Z",
            'datetime': f"{day}T00:00:00Z"
        })

        for asset in assets:
            url = f"{worker.get_catalogue_download_host()}/{asset['href']}"
            protocol, rest = url.split("://", 1)
            rest = re.sub(r'/+', '/', rest)
            clean_url = f"{protocol}://{rest}"

            key = f"{asset['product_type'].replace('_', '-')}-{asset['data_format']}"
            feature['assets'][key]['href'] = clean_url

        # Remove empty assets
        feature['assets'] = {k: v for k, v in feature['assets'].items() if v.get('href')}

        return feature_dict

    def _prepare_feature(self, worker: DatasetWorker, dataset: str, day: date, aoi: AOI, assets: list[dict]) -> str:
        feature_dict = self._load_feature_json(dataset)
        feature_dict = self._populate_feature_dict(feature_dict, worker, day, aoi, assets)
        return json.dumps(feature_dict, indent=2)

    # ------------------------
    # HTTP requests
    # ------------------------
    def _send_request(
            self,
            endpoint: str,
            headers=None,
            payload: dict | None = None,
            method: str = "GET",
            max_retries: int = 5
    ) -> httpx.Response:
        headers = headers or {}
        payload = payload or {}

        url = urljoin(self._stac_host, endpoint)

        if 'auth' not in endpoint:
            if self._api_token_valid_until < datetime.now(tz=timezone.utc):
                self._login()
            headers['Authorization'] = f"Bearer {self._stac_token}"

        return self._retry_request(url, payload, headers, method, max_retries)

    def _retry_request(self, url, payload, headers, method, max_retries, timeout=10, sleep_base=5) -> httpx.Response:
        retry = 1
        while retry <= max_retries:
            self._logger.info(f"{method} request to {url} (Retry {retry}/{max_retries})")
            try:
                if 'auth' in url:
                    response = httpx.get(url, auth=(payload['username'], payload['password']), timeout=timeout)
                else:
                    response = httpx.request(method, url, json=payload, headers=headers, timeout=timeout)

                return response

            except httpx.RequestError as e:
                retry += 1
                sleep_time = (1 + random.random()) * sleep_base
                self._logger.warning(f"Request failed: {e}. Sleeping {sleep_time:.2f}s before retry.")
                time.sleep(sleep_time)

        raise STACRequestTimeout(retry=retry, max_retries=max_retries)

    # ------------------------
    # Authentication
    # ------------------------
    def _login(self, username: str | None = None, password: str | None = None):
        self._username = username or self._username
        self._password = password or self._password

        if not self._username or not self._password:
            raise STACCredentialsNotProvided()

        self._api_token_valid_until = datetime.now(tz=timezone.utc) + timedelta(hours=12)

        response = self._send_request(
            endpoint='auth',
            payload={'username': self._username, 'password': self._password},
            method="POST"
        )

        if response.status_code != 200:
            raise STACRequestNotOK(status_code=response.status_code)

        content = response.json()
        self._stac_token = content.get('token')
        if not self._stac_token:
            raise STACTokenNotObtainedError()

    # ------------------------
    # STAC Item Management
    # ------------------------
    def _delete_stac_item(self, dataset: str, feature_id: str):
        self._logger.info(f"Deleting STAC item {feature_id} from dataset {dataset}")
        headers = {'Accept': 'application/json'}
        response = self._send_request(f"/collections/{dataset}/items/{feature_id}", headers=headers, method="DELETE")
        if response.status_code != 200:
            raise STACRequestNotOK(status_code=response.status_code)

    def register_stac_item(self, json_string: str, dataset: str) -> str:
        self._logger.info(f"Registering STAC item to dataset {dataset}")
        payload = json.loads(json_string)
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}

        response = self._send_request(f"/collections/{dataset}/items", payload=payload, headers=headers, method="POST")

        if response.status_code != 200:
            raise STACRequestNotOK(status_code=response.status_code)

        content = response.json()
        errors = content.get('errors')
        if errors:
            error = errors[0]
            if error['code'] == 409:
                self._delete_stac_item(dataset, error['error'].split(' ')[1])
                return self.register_stac_item(json_string, dataset)
            raise Exception(f"{error['error']}")

        feature_id = content['features'][0]['featureId']
        self._logger.info(f"STAC item registered. Assigned featureId: {feature_id}.")
        return feature_id
