import httpx
import json
import logging
import os
import random
import re
import time

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.parse import urlparse

from env import env
from .exceptions.usgs_m2m_connector import *


class USGSM2MConnector:
    """
    Client for interacting with the USGS M2M API for Landsat data.

    Much of the workflow is inspired by Angel Farguell's m2m-api (GitHub: https://github.com/Fergui/m2m-api)
    Thanks!
    """

    _logger: logging.Logger = None
    _api_url: str = None
    _username: str = None
    _scene_label: str = None
    _token: str = None
    _api_token: str = None
    _api_token_valid_until: datetime = datetime.now(timezone.utc)

    def __init__(
            self,
            api_url: str = env.get_landsat()['m2m_api_url'],
            username: str = env.get_landsat()['m2m_username'],
            token: str = env.get_landsat()['m2m_token'],
            scene_label: str = env.get_landsat()['m2m_scene_label'],
            logger: logging.Logger = logging.getLogger(env.get_app__name()),
    ):
        self._api_url = api_url
        self._username = username
        self._token = token
        self._scene_label = scene_label

        self._logger = logger

        self._login_token()

    def _login_token(self):
        """
        Obtains the M2M API access token using the user's username and login token.
        """

        if not self._username or not self._token:
            raise USGSM2MCredentialsNotProvided()

        self._api_token = None

        # Set expiration to 2 hours
        self._api_token_valid_until = datetime.now(timezone.utc) + timedelta(hours=2)

        api_payload = {
            "username": self._username,
            "token": self._token
        }

        response_content = self._send_request('login-token', api_payload)
        response_data = json.loads(response_content)
        self._api_token = response_data.get('data')

        if not self._api_token:
            raise USGSM2MTokenNotObtainedException()

        self._logger.info("Successfully obtained M2M API access token.")

    def _refresh_token_if_expired(self):
        """
        Refreshes the API token if expired
        """

        if self._api_token_valid_until < datetime.now(timezone.utc):
            self._login_token()

    def _scene_search(
            self,
            dataset: str, geojson: dict, day_start: datetime, day_end: datetime, max_results: int = 10000
    ) -> Dict:
        """
        Searches for relevant scenes for a given dataset, GeoJSON polygon, and date range.
        Returns a dictionary containing search results.
        """

        api_payload = {
            "maxResults": max_results,
            "datasetName": dataset,
            "sceneFilter": {
                "spatialFilter": {
                    "filterType": "geojson",
                    "geoJson": geojson
                },
                "acquisitionFilter": {
                    "start": str(day_start),
                    "end": str(day_end)
                }
            }
        }

        response_content = self._send_request('scene-search', api_payload)
        scenes = json.loads(response_content)
        return scenes.get('data', {})

    def _scene_list_add(self, dataset_name: str, entity_ids: List[str]):
        """
        Adds scenes to a scene list defined by a label in the M2M API.
        """

        api_payload = {
            "listId": self._scene_label,
            "datasetName": dataset_name,
            "idField": "entityId",
            "entityIds": entity_ids
        }

        self._send_request('scene-list-add', api_payload)

        self._logger.info(f"Added {len(entity_ids)} scenes to scene list '{self._scene_label}'.")

    def scene_list_remove(self):
        """
        Removes a scene list from the M2M API if it exists.
        """

        try:
            api_payload = {"listId": self._scene_label}
            self._send_request('scene-list-remove', api_payload)
            self._logger.info(f"Successfully removed scene list '{self._scene_label}'.")

        except Exception as e:
            self._logger.warning(f"Failed to remove scene list '{self._scene_label}': {str(e)}")

    def _download_options(self, dataset: str) -> List[Dict]:
        """
        Retrieves available download options (URLs, file sizes, etc.) from the M2M API.
        Only returns options that are available and use a supported download system.
        """

        api_payload = {
            "listId": self._scene_label,
            "datasetName": dataset,
            "includeSecondaryFileGroups": "true"
        }

        response_content = self._send_request('download-options', api_payload)
        download_options = json.loads(response_content)

        # Filter for available downloads from specific download systems.
        supported_systems = ['dds', 'dds_ms', 'ls_zip']
        filtered_options = [
            download_option for download_option in download_options.get('data', []) \
            if download_option.get('available') and download_option.get('downloadSystem') in supported_systems
        ]

        self._logger.info(f"Found {len(filtered_options)} valid download options.")

        return filtered_options

    def _unique_urls(self, available_urls: List[Dict]) -> List[Dict]:
        """
        Removes duplicate URLs from a list of download dictionaries.
        """

        return list({url_dict['url']: url_dict for url_dict in available_urls}.values())

    def _download_request(self, download_options: List[Dict]) -> List[Dict]:
        """
        Initiates a download request for a list of download options and waits until
        all corresponding URLs are available.
        """

        available_urls = []
        options_to_process = download_options[:]

        while options_to_process:
            preparing_urls = []

            for option in options_to_process:
                api_payload = {
                    "downloads": [
                        {
                            "entityId": option['entityId'],
                            "productId": option['id']
                        }
                    ]
                }

                try:
                    response_content = self._send_request('download-request', api_payload)
                    download_request = json.loads(response_content)
                    available_urls.extend([
                        {"entityId": option['entityId'], "productId": option['id'], "url": d['url']}
                        for d in download_request['data'].get('availableDownloads', [])
                    ])
                    preparing_urls.extend(download_request['data'].get('preparingDownloads', []))

                except Exception as e:
                    self._logger.warning(f"Failed to request download for entity {option['entityId']}: {e}")

            if not preparing_urls:
                break

            options_to_process = [
                option for option in options_to_process if
                any(prepared_url['entityId'] == option['entityId'] for prepared_url in preparing_urls)
            ]

            self._logger.info(f"Waiting for {len(preparing_urls)} downloads to be ready...")
            time.sleep(5)

        # Remove duplicates
        unique_urls = self._unique_urls(available_urls)
        if len(unique_urls) < len(download_options):
            raise USGSM2MDownloadRequestReturnedFewerURLs(
                entity_ids_count=len(download_options), urls_count=len(unique_urls)
            )

        return unique_urls

    def _get_list_of_files(
            self,
            download_options: List[Dict],
            entity_display_ids: Dict[str, str],
            time_start: datetime,
            time_end: datetime, dataset: str
    ) -> List[Dict]:
        """
        Retrieves downloadable URLs and enriches them with metadata.
        """

        downloadable_urls = self._download_request(download_options)

        for downloadable_url in downloadable_urls:
            downloadable_url.update({
                "displayId": entity_display_ids.get(downloadable_url['entityId']),
                "dataset": dataset,
                "start": time_start,
                "end": time_end
            })

        return downloadable_urls

    def download_file(
            self,
            url: str,
            output_dir: Path | str,
            chunk_size: int = 1024 * 1024,
            max_retries: int = 5,
            timeout: int = 60
    ) -> Tuple[Path, bool]:
        """
        Downloads a file from a given URL into the specified output directory.
        The filename is determined from the server's Content-Disposition header or, if missing, from the URL itself.
        """

        proper_filename = True

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        retry = 0
        while retry <= max_retries:
            try:
                if output_dir.is_absolute():
                    target_str = str(output_dir)
                else:
                    target_str = f"./{output_dir}"
                print(f"Downloading {url} into {target_str}")

                with httpx.stream("GET", url, follow_redirects=True, timeout=timeout) as response:
                    response.raise_for_status()

                    cd = response.headers.get("content-disposition")
                    filename = None
                    if cd:
                        match = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', cd, re.IGNORECASE)
                        if match:
                            filename = match.group(1)

                    if not filename:
                        proper_filename = False
                        path = Path(urlparse(url).path)
                        if path.name:
                            filename = path.name

                    if not filename:
                        proper_filename = False
                        filename = "downloaded_file"

                    output_path = output_dir / filename

                    with output_path.open("wb") as f:
                        for chunk in response.iter_bytes(chunk_size=chunk_size):
                            f.write(chunk)

                self._logger.info(f"Downloaded {url} into {output_path}")

                return output_path, proper_filename

            except httpx.HTTPStatusError as e:
                print(f"HTTP error during download: {e.response.status_code}")
                raise

            except (httpx.RequestError, IOError) as e:
                retry += 1
                print(f"Download failed ({retry}/{max_retries}): {e}")

                if retry > max_retries:
                    raise USGSM2MDownloadRequestFailed(url=url)

                time.sleep((1 + random.random()) * 5)

        raise USGSM2MDownloadRequestFailed(url=url)

    def get_downloadable_files(
            self, dataset: str, geojson: dict, time_start: datetime, time_end: datetime
    ) -> List[Dict]:
        """
        Main public method to get a list of downloadable files and their metadata.
        """

        self.scene_list_remove()
        scenes = self._scene_search(dataset, geojson, time_start, time_end)

        if not scenes.get('results'):
            self._logger.info("No scenes found for the specified criteria.")
            return []

        entity_display_ids = {result['entityId']: result['displayId'] for result in scenes['results']}

        self._logger.info(
            f"Total hits: {scenes.get('totalHits', 0)}, records returned: {scenes.get('recordsReturned', 0)}"
        )

        self._scene_list_add(dataset, list(entity_display_ids.keys()))

        download_options = self._download_options(dataset)

        downloadable_files = self._get_list_of_files(
            download_options, entity_display_ids, time_start, time_end, dataset
        )

        for downloadable_file in downloadable_files:
            downloadable_file.update({'geojson': geojson})

        return downloadable_files

    def _send_request(self, endpoint: str, payload_dict: dict = None, max_retries: int = 5, timeout=60) -> bytes | None:
        """
        Sends an HTTP POST request to the specified M2M API endpoint using httpx.
        Handles authentication, retries, and error handling.
        """

        if payload_dict is None:
            payload_dict = {}

        payload_json = json.dumps(payload_dict)

        endpoint_full_url = os.path.join(self._api_url, endpoint)

        headers = {}

        # Refresh token if expired
        if endpoint not in ['login', 'login-token']:
            self._refresh_token_if_expired()
            headers['X-Auth-Token'] = self._api_token

        with httpx.Client(timeout=timeout) as client:
            return self._retry_request(client, endpoint_full_url, payload_json, max_retries, headers)

    def _retry_request(
            self, client: httpx.Client, endpoint: str, payload: str, max_retries: int, headers: dict
    ) -> bytes | None:
        """
        Retries a POST request with a delay on failure.
        """
        retry = 0

        while retry <= max_retries:
            try:
                payload_to_log = '=== Contains secret, not logged! ===' if 'login' in endpoint else payload
                self._logger.info(
                    f"Sending request to {endpoint}. Attempt: {retry + 1}/{max_retries + 1}. Payload: {payload_to_log}"
                )

                response = client.post(endpoint, content=payload, headers=headers)
                response.raise_for_status()

                return response.content

            except httpx.RequestError as e:
                retry += 1
                self._logger.warning(f"Request failed: {e}. Retrying...")

                if retry > max_retries:
                    raise USGSM2MRequestTimeout(retry=retry, max_retries=max_retries)

                time.sleep((1 + random.random()) * 5)

            except httpx.HTTPStatusError as e:
                self._logger.error(f"Received HTTP status error {e.response.status_code}: {e.response.text}")
                raise USGSM2MRequestNotOK(status_code=e.response.status_code, response_text=e.response.text)

        return None
