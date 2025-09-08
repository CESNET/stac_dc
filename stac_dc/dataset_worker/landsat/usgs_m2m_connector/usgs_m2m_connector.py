import json
import logging
import os
import random
import time
import requests

from datetime import datetime, timedelta, timezone

from env import env

from .exceptions.usgs_m2m_connector import *


class USGSM2MConnector:
    _logger: logging.Logger = None

    _api_url: str = None
    _username: str = None
    _token: str = None

    def __init__(
            self,
            api_url=env.get_landsat()['m2m_api_url'],
            username=env.get_landsat()['m2m_username'],
            token=env.get_landsat()['m2m_token'],
            logger=logging.getLogger(env.get_app__name()),
    ):
        self._api_url = api_url
        self._username = username
        self._token = token

        self._logger = logger

        self._login_token()

    def _login_token(self):
        """
        Method is used for obtaining the M2M API access token using user's username and login token

        :return: None, the value of M2M API access token is stored in self._api_token
        """
        if (self._username is None) or (self._token is None):
            raise USGSM2MCredentialsNotProvided()

        self._api_token = None
        self._api_token_valid_until = datetime.now(timezone.utc) + timedelta(hours=2)

        api_payload = {
            "username": self._username,
            "token": self._token
        }

        response = self._send_request('login-token', api_payload)
        response_content = json.loads(response)

        self._api_token = response_content['data']

        if self._api_token is None:
            raise USGSM2MTokenNotObtainedException()

    def _scene_search(self, dataset, geojson, day_start, day_end):
        """
        Method prepares M2M API payload dictionary for obtaining the relevant scenes for dataset,
        polygon, and date range.

        :param dataset: string: name of demanded dataset
        :param geojson: dictionary representation of GeoJSON polygon
        :param day_start: datetime: date from which the scenes ought to be demanded
        :param day_end: datetime: date to which the scenes ought to be demanded
        :return: dict of scene-search api endpoint (https://m2m.cr.usgs.gov/api/docs/reference/#scene-search)
        """

        api_payload = {
            "maxResults": 10000,
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

        response = self._send_request('scene-search', api_payload)
        scenes = json.loads(response)

        return scenes['data']

    def _scene_list_add(self, label, dataset_name, entity_ids):
        """
        Method adds scenest to M2M API scene list defined by label

        :param label: string: label of M2M API scene list into which demandes datasets and entity-ids are added
        :param dataset_name: string: name of demanded dataset
        :param entity_ids: list of entity_ids
        :return: None
        """

        api_payload = {
            "listId": label,
            "datasetName": dataset_name,
            "idField": "entityId",
            "entityIds": entity_ids
        }

        self._send_request('scene-list-add', api_payload)

    def scene_list_remove(self, label=env.get_landsat()['m2m_scene_label']):
        """
        Removes scene list defined by label name from M2M API

        :param label: string: name (label) of M2M API scene list
        :return: None
        """

        api_payload = {
            "listId": label
        }

        self._send_request('scene-list-remove', api_payload)

    def _download_options(self, label, dataset):
        """
        Method retrieves download options (mainly URLs and filesizes) from M2M API

        :param label: string: label of the M2M API scene which we are working with
        :param dataset: dataset for which we are requesting download options
        :return: list of download options returned from M2M API
        """

        api_payload = {
            "listId": label,
            "datasetName": dataset,
            "includeSecondaryFileGroups": "true"
        }

        response = self._send_request('download-options', api_payload)
        download_options = json.loads(response)

        """
        # Fixed below
        filtered_download_options = [do for do in download_options['data'] if do['downloadSystem'] == 'dds']
        """

        filtered_download_options = []
        for download_option in download_options['data']:
            if download_option['downloadSystem'] == 'dds' and download_option['available'] == True:
                filtered_download_options.append(download_option)
            elif download_option['downloadSystem'] == 'dds_ms' and download_option['available'] == True:
                filtered_download_options.append(download_option)
            elif download_option['downloadSystem'] == 'ls_zip' and download_option['available'] == True:
                filtered_download_options.append(download_option)

        return filtered_download_options

    def _unique_urls(self, available_urls):
        """
        Uniques list of URLs available for downloading. Every URL in list will be in resulting list only once
        :param available_urls: list of URLs available for downloading. But one URL can be in this list multiple times
        :return: list of unique available (downloadable) URLs
        """

        unique_urls = list({url_dict['url']: url_dict for url_dict in available_urls}.values())
        return unique_urls

    def _download_request(self, download_options):
        """
        Method calls download-request M2M API endpoint (https://m2m.cr.usgs.gov/api/docs/reference/#download-request)
        Method is primarily obtaining URLs that are available for download.

        :param download_options: list of download options retreived by self._download_options()
        :return: list of unique URLs that are available for downloading
        """

        # Resulting list of available URLs
        available_urls = []

        # Repeat until break... Well until we won't append any URLs into preparing_urls list. That means that all
        # available URLs ale appended into available_urls list
        while True:
            # Some of the URLs may not be ready for downloading yet. Let's store theme somewhere else
            preparing_urls = []

            # for every download-option...
            for download_option in download_options:
                api_payload = {
                    "downloads": [
                        {
                            "entityId": download_option['entityId'],
                            "productId": download_option['id']
                        }
                    ]
                }

                response = self._send_request('download-request', api_payload)
                download_request = json.loads(response)

                # ...append all of its already available URLs into list of available URLs...
                for available_download in download_request['data']['availableDownloads']:
                    available_urls.append(
                        {
                            "entityId": download_option['entityId'],
                            "productId": download_option['id'],
                            "url": available_download['url']
                        }
                    )

                # ...and URLs that are not available for downloading yet into list of preparing URLs...
                for preparing_download in download_request['data']['preparingDownloads']:
                    preparing_urls.append(
                        {
                            "entityId": download_option['entityId'],
                            "productId": download_option['id'],
                            "url": preparing_download['url']
                        }
                    )

            # If we did not append any URLs into preparing_urls list that means we have all possible URLs
            # in available_urls array, and thus we can break while cycle
            if not preparing_urls:
                break

            time.sleep(5)

        # Some URLs may have been added multiple times. We need to unique the array
        available_urls = self._unique_urls(available_urls)

        # If we have fewer available_urls than download_options then there is some download options for which there
        # should be URL available without any. Which is odd.
        if len(available_urls) < len(download_options):
            raise USGSM2MDownloadRequestReturnedFewerURLs(
                entity_ids_count=len(download_options), urls_count=len(available_urls)
            )

        return available_urls

    def _get_list_of_files(self, download_options, entity_display_ids, time_start, time_end, dataset):
        """
        Method retrieves list of downloadable URLs from self._download_request and appends displayId, dataset, and
        date range to those.

        :param download_options: download_options for demanded scenes (obtained by self._download_options)
        :param entity_display_ids: dictionary of entityIds and corresponding displayId
        :param time_start:
        :param time_end:
        :param dataset: demanded dataset
        :return: updated list of downloadable URLs
        """

        downloadable_urls = self._download_request(download_options)

        for downloadable_url in downloadable_urls:
            downloadable_url.update(
                {
                    "displayId": entity_display_ids[downloadable_url['entityId']],
                    "dataset": dataset,
                    "start": time_start,
                    "end": time_end
                }
            )

        return downloadable_urls

    def get_downloadable_files(
            self,
            dataset, geojson, time_start, time_end,
            label=env.get_landsat()['m2m_scene_label']
    ):
        """
        For specified dataset, geojson, daterange and scene label this method returns a list of downloadable files,
        and corresponding URLs

        :param dataset: demanded dataset
        :param geojson: polygon dict
        :param time_start:
        :param time_end:
        :param label: scene label
        :return: list[dict{}] of downloadable files

        Example returned structure:
        [
          {
            'entityId':'LC91940242024076LGN00',
            'productId':'632210d4770592cf',
            'url':'https://dds.cr.usgs.gov/download/eyJpZCI6NjA3Mzg1OTQyLCJjb250YWN0SWQiOjI2ODY2MzY0fQ==',
            'displayId':'LC09_L2SP_194024_20240316_20240317_02_T2',
            'dataset':'landsat_ot_c2_l2',
            'start':datetime.date(2024,3,16),
            'end':datetime.date(2024,3,16),
            'geojson':{
              'type':'Polygon',
              'coordinates':[
                [
                  [
                    12.09,
                    48.55
                  ],
                  [
                    18.87,
                    48.55
                  ],
                  [
                    18.87,
                    51.06
                  ],
                  [
                    12.09,
                    51.06
                  ],
                  [
                    12.09,
                    48.55
                  ]
                ]
              ]
            }
          }
        ]
        """

        self.scene_list_remove(label)

        scenes = self._scene_search(dataset, geojson, time_start, time_end)

        entity_display_ids = {result['entityId']: result['displayId'] for result in scenes['results']}

        self._logger.info(
            f"Total hits: {scenes['totalHits']}, records returned: {scenes['recordsReturned']}, " +
            f"returned IDs: {entity_display_ids}"
        )

        if not entity_display_ids:
            return []

        self._scene_list_add(label, dataset, list(entity_display_ids.keys()))

        download_options = self._download_options(label, dataset)

        downloadable_files = self._get_list_of_files(
            download_options, entity_display_ids, time_start, time_end, dataset
        )

        for downloadable_file in downloadable_files:
            downloadable_file.update({'geojson': geojson})

        return downloadable_files

    def _send_request(self, endpoint, payload_dict=None, max_retries=5):
        """
        Method sends HTTP request to specified URL endpoint

        :param endpoint: URL endpoint
        :param payload_dict: dict that will be converted to request JSON
        :param max_retries: number of retries, default 5
        :return: request.response.content
        """
        if payload_dict is None:
            payload_dict = {}

        endpoint_full_url = str(os.path.join(self._api_url, endpoint))
        payload_json = json.dumps(payload_dict)

        headers = {}

        if (endpoint != 'login') and (endpoint != 'login-token'):
            if self._api_token_valid_until < datetime.now(timezone.utc):
                self._login_token()

            headers['X-Auth-Token'] = self._api_token

        data = self._retry_request(endpoint_full_url, payload_json, max_retries, headers)

        if data.status_code != 200:
            raise USGSM2MRequestNotOK(status_code=data.status_code)

        return data.content

    def _retry_request(self, endpoint, payload, max_retries=5, headers=None, timeout=30, sleep=5):
        """
        Method sends request to specified endpoint until number of max_retries is reached
        For max_retries=5 the request is sent 6 times, since first (or the "zeroth") is understood as proper request.

        :param endpoint: URL of USGS M2M API endpoint
        :param payload: JSON string of a API payload
        :param max_retries: default 5 retries
        :param headers: dict of headers sent to M2M API endpoint
        :param timeout: default 10 seconds
        :param sleep: wait seconds between retries, default 5 seconds
        :return: request response, bytestring of response, can be parsed to JSON
        :raise USGSM2MRequestTimeout: when limit of max_retries is reached
        """

        if headers is None:
            headers = {}

        retry = 0
        while max_retries > retry:
            if 'login' in endpoint:
                payload_to_log = '=== Contains secret, not logged! ==='
            else:
                payload_to_log = payload

            self._logger.info(f"Sending request to URL {endpoint}. Retry: {retry}. Payload: {payload_to_log}")
            try:
                response = requests.post(endpoint, payload, headers=headers, timeout=timeout)
                return response

            except requests.exceptions.Timeout:
                retry += 1
                self._logger.warning(f"Connection timeout. Retry number {retry} of {max_retries}.")

                sleep = (1 + random.random()) * sleep
                time.sleep(sleep)

        raise USGSM2MRequestTimeout(retry=retry, max_retries=max_retries)
