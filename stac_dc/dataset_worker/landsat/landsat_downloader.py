import json
import logging
import threading

from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from stac_dc.storage.s3 import S3
from stac_dc.catalogue.stac import STAC

from .usgs_m2m_connector import USGSM2MConnector

from downloaded_file import DownloadedFile

from env import env


class LandsatDownloader:
    """
    Class representing the Landsat downloader package
    """

    """
    Filename of a .json file which contains information of last day that was downloaded.
    By default located in s3 bucket landsat/last_downloaded_day.json
    """
    _last_downloaded_day_filename = 'last_downloaded_day.json'

    def __init__(
            self,
            demanded_datasets=env.get_landsat()['demanded_datasets'],
            s3_host=env.get_landsat()['s3_host'],
            s3_access_key=env.get_landsat()['s3_access_key'],
            s3_secret_key=env.get_landsat()['s3_secret_key'],
            s3_host_bucket=env.get_landsat()['s3_host_bucket'],
            stac_host=env.get_landsat()['stac_host'],
            stac_username=env.get_landsat()['stac_username'],
            stac_password=env.get_landsat()['stac_password'],
            logger=logging.getLogger(env.get_app__name()),
    ):
        """
        __init__
        :param demanded_datasets: Datasets demanded to download
        :param m2m_username: Username used to log into USGS M2M API
        :param m2m_token: Login token for USGS M2M API (Generated here: https://ers.cr.usgs.gov/)
        :param stac_username: Username used for publishing features into STAC API
        :param stac_password: Password of STAC API
        :param s3_host: URL of a S3 into which the downloaded data is registered
        :param s3_access_key: Access key for S3 bucket
        :param s3_secret_key: Secret key for S3 bucket
        :param s3_host_bucket: S3 host bucket (by default it should be "landsat")
        :param logger:
        :param stac_asset_download_root: URL of a host on which the http_server script is running
        """

        self._logger = logger

        self._logger.debug("Downloader initializing")

        self._demanded_datasets = demanded_datasets

        self._m2m_api_connector = USGSM2MConnector()
        self._stac_connector = STAC(
            username=stac_username,
            password=stac_password,
            stac_host=stac_host
        )
        self._s3_connector = S3(
            s3_host=s3_host,
            access_key=s3_access_key, secret_key=s3_secret_key,
            host_bucket=s3_host_bucket
        )


        self._logger.debug("Downloader initialized")

    def _get_last_downloaded_day(self):
        """
        Method reads date of last downloaded day from S3 storage
        :return: datetime of last downloaded day
        """

        with TemporaryDirectory() as temporary_directory:
            download_to = Path(temporary_directory).joinpath(self._last_downloaded_day_filename)

            self._s3_connector.download_file(
                download_to, self._last_downloaded_day_filename
            )

            with open(download_to) as last_downloaded_day_file:
                last_downloaded_day = datetime.strptime(
                    json.load(last_downloaded_day_file)['last_downloaded_day'],
                    "%Y-%m-%d"
                ).date()

        return last_downloaded_day

    def _update_last_downloaded_day(self, day):
        """
        Method updates the file with date of last downloaded day on S3

        :param day: datetime of last downloaded day
        :return: nothing
        """

        if self._last_downloaded_day > day:
            return

        last_downloaded_day_dict = {"last_downloaded_day": day.strftime("%Y-%m-%d")}

        with TemporaryDirectory() as temporary_directory:
            local_file = Path(temporary_directory).joinpath(self._last_downloaded_day_filename)
            local_file.touch(exist_ok=True)

            with open(local_file, "w") as last_downloaded_day_file:
                json.dump(last_downloaded_day_dict, last_downloaded_day_file)

            self._s3_connector.upload_file(str(local_file), self._last_downloaded_day_filename)

    def _create_array_of_downloadable_days(self, date_from, date_to):
        """
        Method creates an array of days which are meant to be downloaded by input parameters

        :param date_from: datetime of first day meant to be downloaded
        :param date_to: datetime of last day meant to be downloaded
        :return: array of datetimes
        """

        downloadable_days = []

        while date_from < date_to:
            date_from = date_from + timedelta(days=1)
            downloadable_days.append(date_from)

        return downloadable_days

    def _get_downloadable_days(self):
        """
        Method creates a date range from the day which must be downloaded first to the day which must be downloaded
        last. Then using method _create_array_of_downloadable_days() method generates an array of all days which are
        meant to be downloaded.
        Everytime at least four weeks are being downloaded.

        :return: array of datetime
        """

        should_be_checked_since = datetime.now(timezone.utc).date() - timedelta(weeks=4)
        self._last_downloaded_day = self._get_last_downloaded_day()

        if self._last_downloaded_day < should_be_checked_since:
            date_from = self._last_downloaded_day
        else:
            date_from = should_be_checked_since

        downloadable_days = self._create_array_of_downloadable_days(date_from, datetime.now(timezone.utc).date())

        return downloadable_days

    def run(self):
        """
        Main worker of LandsatDownloader class.
        Method prepares array of days which are in need of downloaading and dictionary of geojsons for which we
        are downloading files. These geojsons must be saved in ./geojson directory.
        Then for every demanded day, dataset and geojson this method prepares M2M API scenes and retrieves URLs of
        available dataset.
        For every URL is created standalone instance of DownloadedFile class in which the method process() is executed
        in threads.
        When all the data for one of the demanded days is downloaded, this method invokes _update_last_downloaded_day()
        and updates the last downloaded day accordingly.

        :return: nothing
        """

        days_to_download = self._get_downloadable_days()

        """
        Preparing the dict of demanded geojsons
        """
        geojsons = {}
        geojson_files_paths = [Path(geojson_file) for geojson_file in Path('geojson').glob("*")]
        for geojson_file_path in geojson_files_paths:
            with open(geojson_file_path, 'r') as geojson_file:
                geojsons.update({geojson_file_path: json.loads(geojson_file.read())})

        for day in days_to_download:  # For each demanded day...
            for dataset in self._demanded_datasets:  # ...each demanded dataset...
                for geojson_key in geojsons.keys():  # ...and each demanded geojson...
                    self._logger.info(
                        f"Request for download dataset: {dataset}, location: {geojson_key}, " +
                        f"date_start: {day}, date_end: {day}."
                    )

                    downloadable_files_attributes = self._m2m_api_connector.get_downloadable_files(
                        dataset=dataset, geojson=geojsons[geojson_key], time_start=day, time_end=day
                    )

                    downloaded_files = []
                    for downloadable_file_attributes in downloadable_files_attributes:
                        downloaded_files.append(
                            DownloadedFile(
                                attributes=downloadable_file_attributes,
                                stac_connector=self._stac_connector,
                                s3_connector=self._s3_connector,
                                logger=self._logger
                            )
                        )

                    threads = []  # Into this list we will save all the threads that we will run

                    for downloaded_file in downloaded_files:
                        threads.append(
                            threading.Thread(
                                target=downloaded_file.process,
                                name=f"Thread-{downloaded_file.get_display_id()}"
                            )
                        )  # Preparing threads to be executed

                    started_threads = []  # There are no started threads
                    for thread in threads:
                        if len(started_threads) < 10:  # If there is less than 10 started threads...
                            thread.start()  # ...start a new one...
                            started_threads.append(thread)  # ...and add it into the list of started threads

                        else:  # If there is 10 or more started threads, we can not start a new thread...
                            for started_thread in started_threads:
                                started_thread.join()  # ...so we wait for those started threads to finish...

                            started_threads = []  # ...then we clear the array of started threads...

                            thread.start()  # ...start the thread we wanted to start as eleventh thread...
                            started_threads.append(thread)  # ...and add it to the list of started threads.

                    for thread in threads:
                        thread.join()  # In the end we will wait for all the threads to finish

                    self._m2m_api_connector.scene_list_remove()

                    for downloaded_file in downloaded_files:
                        if downloaded_file.exception_occurred is not None:
                            raise downloaded_file.exception_occurred

            self._update_last_downloaded_day(day)
