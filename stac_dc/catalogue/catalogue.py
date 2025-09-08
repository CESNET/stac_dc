from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from stac_dc.dataset_worker import DatasetWorker
    from stac_dc.dataset_worker.aoi import AOI

import logging

from abc import ABC, abstractmethod
from datetime import date


class Catalogue(ABC):
    def __init__(self, logger: logging.Logger):
        self._logger = logger

    @abstractmethod
    def register_item(
            self,
            worker: "DatasetWorker", dataset: str, day: date, aoi: "AOI", assets: list[dict]
    ) -> tuple[str, str, str]:
        """
        Method registers item into catalogue

        :param worker:
        :param dataset:
        :param day:
        :param aoi:
        :param assets:
        :return: Returns tuple(str: catalogue_id, str: catalogue_item_contents, str: catalogue_item_contents_format)
        """
        pass
