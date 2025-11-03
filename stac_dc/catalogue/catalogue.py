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
    def register_item(self, json_data: str | dict, dataset: str) -> str:
        pass
