from abc import ABC, abstractmethod

import logging

from env import env as env
from .catalogue_item import CatalogueItem


class Catalogue(ABC):
    def __init__(self, collection: str, logger: logging.Logger = None):
        if not collection:
            raise ValueError("Collection must be specified at construction")
        self._collection = collection.strip("/")

        if logger is None:
            logger = logging.getLogger(env.get_app__name())
        self._logger = logger

    @abstractmethod
    def register_item(self, item: CatalogueItem) -> str:
        raise NotImplementedError

    @abstractmethod
    def delete_item(self, item_id: str) -> None:
        raise NotImplementedError
