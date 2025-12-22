import logging

from abc import ABC, abstractmethod


class Catalogue(ABC):
    def __init__(self, logger: logging.Logger, collection: str):
        self._collection = collection
        self._logger = logger

    @abstractmethod
    def register_item(self, json_data: str | dict) -> str:
        pass
