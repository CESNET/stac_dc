from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin

import httpx

from .catalogue import Catalogue
from .catalogue_item import CatalogueItem
from .exceptions import *


class STAC(Catalogue):
    def __init__(
            self,
            stac_host: str,
            username: str,
            password: str,
            collection: str,
            **kwargs
    ):
        super().__init__(collection=collection, **kwargs)

        if not stac_host:
            raise STACHostNotSpecified()

        self._stac_host = stac_host
        self._username = username
        self._password = password
        self._token: str | None = None
        self._token_valid_until = datetime.fromtimestamp(0, tz=timezone.utc)

    # ------------------------
    # Public API
    # ------------------------
    def register_item(self, item: CatalogueItem) -> str:
        self._ensure_token()

        response = self._request(
            method="POST",
            endpoint=f"collections/{self._collection}/items",
            payload=item.to_stac(),
        )

        if response.status_code == 409:
            item_id = self._extract_conflict_id(response)
            self._logger.warning(f"Replacing existing STAC item {item_id}")
            self.delete_item(item_id)
            return self.register_item(item)

        if response.status_code != 200:
            raise STACRequestNotOK(status_code=response.status_code)

        item_id = self._extract_item_id(response)
        self._logger.info(f"STAC item registered: {item_id}")
        return item_id

    def delete_item(self, item_id: str) -> None:
        self._ensure_token()

        response = self._request(
            method="DELETE",
            endpoint=f"/collections/{self._collection}/items/{item_id}",
        )

        if response.status_code != 200:
            raise STACRequestNotOK(status_code=response.status_code)

    # ------------------------
    # Internals
    # ------------------------
    def _ensure_token(self):
        if self._token_valid_until < datetime.now(tz=timezone.utc):
            self._login()

    def _login(self):
        if not self._username or not self._password:
            raise STACCredentialsNotProvided()

        url = urljoin(self._stac_host, "auth")
        response = httpx.get(url, auth=(self._username, self._password), timeout=10)

        if response.status_code != 200:
            raise STACRequestNotOK(status_code=response.status_code)

        token = response.json().get("token")
        if not token:
            raise STACTokenNotObtainedError()

        self._token = token
        self._token_valid_until = datetime.now(tz=timezone.utc) + timedelta(hours=12)
        self._logger.info("Authenticated with STAC API.")

    def _request(self, method: str, endpoint: str, payload=None) -> httpx.Response:
        url = urljoin(self._stac_host, endpoint)
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/json"
        }
        return httpx.request(
            method=method,
            url=url,
            json=payload,
            headers=headers,
            timeout=15,
        )

    @staticmethod
    def _extract_item_id(response: httpx.Response) -> str:
        try:
            content = response.json()
            return content["features"][0]["featureId"]
        except Exception:
            raise STACError("Invalid STAC response format")

    @staticmethod
    def _extract_conflict_id(response: httpx.Response) -> str:
        try:
            return response.json()["ErrorMessage"].split(" ")[1]
        except Exception:
            raise STACError("Conflict detected but item ID not found")
