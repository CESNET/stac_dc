import json
import logging

from typing import Tuple

from stactools.landsat import stac as stac_landsat

from .landsat_tar_utils import LandsatTarUtils

from .exceptions.landsat_processor import *

from env import env


class LandsatProcessor:
    _landsat_tar_utils: LandsatTarUtils = None

    _tar_path: Path = None
    _dataset: str = None
    _tar_indexes: dict = None
    _stac_json_dict: dict = None

    def __init__(
            self,
            landsat_tar_path: str | Path = None,
            dataset: str = None,
            logger: logging.Logger = logging.getLogger(env.get_app__name()),
    ):

        if landsat_tar_path is None:
            raise LandsatTarFileNotSpecifiedException()
        self._tar_path = landsat_tar_path

        if dataset is None:
            raise LandsatDatasetNotSpecified()
        self._dataset = dataset

        self._logger = logger

    def _load_stac_from_tar(self):
        stac_json_tar_members = [
            member for member in self._landsat_tar_utils.get_members()
            if "_stac.json".lower() in member.name.lower()
        ]

        if len(stac_json_tar_members) == 0:
            raise LandsatTarDoesNotContainStacFile(self._tar_path)

        stac_json_paths = []
        for stac_json_tar_member in stac_json_tar_members:
            stac_json_paths.append(self._landsat_tar_utils.untar_member(stac_json_tar_member))

        stac_json_dict_tmp = {}

        for stac_json_path in stac_json_paths:
            with open(stac_json_path, "r") as f:
                stac_json_dict_tmp.update({stac_json_path.name: json.load(f)})

            stac_json_path.unlink()

        final_stac_dict = stac_json_dict_tmp[list(stac_json_dict_tmp.keys())[0]]
        final_assets = {}
        final_description = ""

        for stac_json_dict in stac_json_dict_tmp.values():
            if final_description == "":
                final_description = stac_json_dict["description"]
            else:
                final_description += " & " + stac_json_dict["description"]

            final_assets = {**final_assets, **stac_json_dict["assets"]}

        final_stac_dict["description"] = final_description
        final_stac_dict["assets"] = final_assets

        self._stac_json_dict = final_stac_dict

    def _process_pregenerated_stac(self) -> Path:
        if self._stac_json_dict is None:
            self._load_stac_from_tar()

        if self._tar_indexes is None:
            self._tar_indexes = self._landsat_tar_utils.build_index()

        self._stac_json_dict["id"] = self._tar_path.stem

        self._stac_json_dict["assets"].pop("index")
        self._stac_json_dict["links"] = []

        self._stac_json_dict["collection"] = self._dataset

        for asset_key in self._stac_json_dict["assets"].keys():
            asset = self._stac_json_dict["assets"][asset_key]

            try:
                asset.pop("alternate")
            except Exception as e:
                raise e

            try:
                asset.pop("file:checksum")
            except Exception as e:
                raise e

            for tar_member_file in self._tar_indexes.keys():
                if tar_member_file in asset["href"]:
                    asset["href"] = (
                        f"{env.get_landsat()["stac_asset_download_root"]}{self._dataset}/{self._tar_path.name}"
                        f"?tarMemberFile={tar_member_file}"
                        f"&offset={self._tar_indexes[tar_member_file]['offset']}"
                        f"&size={self._tar_indexes[tar_member_file]['size']}"
                    )
                    break

        self._stac_json_dict["assets"].update(
            {
                "tar": {
                    "href": f"{env.get_landsat()["stac_asset_download_root"]}{self._dataset}/{self._tar_path.name}",
                    "title": "Full tar file",
                    "description": "Full tar file as published by USGS",
                    "type": "application/x-tar",
                    "roles": ["data"]
                }
            }
        )

        stac_filename = Path(f"{Path(self._tar_path.parent) / self._stac_json_dict['id']}_stac.json")
        with open(stac_filename, "w") as f:
            json.dump(self._stac_json_dict, f, indent=4)

        return stac_filename

    def _generate_stac_item(self) -> Path:
        return Path()
        # TODO

        try:
            self._logger.info("Trying to generate STAC item using stactools.")
            stac_item_dict = (
                stac_landsat.create_item(str(self._metadata_xml_file_path))
                .to_dict(include_self_link=False)
            )

        except Exception as stactools_exception:
            self._logger.warning("stactools were unable to create STAC item, using pre-generated STAC item.")
            if self._pregenerated_stac_item_file_path is not None:
                with open(self._pregenerated_stac_item_file_path, 'r') as pregenerated_stac_item_file:
                    stac_item_dict = json.loads(pregenerated_stac_item_file.read())
            else:
                raise DownloadedFileCannotCreateStacItem(
                    f"Unable to create STAC item. stactools.landsat exception: {str(stactools_exception)}, " +
                    f"pregenerated STAC item does not exists!"
                )

        self._stac_item_clear(stac_item_dict)

        stac_item_dict['properties']['displayId'] = self._display_id

        self._feature_dict = stac_item_dict

    def process_landsat_tar(self) -> Tuple[Path, bool]:
        try:
            self._landsat_tar_utils = LandsatTarUtils(self._tar_path)

            path_to_stac = self._process_pregenerated_stac()

            self._logger.info(f"Success {self._tar_path.name}")

            return path_to_stac, True

        except LandsatTarDoesNotContainStacFile as e:
            self._logger.warning(
                f"File {self._tar_path.name} does not contain STAC JSON file. Generating new."
            )

            path_to_stac = self._generate_stac_item()

            return path_to_stac, False
