import json
import logging

import xmltodict

from enum import Enum
from typing import Tuple

from .landsat_tar_utils import LandsatTarUtils

from .exceptions.landsat_processor import *

from env import env


class MTL_TYPE(Enum):
    JSON = "_MTL.json"
    TXT = "_MTL.txt"
    XML = "_MTL.xml"


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

        try:
            final_stac_dict["properties"].pop("card4l:specification")
        except KeyError:
            pass

        try:
            final_stac_dict["properties"].pop("card4l:specification_version")
        except KeyError:
            pass

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

    def _untar_mtl_from_product(self, type: MTL_TYPE) -> Path:
        tar_members = self._landsat_tar_utils.get_members()

        mtl_files = [m for m in tar_members if m.name.endswith(type.value)]

        if len(mtl_files) != 1:
            raise LandsatTarFileUnexpectedContents(
                path=self._tar_path,
                additional_info=f"Found {len(mtl_files)} MTL files!"
            )

        mtl_member_file = mtl_files[0]

        metadata_file_path: Path = self._landsat_tar_utils.untar_member(
            member=mtl_member_file,
        )

        return metadata_file_path

    def _populate_stac_item(self, metadata_dict: dict):
        stac_template_path = Path(__file__).resolve().parent / "stac_templates" / "[feature]landsat.json"
        with stac_template_path.open("r", encoding="utf-8") as stac_template_file:
            stac_json_dict = json.load(stac_template_file)

        stac_json_dict["features"][0]["properties"]["temporary"] = True

        stac_json_dict["features"][0]["id"] = (
            metadata_dict["LANDSAT_METADATA_FILE"]["PRODUCT_CONTENTS"]["LANDSAT_PRODUCT_ID"]
        )

        stac_json_dict["features"][0]["collection"] = self._dataset

        datetime = (
                metadata_dict["LANDSAT_METADATA_FILE"]["IMAGE_ATTRIBUTES"]["DATE_ACQUIRED"] +
                "T" +
                metadata_dict["LANDSAT_METADATA_FILE"]["IMAGE_ATTRIBUTES"]["SCENE_CENTER_TIME"]
        )
        stac_json_dict["features"][0]["properties"]["start_datetime"] = datetime
        stac_json_dict["features"][0]["properties"]["end_datetime"] = datetime
        stac_json_dict["features"][0]["properties"]["datetime"] = datetime

        corners_lats = [
            float(metadata_dict["LANDSAT_METADATA_FILE"]["PROJECTION_ATTRIBUTES"]["CORNER_UL_LAT_PRODUCT"]),
            float(metadata_dict["LANDSAT_METADATA_FILE"]["PROJECTION_ATTRIBUTES"]["CORNER_UR_LAT_PRODUCT"]),
            float(metadata_dict["LANDSAT_METADATA_FILE"]["PROJECTION_ATTRIBUTES"]["CORNER_LL_LAT_PRODUCT"]),
            float(metadata_dict["LANDSAT_METADATA_FILE"]["PROJECTION_ATTRIBUTES"]["CORNER_LR_LAT_PRODUCT"]),
        ]

        corners_lons = [
            float(metadata_dict["LANDSAT_METADATA_FILE"]["PROJECTION_ATTRIBUTES"]["CORNER_UL_LON_PRODUCT"]),
            float(metadata_dict["LANDSAT_METADATA_FILE"]["PROJECTION_ATTRIBUTES"]["CORNER_UR_LON_PRODUCT"]),
            float(metadata_dict["LANDSAT_METADATA_FILE"]["PROJECTION_ATTRIBUTES"]["CORNER_LL_LON_PRODUCT"]),
            float(metadata_dict["LANDSAT_METADATA_FILE"]["PROJECTION_ATTRIBUTES"]["CORNER_LR_LON_PRODUCT"]),
        ]

        bbox = [
            min(corners_lons),  # WEST
            min(corners_lats),  # SOUTH
            max(corners_lons),  # EAST
            max(corners_lats),  # NORTH
        ]
        stac_json_dict["features"][0]["bbox"] = bbox

        polygon = [[
            [bbox[0], bbox[1]],  # LOWER LEFT
            [bbox[2], bbox[1]],  # LOWER RIGHT
            [bbox[2], bbox[3]],  # UPPER RIGHT
            [bbox[0], bbox[3]],  # UPPER LEFT
            [bbox[0], bbox[1]],  # LOWER LEFT - back to beginning
        ]]
        stac_json_dict["features"][0]["geometry"]["coordinates"] = polygon

        stac_json_dict["features"][0]["assets"].update(
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

        return stac_json_dict

    def _generate_stac_item(self) -> Path:
        metadata_file_path: Path = self._untar_mtl_from_product(type=MTL_TYPE.XML)
        try:
            with metadata_file_path.open("r", encoding="utf-8") as metadata_file:
                metadata_dict = xmltodict.parse(metadata_file.read())
        finally:
            if metadata_file_path.exists():
                metadata_file_path.unlink(missing_ok=True)

        stac_dict = self._populate_stac_item(metadata_dict=metadata_dict)
        self._stac_json_dict = stac_dict["features"][0]

        stac_filename = Path(f"{Path(self._tar_path.parent) / self._stac_json_dict['id']}_stac.json")
        with open(stac_filename, "w") as f:
            json.dump(self._stac_json_dict, f, indent=4)

        return stac_filename

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
