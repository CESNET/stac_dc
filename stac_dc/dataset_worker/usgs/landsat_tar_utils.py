import tarfile
from tarfile import TarInfo

from .exceptions.landsat_tar_utils import *


class LandsatTarUtils:
    _tar_file_path: Path

    def __init__(self, tar_file_object: Path):
        if tar_file_object is None:
            raise TarObjectNotSpecifiedException()
        if not tar_file_object.exists():
            raise FileNotFoundError(f"Tar file not found: {tar_file_object}")
        self._tar_file_path = tar_file_object

    def get_members(self) -> list[TarInfo]:
        with tarfile.open(self._tar_file_path, mode="r") as tar_file:
            return tar_file.getmembers()

    def untar_member(self, member: TarInfo, untar_dir: Path = None) -> Path:
        if untar_dir is None:
            untar_dir = self._tar_file_path.parent

        untar_dir.mkdir(parents=True, exist_ok=True)
        untar_path = untar_dir / member.name

        with tarfile.open(self._tar_file_path, mode="r") as tar_file:
            tar_file.extract(member, path=untar_dir)

        return untar_path

    def build_index(self) -> dict[str, dict[str, int]]:
        index: dict[str, dict[str, int]] = {}

        with tarfile.open(self._tar_file_path, mode="r") as tar_file:
            print(f"Building index for {self._tar_file_path.name}")
            for member in tar_file:
                if member.isfile():
                    index[member.name] = {
                        "offset": member.offset_data,
                        "size": member.size,
                    }

        return index
