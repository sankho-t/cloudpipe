from pathlib import Path, PurePath
from typing import List, Dict, Optional, Protocol, Type, Tuple, TypedDict, Any, Callable

MAP_SOURCE = Dict[str, str]
MAP_DESTN = Dict[str, str]

SOURCE_LOCATIONS = TypedDict(
    "SOURCE_LOCATIONS", {"_root": Path, "_save": Dict[str, Path]}, total=False)

CLOUD_STORE = TypedDict("CLOUD_LOCATIONS", {'s3': str})


class Downloader(Protocol):
    def __call__(self, Key: str, Filename: str) -> None: ...


class Uploader(Protocol):
    def __call__(self, Key: str, Filename: str) -> None: ...


INFO_FROM_PATH = Callable[[Path], Dict[str, Any]]


class DownloadError(RuntimeError):
    pass


class CloudStoreBase(Protocol):
    def downloader(self, Key: str, Filename: str) -> None:
        ...

    def uploader(self, Key: str, Filename: str) -> None:
        ...


def DummyDownloader(*args, **kwargs):
    pass


def DummyUploader(*args, **kwargs):
    pass
