from dataclasses import dataclass, field
from pathlib import Path, PurePath
from typing import (Any, Callable, Dict, List, Optional, Tuple, Type, Union)

from .types import *


@dataclass
class EventFSMap:

    downloader: Downloader
    uploader: Uploader

    source: MAP_SOURCE
    event: Dict[str, Any]
    root: Path

    destn: MAP_DESTN = field(default_factory=dict)

    ignore_missing_source: List[str] = field(default_factory=list)
    ignore_missing_destn: List[str] = field(default_factory=list)
    list_copy_keys: List[str] = field(default_factory=list)

    source_locations: SOURCE_LOCATIONS = field(init=False)
    all_uploads: Dict[str, Union[Dict[str, str],
                                 List[str]]] = field(init=False)
    current_names: Dict[str, Optional[str]] = field(init=False)

    def __post_init__(self) -> None:

        if not self.root:
            self.root = Path(self.name)

        self.source_locations = {'_root': self.root}

        self.all_uploads = {}

        # map template names, e.g. {file} to key
        self.current_names = {'_return': self.all_uploads}

        if doc := self.event.get("document", None):
            self.current_names['doc'] = doc['name']

        for source_name, s3_args in self.iter_fs_maps(map_=self.source, event=self.event, create_path=True,
                                                      ignore_missing_keys=self.ignore_missing_source):
            try:
                self.downloader(**s3_args)
            except DownloadError as exc:
                raise DownloadError(
                    f"Error downloading source key {source_name}: {self.source[source_name]}, from {s3_args}:: {exc.args[0]}")
            self.source_locations[source_name] = Path(s3_args['Filename'])

        self.make_destn_paths()

        self.source_locations['_save'] = self.save_destn(dryrun=True)

    def iter_fs_maps(self, map_: Dict[str, str], event: dict, create_path: bool,
                     ignore_missing_keys=None):

        for _name, _fs in map_.items():
            try:
                mapx: Dict[str, str] = event[_name]
            except KeyError:
                if ignore_missing_keys and _name in ignore_missing_keys:
                    continue
                raise
            key = mapx["key"]

            destn = self.root / \
                self.format_path_from_event(
                    _fs, mapx, ignore_prefix=_name, docinfo=event)
            if create_path:
                destn.parent.mkdir(parents=True, exist_ok=True)
            yield _name, dict(Key=key, Filename=str(destn))

    def make_destn_paths(self):
        for key, file_path in self.destn.items():
            file_path = file_path.format(**self.current_names)
            (self.root / file_path).parent.mkdir(parents=True, exist_ok=True)

    def save_destn(self, dryrun=False):
        destn_paths: Dict[str, Path] = {}
        key, has_multiple = None, []
        for key, file_path in self.destn.items():
            file_path = file_path.format(**self.current_names)
            if path_is_wild(str(file_path)):
                has_multiple.append(key)
                continue
            upload_key = Path(key) / file_path
            destn_paths[key] = self.root / file_path
            if not dryrun:
                try:
                    self.uploader(
                        Filename=str(destn_paths[key]),
                        Key=str(upload_key))
                except FileNotFoundError:
                    if key not in self.ignore_missing_destn:
                        raise
                else:
                    self.all_uploads[key] = dict(key=str(upload_key))

        for key in has_multiple:
            if dryrun:
                path = wild_path_parent(
                    self.destn[key].format(**self.current_names))
                path = self.root / path
                path.mkdir(parents=True, exist_ok=True)
                destn_paths[key] = path
            else:
                self.save_destn_wildcard(
                    key, destn_paths=destn_paths, dryrun=dryrun)

        return destn_paths

    def save_destn_wildcard(self, key: str, destn_paths=None,
                            dryrun=False):

        upload_wild_path = self.destn[key].format(**self.current_names)

        if destn_paths == None:
            destn_paths = {}
        destn_paths[key] = []
        self.all_uploads[key] = []

        for file_path in self.root.glob(upload_wild_path):

            if file_path.is_absolute():
                file_path = file_path.relative_to(self.root)

            destn_paths[key].append(file_path)
            upload_key = Path(key) / file_path.relative_to(self.root)
            if not dryrun:
                self.uploader(Filename=str(file_path), Key=str(upload_key))
                self.all_uploads[key].append(str(upload_key))

        return destn_paths

    def __exit__(self,
                 exc_type: Optional[Type[BaseException]],
                 exc_val: Optional[BaseException],
                 exc_tb) -> None:

        if not exc_type:
            # no exception has been hit
            self.save_destn()

    def __enter__(self) -> Tuple[Dict[str, Path], Dict[str, str]]:
        return self.source_locations, self.current_names

    def format_path_from_event(self, template: str, event_s3, ignore_prefix="", docinfo={}):
        # On python >= 3.9
        # key = PurePath(event_s3["key"])
        # if ignore_prefix and key.is_relative_to(ignore_prefix):
        #     key = key.relative_to(ignore_prefix)

        key = event_s3["key"]
        if ignore_prefix and key.startswith(f"{ignore_prefix}/"):
            key = key.split("/", maxsplit=1)[1]
        key = PurePath(key)

        format_names = dict(
            file=key.name
        )
        if docinfo:

            if doc := docinfo.get("document", None):
                format_names['doc'] = doc["name"]

            if is_page := docinfo.get("SourcePage", None):
                format_names['page'] = f"{is_page['index']}.jpg"
                format_names['pagenum'] = f"{is_page['index']}"

            if is_header := docinfo.get("Header", None):
                format_names['header_index'] = f"{is_header['index']}"

        self.current_names.update(format_names)
        return template.format(**format_names)


def return_body(event, s3map, list_keys: List[str] = None,
                extra_return: Dict[str, Any] = None,
                additional_info: INFO_FROM_PATH = None, key_copy: List[str] = None):

    out = dict(event)
    if retn := s3map.get('_return'):
        out.update(retn)
        for extra_key, value in extra_return.items():
            if cur := out.get(extra_key):
                if extra_key not in list_keys:
                    assert isinstance(cur, dict)
                    assert isinstance(value, dict)
                    cur.update(value)

    for listed in list_keys:
        data = return_body_list(event, s3map=s3map, multiples_key=listed,
                                root_list_key=listed, get_additional_info=additional_info,
                                top_level_list=False)
        for key in key_copy:
            for item in data[listed]:
                item.update({key: out[key]})
                if val := extra_return.get(listed):
                    assert isinstance(val, dict)
                    item[listed].update(val)
        out[listed] = data[listed]

    return out


def return_body_list(event, s3map, multiples_key: str, root_list_key: str,
                     get_additional_info: INFO_FROM_PATH = None,
                     top_level_list=True):

    out = dict(event)
    listed = []
    out[root_list_key] = listed

    for sub_path in s3map['_return'][multiples_key]:
        x = {'key': str(sub_path)}

        if get_additional_info:
            x.update(get_additional_info(Path(sub_path)))

        listed.append({multiples_key: x})

    for constant_key, path in s3map['_return'].items():
        if constant_key != multiples_key:
            if top_level_list:
                for item in listed:
                    item[constant_key] = {'key': str(path)}
            else:
                out[constant_key] = path
    return out


def path_is_wild(path: Path) -> bool:
    return "*" in str(path) or "?" in str(path)


def wild_path_parent(path: Union[Path, str]) -> Path:
    for parent in Path(path).parents:
        if not path_is_wild(str(parent)):
            return parent
    assert False
