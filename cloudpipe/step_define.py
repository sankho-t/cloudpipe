import atexit
import functools
import shutil
import tempfile
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional, Union

from .init_cloud import default_assume, in_cloud, new_storage
from .main import *
from .types import *


@dataclass
class Step:
    location_env_key: CLOUD_STORE = field(default_factory=dict)
    """Environment variable containing the storage name (e.g. s3 bucket name).
    If not found it will directly be used as the storage name."""

    local: Optional[Union[Path, str]] = None
    """Path for storing data locally. If not provided, will create a temporary space."""

    in_cloud: Optional[bool] = field(default=None)
    """False => do not use cloud storage (override with `downloader` or `uploader`)
    True => use only if working in a cloud environment
    None => if unable to identify cloud env, then assume cloud environment as in `default_assume`
    """

    downloader: Downloader = field(init=False)
    uploader: Uploader = field(init=False)

    storage: CloudStoreBase = field(init=False)

    more_info: INFO_FROM_PATH = None
    """Callable to gather additional info for each file being uploaded"""

    def __post_init__(self):
        if self.local is None:
            self.local = Path(tempfile.mkdtemp())
            atexit.register(lambda:
                            shutil.rmtree(self.local, ignore_errors=True))

        if self.in_cloud is not False:
            self.storage = new_storage(self.location_env_key)
            if (not self.storage) and (self.in_cloud is None):
                self.storage = new_storage(self.location_env_key,
                                           **{default_assume: True})
            if self.storage:
                self.downloader = self.storage.downloader
                self.uploader = self.storage.uploader
        else:
            if self.downloader is None:
                self.downloader = DummyDownloader
            if self.uploader is None:
                self.uploader = DummyUploader

    def __call__(self, source: MAP_SOURCE, destn: MAP_DESTN = None,
                 list_copy_keys: List[str] = None,
                 more_info: INFO_FROM_PATH = None,
                 **kwargs):

        multi_saves: List[str] = []
        if destn is None:
            destn = {}

        def modifier(func):

            if self.in_cloud is not False:
                @functools.wraps(func)
                def handler(event, context):
                    with EventFSMap(
                        event=event,
                        source=source, destn=destn, root=self.local,
                        downloader=self.downloader, uploader=self.uploader, **kwargs) \
                            as (fsmap, remotemap):

                        args = dict(context=context)

                        for key in source:
                            args[key] = fsmap[key]
                            if evparam := event.get(key, {}):
                                for name, val in evparam.items():
                                    if name != 'Key':
                                        args[f"{key}_{name}"] = val

                        extra_retn = {}

                        for key, path in destn.items():
                            key2 = key
                            if key in args:
                                key2 += "_save"
                            args[key2] = fsmap['_save'][key]

                            args[f"{key2}_args"] = extra_retn[key2] = {}

                            if path_is_wild(path):
                                multi_saves.append(key)

                        func(**args)

                    return {
                        'statusCode': '200',
                        'body': return_body(event=event, s3map=remotemap,
                                            list_keys=multi_saves,
                                            additional_info=more_info, key_copy=list_copy_keys)
                    }
                return handler
            else:
                @functools.wraps(func)
                def dummy(*args, **kwargs):
                    if 'event' in kwargs:
                        warnings.warn(f"""`event` argument is passed to {func}, 
                        although `in_cloud` == {in_cloud}, i.e function may be running in cloud mode
                        but detected environment is local. """)
                    func(*args, **kwargs)
                return dummy
        return modifier
