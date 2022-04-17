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
    """ 
    Create a decorator to seamlessly run a function in the cloud as event handler.

    Parameters:
        source: Dict[str, str]
            Map of process key and relative path for local storage
        destn: Dict[str, str]
            Map of process key and relative path for cloud storage. May include wildcards: *, ?
        list_copy_keys: List[str]
            Process keys which should be copied into list file outputs (for wildcard paths).
        more_info: INFO_FROM_PATH
            Callable for getting additional return info, given path

    Parameters of functions to be decorated:

        argument 1..N : pathlib.Path
            Includes both `source` and `destn` arguments (as in decorator definition)
        argument_var : Union[dict, str]
            Additional info var
        argument_args : Dict[str, Any] (output)
            Map of process key against any additional information to be returned

    """

    location_env_key: CLOUD_STORE = field(default_factory=dict)
    """Environment variable containing the storage name (e.g. s3 bucket name).
    If not found it will directly be used as the storage name."""

    arg_override_location_env_key: Dict[str, CLOUD_STORE] = \
        field(default_factory=dict)
    """Override `location_env_key` by function argument name.
    Will be used only for downloading."""

    local: Optional[Union[Path, str]] = None
    """Path for storing data locally. If not provided, will create a temporary space."""

    in_cloud: Optional[bool] = field(default=None)
    """False => do not use cloud storage (override with `downloader` or `uploader`)
    True => use only if working in a cloud environment
    None => if unable to identify cloud env, then assume cloud environment as in `default_assume`
    """

    downloader: Downloader = field(default=None)
    uploader: Uploader = field(default=None)

    arg_override_downloader: Dict[str, Downloader] = \
        field(default_factory=dict)
    storage: CloudStoreBase = field(init=False)

    save_prefix: str = field(default="")
    "Fixed prefix for uploaded files"

    more_info: INFO_FROM_PATH = None
    """Callable to gather additional info for each file being uploaded"""

    def __post_init__(self):
        if self.local is None:
            self.local = Path(tempfile.mkdtemp())
            atexit.register(lambda:
                            shutil.rmtree(self.local, ignore_errors=True))

        if self.in_cloud is not False:
            new_storage_call = {}
            self.storage = new_storage(
                self.location_env_key, **new_storage_call)
            if (not self.storage) and (self.in_cloud is None):
                new_storage_call = {default_assume: True}
                self.storage = new_storage(self.location_env_key,
                                           **new_storage_call)
            if self.storage:
                for arg, store in self.arg_override_location_env_key.items():
                    if store := new_storage(store, **new_storage_call):
                        self.arg_override_downloader[arg] = store.downloader
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
                 pass_event_as: str = None,
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
                        downloader=self.downloader, uploader=self.uploader,
                        arg_override_downloader=self.arg_override_downloader,
                        require_save_prefix=self.save_prefix,
                        **kwargs) \
                            as (fsmap, remotemap):

                        args = dict(context=context)

                        if pass_event_as:
                            args[pass_event_as] = event

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
                                            extra_return=extra_retn,
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
