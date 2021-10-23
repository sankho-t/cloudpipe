import atexit
import functools
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional, Union

from .main import *


@dataclass
class Step:
    location_env_key: Dict[str, str] = field(default_factory=dict)
    local: Optional[Union[Path, str]] = None

    downloader: Downloader = field(init=False)
    uploader: Uploader = field(init=False)
    in_cloud: bool = field(default=False)

    more_info: INFO_FROM_PATH = None

    def __post_init__(self):
        if self.local is None:
            self.local = Path(tempfile.mkdtemp())
            atexit.register(lambda:
                            shutil.rmtree(self.local, ignore_errors=True))

    def __call__(self, source: MAP_SOURCE, destn: MAP_DESTN = None,
                 list_copy_keys: List[str] = None,
                 more_info: INFO_FROM_PATH = None,
                 **kwargs):

        multi_saves: List[str] = []
        if destn is None:
            destn = {}

        def modifier(func):

            if self.in_cloud:
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
                    func(*args, **kwargs)
                return dummy
        return modifier
