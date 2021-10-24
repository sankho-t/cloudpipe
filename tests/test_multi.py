from typing import Dict
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from cloudpipe import *


class TestMultiple(unittest.TestCase):
    def setUp(self) -> None:

        self.cmap = Step(location_env_key={'s3': 'dummy_bucket'})
        self.cmap.downloader = MagicMock()
        self.cmap.uploader = MagicMock()

        return super().setUp()

    def test(self):

        self.cmap.local = Path("dummy_path")

        @self.cmap(
            source={"original": "{doc}/input.jpeg"},
            destn={"objects": "{doc}/output/*.jpeg"},
            list_copy_keys=["original"])
        def simple_worker(original: Path, objects: Path, objects_args: Dict[str, str],
                          *args, **kwargs):

            self.assertEqual(objects, Path("dummy_path") /
                             "dummy_doc" / "output")
            (objects / "1.jpeg").touch()
            (objects / "0.jpeg").touch()
            objects_args["total"] = "2"

        retn = simple_worker(
            event={"document": {"name": "dummy_doc"},
                   "original": {"key": "image.jpeg"}},
            context=None)

        self.assertTrue(isinstance(retn["body"]["objects"], list))

        up_keys = [x["objects"]["key"] for x in retn["body"]["objects"]]
        self.assertIn("objects/dummy_doc/output/0.jpeg", up_keys)
        self.assertIn("objects/dummy_doc/output/1.jpeg", up_keys)

        extras = set([x["objects"]["total"] for x in retn["body"]["objects"]])
        self.assertEqual(extras, {"2"})
