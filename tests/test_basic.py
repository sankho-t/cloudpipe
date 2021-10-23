import unittest
from unittest.mock import MagicMock
from pathlib import Path

from cloudpipe import *


class TestDownload(unittest.TestCase):
    def setUp(self) -> None:

        self.cmap = Step()
        self.cmap.downloader = MagicMock()
        self.cmap.uploader = MagicMock()
        self.cmap.in_cloud = True

        return super().setUp()

    def test(self):
        self.cmap.local = Path("dummy_path")

        @self.cmap(
            source={"upload": "{doc}"})
        def simple_worker(upload: Path, *args, **kwargs):
            self.assertEqual(upload, self.cmap.local / "dummy_doc")

        simple_worker(
            event={"document": {"name": "dummy_doc"}, "upload": {"key": "a"}},
            context=None)

        self.cmap.downloader.assert_called_with(
            Key="a", Filename="dummy_path/dummy_doc")


class TestUpload(TestDownload):

    def test(self):

        @self.cmap(
            source={"source": "{doc}"},
            destn={"result": "{doc}"})
        def simple_worker(source: Path, result: Path, *args, **kwargs):
            self.assertFalse(result.exists())
            result.touch()

        retn = simple_worker(
            event={"document": {"name": "dummy_doc"}, "source": {"key": "a"}},
            context=None)

        self.assertIn('Key', self.cmap.uploader.call_args.kwargs)
        self.assertEqual(
            self.cmap.uploader.call_args.kwargs['Key'], 'result/dummy_doc')

        self.assertIn("body", retn)
        self.assertEqual(retn["body"]["result"]["key"], "result/dummy_doc")
