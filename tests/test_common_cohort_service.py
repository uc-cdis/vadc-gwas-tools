"""This modules tests `vadc_gwas_tools.common.cohort_middleware.CohortServiceClient` class."""
import gzip
import json
import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

import requests
from utils import cleanup_files

from vadc_gwas_tools.common.cohort_middleware import CohortServiceClient as MOD
from vadc_gwas_tools.common.const import GEN3_ENVIRONMENT_KEY


class TestCohortServiceClient(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.mocks = SimpleNamespace(requests=mock.MagicMock(spec_set=requests))

    def test_init_noenv(self):
        if GEN3_ENVIRONMENT_KEY in os.environ:
            del os.environ[GEN3_ENVIRONMENT_KEY]

        obj = MOD()
        self.assertEqual(obj.gen3_environment, "default")
        self.assertEqual(obj.service_url, "http://cohort-middleware-service.default")

    def test_init_wenv(self):
        os.environ[GEN3_ENVIRONMENT_KEY] = "something-else"
        obj = MOD()

        self.assertEqual(obj.gen3_environment, "something-else")
        self.assertEqual(
            obj.service_url, "http://cohort-middleware-service.something-else"
        )

    def test_get_header(self):
        expected = {"Content-Type": "application/json", "Authorization": "Bearer abc"}
        obj = MOD()
        obj.wts.get_refresh_token = mock.MagicMock(return_value={"token": "abc"})
        res = obj.get_header()
        self.assertEqual(res, expected)

    def _return_generator(self, items):
        for item in items:
            yield item

    def test_get_cohort_csv(self):
        if GEN3_ENVIRONMENT_KEY in os.environ:
            del os.environ[GEN3_ENVIRONMENT_KEY]

        fake_items = [b"sample.id,ID_001,ID_002\n", b"1001,0.01,1.5\n"]
        mock_proc = mock.create_autospec(requests.Response)
        mock_proc.raise_for_status.return_value = None
        mock_proc.iter_content.return_value = self._return_generator(fake_items)
        self.mocks.requests.post.return_value = mock_proc

        obj = MOD()
        obj.get_header = mock.MagicMock(
            return_value={
                "Content-Type": "application/json",
                "Authorization": "Bearer abc",
            }
        )

        (fd1, fpath1) = tempfile.mkstemp()
        try:
            obj.get_cohort_csv(
                1, 2, fpath1, ["ID_001", "ID_002"], _di=self.mocks.requests
            )
            self.mocks.requests.post.assert_called_with(
                "http://cohort-middleware-service.default/cohort-data/by-source-id/1/by-cohort-definition-id/2",
                data=json.dumps({"PrefixedConceptIds": ["ID_001", "ID_002"]}),
                headers={
                    "Content-Type": "application/json",
                    "Authorization": "Bearer abc",
                },
                stream=True,
            )

            with self.assertRaises(OSError) as _:
                with gzip.open(fpath1, "rt") as fh:
                    for line in fh:
                        pass

            with open(fpath1, "rt") as fh:
                header = fh.readline().rstrip("\r\n").split(",")
                self.assertEqual(header, ["sample.id", "ID_001", "ID_002"])

                dat1 = fh.readline().rstrip("\r\n").split(",")
                self.assertEqual(dat1, ["1001", "0.01", "1.5"])

        finally:
            cleanup_files(fpath1)

    def test_get_cohort_csv_gzip(self):
        if GEN3_ENVIRONMENT_KEY in os.environ:
            del os.environ[GEN3_ENVIRONMENT_KEY]

        fake_items = [b"sample.id,ID_001,ID_002\n", b"1001,0.01,1.5\n"]
        mock_proc = mock.create_autospec(requests.Response)
        mock_proc.raise_for_status.return_value = None
        mock_proc.iter_content.return_value = self._return_generator(fake_items)
        self.mocks.requests.post.return_value = mock_proc

        obj = MOD()
        obj.get_header = mock.MagicMock(
            return_value={
                "Content-Type": "application/json",
                "Authorization": "Bearer abc",
            }
        )

        (fd1, fpath1) = tempfile.mkstemp(suffix=".csv.gz")
        try:
            obj.get_cohort_csv(
                1, 2, fpath1, ["ID_001", "ID_002"], _di=self.mocks.requests
            )
            with gzip.open(fpath1, "rt") as fh:
                header = fh.readline().rstrip("\r\n").split(",")
                self.assertEqual(header, ["sample.id", "ID_001", "ID_002"])

                dat1 = fh.readline().rstrip("\r\n").split(",")
                self.assertEqual(dat1, ["1001", "0.01", "1.5"])

        finally:
            cleanup_files(fpath1)
