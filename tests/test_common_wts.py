"""This modules tests `vadc_gwas_tools.common.wts.WorkspaceTokenServiceClient` class."""
import os
import unittest
from types import SimpleNamespace
from unittest import mock

import requests

from vadc_gwas_tools.common.const import GEN3_ENVIRONMENT_KEY
from vadc_gwas_tools.common.wts import WorkspaceTokenServiceClient as MOD


class TestWtsClient(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.mocks = SimpleNamespace(requests=mock.MagicMock(spec_set=requests))

    def test_init_noenv(self):
        if GEN3_ENVIRONMENT_KEY in os.environ:
            del os.environ[GEN3_ENVIRONMENT_KEY]

        obj = MOD()
        self.assertEqual(obj.gen3_environment, "default")
        self.assertEqual(obj.service_url, "http://workspace-token-service.default")

    def test_init_wenv(self):
        os.environ[GEN3_ENVIRONMENT_KEY] = "something-else"
        obj = MOD()

        self.assertEqual(obj.gen3_environment, "something-else")
        self.assertEqual(
            obj.service_url, "http://workspace-token-service.something-else"
        )

    def test_refresh_token(self):
        mock_proc = mock.create_autospec(requests.Response)
        mock_proc.raise_for_status.return_value = None
        mock_proc.json.return_value = {"token": "abc"}
        self.mocks.requests.get.return_value = mock_proc

        obj = MOD()
        res = obj.get_refresh_token(_di=self.mocks.requests)
        self.assertEqual(res, {"token": "abc"})

    def test_refresh_token_exception(self):
        mock_proc = mock.create_autospec(requests.Response)
        mock_proc.raise_for_status.side_effect = requests.HTTPError("fake")
        self.mocks.requests.get.return_value = mock_proc

        obj = MOD()
        with self.assertRaises(requests.HTTPError) as e:
            res = obj.get_refresh_token(_di=self.mocks.requests)
