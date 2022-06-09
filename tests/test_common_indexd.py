"""This modules tests `vadc_gwas_tools.common.indexd` classes."""
import json
import os
from unittest import TestCase
from unittest.mock import patch

from requests import HTTPError

from vadc_gwas_tools.common.const import (
    GEN3_ENVIRONMENT_KEY,
    INDEXD_PASSWORD,
    INDEXD_USER,
)
from vadc_gwas_tools.common.indexd import IndexdServiceClient as ISC


def mocked_requests_post(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

        def raise_for_status(self):
            if self.status_code != 200:
                raise HTTPError(f"{self.status_code} error")

    if kwargs.get("auth") == ("TESTUSER", "TESTPASS"):
        try:
            json.loads(kwargs.get("json"))
            # Everything looks good, return data and status 200 response
            return MockResponse(
                {
                    "baseid": "e044a62c-fd60-4203-b1e5-a62d1005f027",
                    "did": "e044a62c-fd60-4203-b1e5-a62d1005f028",
                    "rev": "rev1",
                },
                200,
            )
        except ValueError as e:
            # No metadata provided, return 400 Bad Request error
            return MockResponse(None, 400)
    else:
        # No INDEXD user/password provided, return 403 Forbidden error
        return MockResponse(None, 403)

    return MockResponse(None, 404)


class TestIndexdServiceClient(TestCase):
    def setUp(self):
        super().setUp()
        os.environ[GEN3_ENVIRONMENT_KEY] = "default"
        os.environ[INDEXD_USER] = "TESTUSER"
        os.environ[INDEXD_PASSWORD] = "TESTPASS"  # pragma: allowlist secret
        metadata = {
            "authz": ["/programs/test/projects/test"],
            "file_name": "key",
            "hashes": {
                "md5": "8b9942cf415384b27cadf1f4d2d682e5"  # pragma: allowlist secret
            },
            "size": "123",
            "urls": ["s3://endpointurl/bucket/key"],
            "urls_metadata": {"s3://endpointurl/bucket/key": {}},
            "form": "object",
        }
        self.metadata = json.dumps(metadata)

    def tearDown(self):
        os.environ.pop(GEN3_ENVIRONMENT_KEY, None)
        os.environ.pop(INDEXD_USER, None)
        os.environ.pop(INDEXD_PASSWORD, None)
        super().tearDown()

    def test_init_noenv(self):
        "Test __init__ when no environment key provided"
        os.environ.pop(GEN3_ENVIRONMENT_KEY, None)
        client = ISC()
        self.assertEqual(
            client.gen3_environment,
            "default",
            "Init assigned wrong gen3_environment when gen3_environment_key "
            "is absent",
        )
        self.assertEqual(
            client.service_url,
            "http://indexd-service.default",
            "Init assigned wrong service_url when gen3_environment_key is absent",
        )

    def test_init_wenv(self):
        "Test __init__ when with environment key provided"
        os.environ[GEN3_ENVIRONMENT_KEY] = "something-else"
        client = ISC()
        self.assertEqual(
            client.gen3_environment,
            "something-else",
            "Init assigned wrong gen3_environment when gen3_environment_key present",
        )
        self.assertEqual(
            client.service_url,
            "http://indexd-service.something-else",
            "Init assigned wrong service_url when gen3_environment_key is present",
        )
        del os.environ[GEN3_ENVIRONMENT_KEY]

    def test_get_auth_wuser(self):
        "Test get_auth() when INDEXD user/password provided"
        client = ISC()
        auth = client.get_auth()
        self.assertEqual(
            auth,
            ("TESTUSER", "TESTPASS"),
            "Authentication doesn't match when environment INDEXD keys are present",
        )

    def test_get_auth_nouser(self):
        "Test get_auth() when INDEXD user/password not provided"
        os.environ.pop(INDEXD_USER, None)
        os.environ.pop(INDEXD_PASSWORD, None)
        client = ISC()
        auth = client.get_auth()
        self.assertEqual(
            auth,
            ("", ""),
            "Authentication doesn't match when environment INDEXD keys are absent",
        )

    @patch("requests.post", side_effect=mocked_requests_post)
    def test_create_indexd_record_nouser(self, mock_post):
        "Test create_indexd_record when no INDEXD user/password provided"
        os.environ.pop(INDEXD_USER, None)
        os.environ.pop(INDEXD_PASSWORD, None)
        client = ISC()
        self.assertRaises(
            HTTPError, client.create_indexd_record, metadata=self.metadata
        )

    @patch("requests.post", side_effect=mocked_requests_post)
    def test_create_indexd_record_nometadata(self, mock_post):
        "Test create_indexd_record when no metadata provided"
        client = ISC()
        self.assertRaises(HTTPError, client.create_indexd_record)

    @patch("requests.post", side_effect=mocked_requests_post)
    def test_create_indexd_record(self, mock_post):
        "Test create_indexd_record with metadata and INDEXD user/password"
        client = ISC()
        res = client.create_indexd_record(metadata=self.metadata)
        expected = {
            "baseid": "e044a62c-fd60-4203-b1e5-a62d1005f027",
            "did": "e044a62c-fd60-4203-b1e5-a62d1005f028",
            "rev": "rev1",
        }
        self.assertEqual(res, expected, "Indexd record doesn't match expected")
