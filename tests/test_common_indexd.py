"""This modules tests `vadc_gwas_tools.common.indexd` classes."""
import json
import os
import unittest

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
            # Everything looks good
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


class TestIndexdServiceClient(unittest.TestCase):
    def setUp(self):
        super().setUp()
        os.environ[GEN3_ENVIRONMENT_KEY] = "default"
        os.environ[INDEXD_USER] = "TESTUSER"
        os.environ[INDEXD_PASSWORD] = "TESTPASS"

    def tearDown(self):
        os.environ.pop(GEN3_ENVIRONMENT_KEY, None)
        os.environ.pop(INDEXD_USER, None)
        os.environ.pop(INDEXD_PASSWORD, None)
        super().tearDown()

    def test_init_noenv(self):
        if GEN3_ENVIRONMENT_KEY in os.environ:
            del os.environ[GEN3_ENVIRONMENT_KEY]

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

    def test_get_auth_no_user(self):
        # No INDEXD user/password
        os.environ.pop(INDEXD_USER, None)
        os.environ.pop(INDEXD_PASSWORD, None)
        auth = ISC().get_auth()
        self.assertEqual(
            auth,
            ("", ""),
            "Authentication doesn't match when environment INDEXD keys are absent",
        )

        # With INDEXD user/password
        os.environ[INDEXD_USER] = "TESTUSER"
        os.environ[INDEXD_PASSWORD] = "TESTPASS"
        auth = ISC().get_auth()
        self.assertEqual(
            auth,
            ("TESTUSER", "TESTPASS"),
            "Authentication doesn't match when environment INDEXD keys are present",
        )

    @unittest.mock.patch(
        'vadc_gwas_tools.common.indexd.requests.post', side_effect=mocked_requests_post
    )
    def test_create_indexd_record(self, mock_post):
        metadata = {
            "authz": ["/programs/test/projects/test"],
            "file_name": "key",
            "hashes": {"md5": "8b9942cf415384b27cadf1f4d2d682e5"},
            "size": "123",
            "urls": ["ss3://endpointurl/bucket/key"],
            "urls_metadata": {"s3://endpointurl/bucket/key": {}},
            "form": "object",
        }
        metadata = json.dumps(metadata)

        client = ISC()

        # INDEXD user/password are absent
        os.environ.pop(INDEXD_USER, None)
        os.environ.pop(INDEXD_PASSWORD, None)
        self.assertRaises(HTTPError, client.create_indexd_record, metadata=metadata)

        # INDEXD user/password present, no metadata provided
        os.environ[INDEXD_USER] = "TESTUSER"
        os.environ[INDEXD_PASSWORD] = "TESTPASS"
        self.assertRaises(HTTPError, client.create_indexd_record)

        # Normal flow with INDEXD user/password and metadata provided
        res = client.create_indexd_record(metadata=metadata)
        expected = {
            "baseid": "e044a62c-fd60-4203-b1e5-a62d1005f027",
            "did": "e044a62c-fd60-4203-b1e5-a62d1005f028",
            "rev": "rev1",
        }
        self.assertEqual(res, expected, "Indexd record doesn't match expected")
