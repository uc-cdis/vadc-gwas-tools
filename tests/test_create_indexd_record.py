"""Tests for the ``vadc_gwas_tools.subcommands.CreateIndexdRecord`` subcommand"""
import json
import os
import tempfile
from typing import List, NamedTuple
from unittest import TestCase
from unittest.mock import mock_open, patch

from utils import captured_output, cleanup_files

from vadc_gwas_tools.common.indexd import IndexdServiceClient as ISC
from vadc_gwas_tools.subcommands.create_indexd_record import CreateIndexdRecord as CIR


class _mock_args(NamedTuple):
    gwas_archive: str
    arborist_resource: List[str]
    s3_uri: str
    output: str


class TestCreateIndexdRecord(TestCase):
    def setUp(self):
        (tmp_handle, self.tmp_path) = tempfile.mkstemp()
        super().setUp()

    def tearDown(self):
        cleanup_files(self.tmp_path)
        super().tearDown()

    @patch("builtins.open", new_callable=mock_open, read_data=b"test data")
    def test_get_md5_sum(self, mock_file):
        "Test _get_md5_sum helper"
        res = CIR()._get_md5_sum("test/path/to/open")
        expected = {
            "md5": "eb733a00c0c9d336e65691a37ab54293"  # pragma: allowlist secret
        }
        self.assertEqual(res, expected, "MD5 sum record doesn't match expected")

    @patch("vadc_gwas_tools.common.indexd.IndexdServiceClient.create_indexd_record")
    @patch("os.path.getsize")
    @patch(
        "vadc_gwas_tools.subcommands.create_indexd_record.CreateIndexdRecord._get_md5_sum"
    )
    def test_main(self, mock_md5, mock_size, mock_indexd_json):
        "Test main function"
        mock_md5.return_value = {
            "md5": "eb733a00c0c9d336e65691a37ab54293"  # pragma: allowlist secret
        }
        mock_size.return_value = 1024
        mock_indexd_json.return_value = {
            "baseid": "e044a62c-fd60-4203-b1e5-a62d1005f027",
            "did": "e044a62c-fd60-4203-b1e5-a62d1005f028",
            "rev": "rev1",
        }
        args = _mock_args(
            gwas_archive="/path/to/test_gwas.tar.gz",
            arborist_resource="/programs/test/projects/test",
            s3_uri="s3://endpointurl/bucket/key",
            output=self.tmp_path,
        )

        with captured_output() as (_, out):
            CIR().main(args)
            # Check if functions inside main were called
            CIR._get_md5_sum.assert_called_once()
            os.path.getsize.assert_called_once()
            ISC.create_indexd_record.assert_called_once()

        # Check output
        expected_out = []
        expected_out.append(
            "Hash calculated: {'md5': 'eb733a00c0c9d336e65691a37ab54293'}"  # pragma: allowlist secret
        )
        expected_out.append("Size calculated: 1024")
        expected_out.append("'file_name': 'test_gwas.tar.gz'")
        expected_out.append("'authz': '/programs/test/projects/test'")
        expected_out.append(
            "'hashes': {'md5': 'eb733a00c0c9d336e65691a37ab54293'}"  # pragma: allowlist secret
        )
        expected_out.append("'size': 1024")
        expected_out.append("'urls': ['s3://endpointurl/bucket/key'")
        expected_out.append("'urls_metadata': {'s3://endpointurl/bucket/key': {}}")
        expected_out.append("'form': 'object'")
        expected_out.append("Creating Indexd record...")
        expected_out.append(f"JSON response saved in {self.tmp_path}")
        for eo in expected_out:
            self.assertTrue(
                eo in out.getvalue(), f"String not found in the output: {eo}"
            )

        # Check if output file was created properly
        expected_json = {
            "baseid": "e044a62c-fd60-4203-b1e5-a62d1005f027",
            "did": "e044a62c-fd60-4203-b1e5-a62d1005f028",
            "rev": "rev1",
        }
        with open(self.tmp_path, "r") as f:
            res_json = json.load(f)
        self.assertEqual(
            res_json,
            expected_json,
            "Output JSON file's content doesn't match expected.",
        )
