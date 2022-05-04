"""Tests for the ``vadc_gwas_tools.subcommands.SplitFilnameByChr`` subcommand"""
import json
import tempfile
import unittest
from typing import NamedTuple, Optional

from utils import captured_output, cleanup_files

from vadc_gwas_tools.subcommands import SplitFilenameByChr as MOD


class _mock_args(NamedTuple):
    gds_file: str
    output: Optional[str]


class TestSplitFilnameByChrSubcommand(unittest.TestCase):
    def test_main_ok(self):
        expected = json.dumps(
            {"file_prefix": "my.project.chr", "file_suffix": ".vcf.gds"}, sort_keys=True
        )

        with captured_output() as (so, _):
            args = _mock_args(gds_file="/path/to/my.project.chrX.vcf.gds", output=None)
            MOD.main(args)

        sout = so.getvalue()
        self.assertEqual(expected, sout)

        # Case with file output
        try:
            (fd1, path1) = tempfile.mkstemp()
            with captured_output() as (_, _):
                args = _mock_args(
                    gds_file="/path/to/my.project.chrX.vcf.gds", output=path1
                )
                MOD.main(args)
            with open(path1, "rt") as fh:
                obs = fh.read().rstrip("\r\n")
            self.assertEqual(expected, obs)
        finally:
            cleanup_files(path1)

    def test_main_no_chr(self):
        with captured_output() as (_, _):
            args = _mock_args(gds_file="/path/to/my.project.X.vcf.gds", output=None)
            with self.assertRaises(AssertionError) as e:
                MOD.main(args)

    def test_main_no_dot(self):
        with captured_output() as (_, _):
            args = _mock_args(gds_file="/path/to/my_project_chrX_vcf.gds", output=None)
            with self.assertRaises(AssertionError) as e:
                MOD.main(args)
