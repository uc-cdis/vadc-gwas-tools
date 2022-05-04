"""Tests for the ``vadc_gwas_tools.subcommands.SplitFilnameByChr`` subcommand"""
import json
import unittest
from typing import NamedTuple, Optional

from utils import captured_output

from vadc_gwas_tools.subcommands import SplitFilenameByChr as MOD


class _mock_args(NamedTuple):
    gds_file: str
    output: Optional[str]


class TestSplitFilnameByChrSubcommand(unittest.TestCase):
    def test_main_ok(self):
        args = _mock_args(gds_file="/path/to/my.project.chrX.vcf.gds", output=None)

        expected = json.dumps(
            {"file_prefix": "my.project.chr", "file_suffix": ".vcf.gds"}, sort_keys=True
        )

        with captured_output() as (so, _):
            MOD.main(args)

        sout = so.getvalue()
        self.assertEqual(expected, sout)
