"""Tests for the ``vadc_gwas_tools.subcommands.FilterSegments`` subcommand"""
import json
import tempfile
import unittest
from typing import List, NamedTuple, Optional

from utils import captured_output, cleanup_files

from vadc_gwas_tools.subcommands import FilterSegments as MOD


class _mock_args(NamedTuple):
    gds_filenames: List[str]
    file_prefix: str
    file_suffix: str
    segment_file: str
    output: Optional[str]


class TestFilterSegmentsSubcommand(unittest.TestCase):
    def test_main_ok(self):
        file_prefix = "my.project.chr"
        file_suffix = ".vcf.gds"
        gds_filenames = [
            "/path/to/my.project.chr1.vcf.gds",
            "/path/to/my.project.chr3.vcf.gds",
        ]

        expected = json.dumps(
            {"chromosomes": ["1", "3"], "segments": [1, 2, 5, 6]}, sort_keys=True
        )

        segments = [
            ["chromosome", "start", "end"],
            ["1", "1", "100"],
            ["1", "101", "200"],
            ["2", "1", "100"],
            ["2", "101", "200"],
            ["3", "1", "100"],
            ["3", "101", "200"],
        ]

        try:
            (fd1, segpath) = tempfile.mkstemp()
            with open(segpath, "wt") as o:
                for item in segments:
                    o.write("\t".join(item) + "\n")

            with captured_output() as (so, _):
                args = _mock_args(
                    gds_filenames=gds_filenames,
                    file_prefix=file_prefix,
                    file_suffix=file_suffix,
                    segment_file=segpath,
                    output=None,
                )
                MOD.main(args)

            sout = so.getvalue()
            self.assertEqual(expected, sout)

            (fd2, path2) = tempfile.mkstemp()
            with captured_output() as (_, _):
                args = _mock_args(
                    gds_filenames=gds_filenames,
                    file_prefix=file_prefix,
                    file_suffix=file_suffix,
                    segment_file=segpath,
                    output=path2,
                )
                MOD.main(args)
            with open(path2, "rt") as fh:
                obs = fh.read().rstrip("\r\n")
            self.assertEqual(expected, obs)

        finally:
            cleanup_files([segpath, path2])
