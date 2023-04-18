"""Tests for the ``vadc_gwas_tools.subcommands.GetPheWebPlotJson`` subcommand"""
import csv
import json
import tempfile
import unittest
from typing import NamedTuple

from utils import captured_output, cleanup_files

from vadc_gwas_tools.subcommands import GetPheWebPlotJson as MOD


class _mock_args(NamedTuple):
    in_tsv: int
    out_json: int
    out_plot_type: str


class TestGetPheWebPlotJsonSubcommand(unittest.TestCase):
    def test_main_ok(self):

        (_, tmp_in_tsv_path) = tempfile.mkstemp(suffix=".tsv")

        with open(tmp_in_tsv_path, "wt") as o:
            writer = csv.writer(o, delimiter="\t")
            writer.writerow(
                [
                    "chrom",
                    "pos",
                    "ref",
                    "alt",
                    "rsids",
                    "nearest_genes",
                    "pval",
                    "beta",
                    "sebeta",
                    "maf",
                    "ac",
                    "r2",
                ]
            )
            writer.writerow(
                [
                    "1",
                    "662622",
                    "G",
                    "A",
                    "rs61769339",
                    "OR4F16",
                    "0.97",
                    "0.0014",
                    "0.037",
                    "0.076",
                    "896.0",
                    "2.3e-07",
                ]
            )
            writer.writerow(
                [
                    "1",
                    "722429",
                    "G",
                    "A",
                    "",
                    "OR4F16",
                    "0.5",
                    "-0.14",
                    "0.21",
                    "0.0022",
                    "26.0",
                    "7.7e-05",
                ]
            )

        expected = json.dumps(
            {
                "variant_bins": [],
                "unbinned_variants": [
                    {
                        "chrom": "1",
                        "pos": 722429,
                        "ref": "G",
                        "alt": "A",
                        "rsids": "",
                        "nearest_genes": "OR4F16",
                        "pval": 0.5,
                        "beta": -0.14,
                        "sebeta": 0.21,
                        "maf": 0.0022,
                        "ac": 26.0,
                        "r2": 7.7e-05,
                    },
                    {
                        "chrom": "1",
                        "pos": 662622,
                        "ref": "G",
                        "alt": "A",
                        "rsids": "rs61769339",
                        "nearest_genes": "OR4F16",
                        "pval": 0.97,
                        "beta": 0.0014,
                        "sebeta": 0.037,
                        "maf": 0.076,
                        "ac": 896.0,
                        "r2": 2.3e-07,
                    },
                ],
            },
            sort_keys=False,
        )
        # Case with file output; check output against expected ouptut:
        try:
            (_, path1) = tempfile.mkstemp()
            with captured_output() as (_, _):
                args = _mock_args(
                    in_tsv=tmp_in_tsv_path, out_json=path1, out_plot_type="manhattan"
                )
                MOD.main(args)
            with open(path1, "rt") as fh:
                obs = fh.read().rstrip("\r\n")
            self.assertEqual(expected, obs)
        finally:
            cleanup_files(path1)
