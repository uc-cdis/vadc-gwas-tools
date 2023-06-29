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
    def test_main_manhattan_ok(self):
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

    def test_main_qq_ok(self):
        (_, tmp_in_tsv_path) = tempfile.mkstemp(suffix=".tsv")

        with open(tmp_in_tsv_path, "wt") as o:
            writer = csv.writer(o, delimiter="\t")
            writer.writerow(
                [
                    "chrom",
                    "pos",
                    "ref",
                    "alt",
                    "pval",
                    "maf",
                    "af",
                    "ac",
                ]
            )
            tsv_records=[
                ["10", "3686300", "C", "T", "0.366", "0.239", "0.761", "2592"],
                ["10", "3686327", "C", "T", "0.262", "0.175", "0.825", "2808"],
                ["10", "3695498", "A", "G", "0.0394", "0.366", "0.366", "1246"],
                ["10", "30951327", "C", "T", "0.159", "0.198", "0.802", "2729"],
                ["10", "67382457", "T", "C", "0.165", "0.112", "0.888", "3024"],
                ["10", "130258262", "T", "C", "0.433", "0.382", "0.618","2102"],
                ["10", "130457159", "C", "T", "0.599", "0.261", "0.261", "889"]
            ]
            writer.writerows(tsv_records)

        expected_qq_json = json.dumps(
            {
                "by_maf": [
                    {
                        "maf_range": [
                            0.10999999940395355,
                            0.10999999940395355
                        ],
                        "count": 1,
                        "qq": {
                            "bins": [
                                [
                                    0.3010299956639812,
                                    0.7825160622596741
                                ]
                            ],
                            "max_exp_qval": 0.3010299956639812
                        }
                    },
                    {
                        "maf_range": [
                            0.18000000715255737,
                            0.20000000298023224
                        ],
                        "count": 2,
                        "qq": {
                            "bins": [
                                [
                                    0.1249274482005522,
                                    0.580983594506979
                                ],
                                [
                                    0.6020599913279624,
                                    0.7986028790473938
                                ]
                            ],
                            "max_exp_qval": 0.6020599913279624
                        }
                    },
                    {
                        "maf_range": [
                            0.23999999463558197,
                            0.25999999046325684
                        ],
                        "count": 2,
                        "qq": {
                            "bins": [
                                [
                                    0.1249274482005522,
                                    0.2212570468130262
                                ],
                                [
                                    0.6020599913279624,
                                    0.43649349371277274
                                ]
                            ],
                            "max_exp_qval": 0.6020599913279624
                        }
                    },
                    {
                        "maf_range": [
                            0.3700000047683716,
                            0.3799999952316284
                        ],
                        "count": 2,
                        "qq": {
                            "bins": [
                                [
                                    0.1249274482005522,
                                    0.361659734249115
                                ],
                                [
                                    0.6020599913279624,
                                    1.4045038223266602
                                ]
                            ],
                            "max_exp_qval": 0.6020599913279624
                        }
                    }
                ],
                "overall": {
                    "count": 7,
                    "gc_lambda": {
                        "0.5": 2.7656,
                        "0.1": 1.5685,
                        "0.01": 0.63958,
                        "0.001": 0.39192
                    }
                },
                "ci": [
                    {
                        "x": 0.85,
                        "y_min": 0.01,
                        "y_max": 2.14
                    },
                    {
                        "x": 0.54,
                        "y_min": 0.0,
                        "y_max": 1.48
                    }
                ]
            },
            sort_keys=False,
        )

        # Case with file output; check output against expected ouptut:
        try:
            (_, path1) = tempfile.mkstemp()
            
            # generate qq json output
            with captured_output() as (_, _):
                args_qq = _mock_args(
                    in_tsv=tmp_in_tsv_path, out_json=path1, out_plot_type="qq"
                )
                MOD.main(args_qq)
            with open(path1, "rt") as fh:
                obs_qq = fh.read().rstrip("\r\n")
            self.assertEqual(expected_qq_json, obs_qq)
        finally:
            cleanup_files(path1)