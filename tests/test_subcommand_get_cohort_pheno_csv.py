"""Tests for the ``vadc_gwas_tools.subcommands.GetCohortPheno`` subcommand"""
import csv
import gzip
import json
import tempfile
import unittest
from typing import List, NamedTuple, Optional
from unittest import mock

from utils import captured_output, cleanup_files

from vadc_gwas_tools.common.cohort_middleware import CohortServiceClient
from vadc_gwas_tools.common.logger import Logger
from vadc_gwas_tools.subcommands import GetCohortPheno as MOD


class _mock_args(NamedTuple):
    source_id: int
    source_population_cohort: int
    variables_json: str
    output: str


class TestGetCohortPhenoSubcommand(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.variable_list = [
            {"variable_type": "concept", "concept_id": 1001},
            {"variable_type": "concept", "concept_id": 1002},
            {"variable_type": "custom_dichotomous", "cohort_ids": [10, 20]},
        ]

    def test_process_variables_csv(self):
        client = CohortServiceClient()
        client.get_cohort_csv = mock.MagicMock(return_value=None)

        try:
            (fd1, fpath1) = tempfile.mkstemp()
            variable_str = json.dumps(self.variable_list)
            variable_obj = json.loads(
                variable_str,
                object_hook=CohortServiceClient.decode_concept_variable_json,
            )
            MOD._process_variables_csv(client, 1, 2, variable_obj, fpath1, None)
            client.get_cohort_csv.assert_called_with(1, 2, fpath1, variable_obj)
        finally:
            cleanup_files(fpath1)

    def test_process_variable_csv_gzip(self):
        client = CohortServiceClient()
        client.get_cohort_csv = mock.MagicMock(return_value=None)

        (_, tmp_csv_path) = tempfile.mkstemp(suffix=".csv.gz")

        with gzip.open(tmp_csv_path, "wt") as o:
            writer = csv.writer(o)
            writer.writerow(["sample.id", "ID_1001", "ID_1002", "ID_10_20"])
            writer.writerow([1, 100.0, 300.0, 0])
            writer.writerow([2, 300.0, 400.0, 1])

        try:
            variable_str = json.dumps(self.variable_list)
            variable_obj = json.loads(
                variable_str,
                object_hook=CohortServiceClient.decode_concept_variable_json,
            )

            with mock.patch("tempfile.mkstemp") as mock_tmpfile:
                mock_tmpfile.side_effect = [
                    ("a", tmp_csv_path)
                ]
                MOD._process_variables_csv(client, 1, 2, variable_obj, tmp_csv_path, None)
                self.assertEqual(client.get_cohort_csv.call_count, 1)
                exp_calls = [
                    mock.call(1, 2, tmp_csv_path, variable_obj)
                ]
                self.assertEqual(client.get_cohort_csv.call_args_list, exp_calls)

            with gzip.open(tmp_csv_path, "rt") as fh:
                reader = csv.DictReader(fh)
                self.assertEqual(
                    reader.fieldnames,
                    ["sample.id", "ID_1001", "ID_1002", "ID_10_20"],
                )
                curr = next(reader)
                self.assertEqual(curr["sample.id"], "1")
                self.assertEqual(curr["ID_1001"], "100.0")
                self.assertEqual(curr["ID_10_20"], "0")

                curr = next(reader)
                self.assertEqual(curr["sample.id"], "2")
                self.assertEqual(curr["ID_1001"], "300.0")
                self.assertEqual(curr["ID_10_20"], "1")

        finally:
            cleanup_files(tmp_csv_path)



class TestGetCohortPhenoSubcommandMain(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.variable_list = [
            {"variable_type": "concept", "concept_id": 1001},
            {"variable_type": "concept", "concept_id": 1002},
            {"variable_type": "custom_dichotomous", "cohort_ids": [10, 20]},
        ]

    def test_main(self):
        (_, fpath1) = tempfile.mkstemp()
        try:
            with open(fpath1, 'wt') as o:
                json.dump(self.variable_list, o)

            args = _mock_args(
                source_id=1,
                source_population_cohort=2,
                variables_json=fpath1,
                output="/path/to/fake.csv",
            )

            MOD._process_variables_csv = mock.MagicMock(return_value=None)
            with captured_output() as (_, se):
                MOD.main(args)
                MOD._process_variables_csv.assert_called_once()
            serr = se.getvalue()
            self.assertTrue('GWAS unified workflow design...' in serr)
        finally:
            cleanup_files(fpath1)
