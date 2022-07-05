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
    case_cohort_id: int
    control_cohort_id: Optional[int]
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

    def test_process_continuous(self):
        client = CohortServiceClient()
        client.get_cohort_csv = mock.MagicMock(return_value=None)

        try:
            (fd1, fpath1) = tempfile.mkstemp()
            variable_str = json.dumps(self.variable_list)
            variable_obj = json.loads(
                variable_str,
                object_hook=CohortServiceClient.decode_concept_variable_json,
            )
            MOD._process_continuous(client, 1, 2, variable_obj, fpath1, None)
            client.get_cohort_csv.assert_called_with(1, 2, fpath1, variable_obj)
        finally:
            cleanup_files(fpath1)

    def test_extract_cohort_ids(self):
        (_, tmp_case_path) = tempfile.mkstemp()

        with open(tmp_case_path, "wt") as o:
            writer = csv.writer(o)
            writer.writerow(["sample.id", "ID_1001", "ID_1002"])
            writer.writerow([1, 100.0, 300.0])
            writer.writerow([2, 300.0, 400.0])
        try:
            res = MOD._extract_cohort_ids(tmp_case_path)
            exp = set(["1", "2"])
            self.assertEqual(res, exp)
        finally:
            cleanup_files([tmp_case_path])

    def test_process_case_control(self):
        client = CohortServiceClient()
        client.get_cohort_csv = mock.MagicMock(return_value=None)

        (_, tmp_case_path) = tempfile.mkstemp()
        (_, tmp_control_path) = tempfile.mkstemp()
        (_, fpath1) = tempfile.mkstemp()

        with open(tmp_case_path, "wt") as o:
            writer = csv.writer(o)
            writer.writerow(["sample.id", "ID_1001", "ID_1002", "ID_10_20"])
            writer.writerow([1, 100.0, 300.0, 0])
            writer.writerow([2, 300.0, 400.0, 1])
        with open(tmp_control_path, "wt") as o:
            writer = csv.writer(o)
            writer.writerow(["sample.id", "ID_1001", "ID_1002", "ID_10_20"])
            writer.writerow([3, 200.0, 400.0, 1])
            writer.writerow([4, 500.0, 300.0, 0])
        try:
            variable_str = json.dumps(self.variable_list)
            variable_obj = json.loads(
                variable_str,
                object_hook=CohortServiceClient.decode_concept_variable_json,
            )
            with mock.patch("tempfile.mkstemp") as mock_tmpfile:
                mock_tmpfile.side_effect = [
                    ("a", tmp_case_path),
                    ("b", tmp_control_path),
                ]
                MOD._process_case_control(client, 1, 2, 3, variable_obj, fpath1, None)
                self.assertEqual(client.get_cohort_csv.call_count, 2)
                exp_calls = [
                    mock.call(1, 2, tmp_case_path, variable_obj),
                    mock.call(1, 3, tmp_control_path, variable_obj),
                ]
                self.assertEqual(client.get_cohort_csv.call_args_list, exp_calls)

            with self.assertRaises(OSError) as _:
                with gzip.open(fpath1, "rt") as fh:
                    for line in fh:
                        pass

            with open(fpath1, "rt") as fh:
                reader = csv.DictReader(fh)
                self.assertEqual(
                    reader.fieldnames,
                    ["sample.id", "ID_1001", "ID_1002", "ID_10_20", "CASE_CONTROL"],
                )
                curr = next(reader)
                self.assertEqual(curr["sample.id"], "1")
                self.assertEqual(curr["ID_1001"], "100.0")
                self.assertEqual(curr["ID_10_20"], "0")
                self.assertEqual(curr["CASE_CONTROL"], "1")

                curr = next(reader)
                self.assertEqual(curr["sample.id"], "2")
                self.assertEqual(curr["ID_1001"], "300.0")
                self.assertEqual(curr["ID_10_20"], "1")
                self.assertEqual(curr["CASE_CONTROL"], "1")

                curr = next(reader)
                self.assertEqual(curr["sample.id"], "3")
                self.assertEqual(curr["ID_1001"], "200.0")
                self.assertEqual(curr["ID_10_20"], "1")
                self.assertEqual(curr["CASE_CONTROL"], "0")

                curr = next(reader)
                self.assertEqual(curr["sample.id"], "4")
                self.assertEqual(curr["ID_1001"], "500.0")
                self.assertEqual(curr["ID_10_20"], "0")
                self.assertEqual(curr["CASE_CONTROL"], "0")
        finally:
            cleanup_files([fpath1, tmp_case_path, tmp_control_path])

    def test_process_case_control_duplicate_samples(self):
        client = CohortServiceClient()
        client.get_cohort_csv = mock.MagicMock(return_value=None)

        (_, tmp_case_path) = tempfile.mkstemp()
        (_, tmp_control_path) = tempfile.mkstemp()
        (_, fpath1) = tempfile.mkstemp()

        with open(tmp_case_path, "wt") as o:
            writer = csv.writer(o)
            writer.writerow(["sample.id", "ID_1001", "ID_1002", "ID_10_20"])
            writer.writerow([1, 100.0, 300.0, 0])
            writer.writerow([2, 300.0, 400.0, 1])
        with open(tmp_control_path, "wt") as o:
            writer = csv.writer(o)
            writer.writerow(["sample.id", "ID_1001", "ID_1002", "ID_10_20"])
            writer.writerow([3, 200.0, 400.0, 1])
            writer.writerow([4, 500.0, 300.0, 0])
            writer.writerow([2, 300.0, 400.0, 1])
        try:
            variable_str = json.dumps(self.variable_list)
            variable_obj = json.loads(
                variable_str,
                object_hook=CohortServiceClient.decode_concept_variable_json,
            )

            with mock.patch("tempfile.mkstemp") as mock_tmpfile:
                mock_tmpfile.side_effect = [
                    ("a", tmp_case_path),
                    ("b", tmp_control_path),
                ]
                with captured_output() as (_, serr):
                    logger = Logger.get_logger("test_get_cohort_pheno")
                    MOD._process_case_control(
                        client, 1, 2, 3, variable_obj, fpath1, logger
                    )
                self.assertEqual(client.get_cohort_csv.call_count, 2)
                exp_calls = [
                    mock.call(1, 2, tmp_case_path, variable_obj),
                    mock.call(1, 3, tmp_control_path, variable_obj),
                ]
                self.assertEqual(client.get_cohort_csv.call_args_list, exp_calls)

                stderr = serr.getvalue()
                self.assertTrue(
                    "Found 1 overlapping samples between case/control" in stderr
                )

            with open(fpath1, "rt") as fh:
                reader = csv.DictReader(fh)
                self.assertEqual(
                    reader.fieldnames,
                    ["sample.id", "ID_1001", "ID_1002", "ID_10_20", "CASE_CONTROL"],
                )
                curr = next(reader)
                self.assertEqual(curr["sample.id"], "1")
                self.assertEqual(curr["ID_1001"], "100.0")
                self.assertEqual(curr["ID_10_20"], "0")
                self.assertEqual(curr["CASE_CONTROL"], "1")

                curr = next(reader)
                self.assertEqual(curr["sample.id"], "3")
                self.assertEqual(curr["ID_1001"], "200.0")
                self.assertEqual(curr["ID_10_20"], "1")
                self.assertEqual(curr["CASE_CONTROL"], "0")

                curr = next(reader)
                self.assertEqual(curr["sample.id"], "4")
                self.assertEqual(curr["ID_1001"], "500.0")
                self.assertEqual(curr["ID_10_20"], "0")
                self.assertEqual(curr["CASE_CONTROL"], "0")

        finally:
            cleanup_files([fpath1, tmp_case_path, tmp_control_path])

    def test_process_case_control_gzip(self):
        client = CohortServiceClient()
        client.get_cohort_csv = mock.MagicMock(return_value=None)

        (_, tmp_case_path) = tempfile.mkstemp()
        (_, tmp_control_path) = tempfile.mkstemp()
        (_, fpath1) = tempfile.mkstemp(suffix=".csv.gz")

        with open(tmp_case_path, "wt") as o:
            writer = csv.writer(o)
            writer.writerow(["sample.id", "ID_1001", "ID_1002", "ID_10_20"])
            writer.writerow([1, 100.0, 300.0, 0])
            writer.writerow([2, 300.0, 400.0, 1])
        with open(tmp_control_path, "wt") as o:
            writer = csv.writer(o)
            writer.writerow(["sample.id", "ID_1001", "ID_1002", "ID_10_20"])
            writer.writerow([3, 200.0, 400.0, 1])
            writer.writerow([4, 500.0, 300.0, 0])
        try:
            variable_str = json.dumps(self.variable_list)
            variable_obj = json.loads(
                variable_str,
                object_hook=CohortServiceClient.decode_concept_variable_json,
            )

            with mock.patch("tempfile.mkstemp") as mock_tmpfile:
                mock_tmpfile.side_effect = [
                    ("a", tmp_case_path),
                    ("b", tmp_control_path),
                ]
                MOD._process_case_control(client, 1, 2, 3, variable_obj, fpath1, None)
                self.assertEqual(client.get_cohort_csv.call_count, 2)
                exp_calls = [
                    mock.call(1, 2, tmp_case_path, variable_obj),
                    mock.call(1, 3, tmp_control_path, variable_obj),
                ]
                self.assertEqual(client.get_cohort_csv.call_args_list, exp_calls)

            with gzip.open(fpath1, "rt") as fh:
                reader = csv.DictReader(fh)
                self.assertEqual(
                    reader.fieldnames,
                    ["sample.id", "ID_1001", "ID_1002", "ID_10_20", "CASE_CONTROL"],
                )
                curr = next(reader)
                self.assertEqual(curr["sample.id"], "1")
                self.assertEqual(curr["ID_1001"], "100.0")
                self.assertEqual(curr["ID_10_20"], "0")
                self.assertEqual(curr["CASE_CONTROL"], "1")

                curr = next(reader)
                self.assertEqual(curr["sample.id"], "2")
                self.assertEqual(curr["ID_1001"], "300.0")
                self.assertEqual(curr["ID_10_20"], "1")
                self.assertEqual(curr["CASE_CONTROL"], "1")

                curr = next(reader)
                self.assertEqual(curr["sample.id"], "3")
                self.assertEqual(curr["ID_1001"], "200.0")
                self.assertEqual(curr["ID_10_20"], "1")
                self.assertEqual(curr["CASE_CONTROL"], "0")

                curr = next(reader)
                self.assertEqual(curr["sample.id"], "4")
                self.assertEqual(curr["ID_1001"], "500.0")
                self.assertEqual(curr["ID_10_20"], "0")
                self.assertEqual(curr["CASE_CONTROL"], "0")
        finally:
            cleanup_files([fpath1, tmp_case_path, tmp_control_path])


class TestGetCohortPhenoSubcommandMain(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.variable_list = [
            {"variable_type": "concept", "concept_id": 1001},
            {"variable_type": "concept", "concept_id": 1002},
            {"variable_type": "custom_dichotomous", "cohort_ids": [10, 20]},
        ]

    def test_main_continuous(self):
        (_, fpath1) = tempfile.mkstemp()
        try:
            with open(fpath1, 'wt') as o:
                json.dump(self.variable_list, o)

            args = _mock_args(
                source_id=1,
                case_cohort_id=2,
                control_cohort_id=None,
                variables_json=fpath1,
                output="/path/to/fake.csv",
            )

            MOD._process_case_control = mock.MagicMock(return_value=None)
            MOD._process_continuous = mock.MagicMock(return_value=None)
            with captured_output() as (_, se):
                MOD.main(args)
                MOD._process_case_control.assert_not_called()
                MOD._process_continuous.assert_called_once()
            serr = se.getvalue()
            self.assertTrue('Continuous phenotype Design...' in serr)
        finally:
            cleanup_files(fpath1)

    def test_main_case_control(self):
        (_, fpath1) = tempfile.mkstemp()
        try:
            with open(fpath1, 'wt') as o:
                json.dump(self.variable_list, o)
            args = _mock_args(
                source_id=1,
                case_cohort_id=2,
                control_cohort_id=3,
                variables_json=fpath1,
                output="/path/to/fake.csv",
            )

            MOD._process_case_control = mock.MagicMock(return_value=None)
            MOD._process_continuous = mock.MagicMock(return_value=None)
            with captured_output() as (_, se):
                MOD.main(args)
                MOD._process_case_control.assert_called_once()
                MOD._process_continuous.assert_not_called()
            serr = se.getvalue()
            self.assertTrue('Case-Control Design...' in serr)
            self.assertTrue('Case Cohort: 2; Control Cohort: 3' in serr)
        finally:
            cleanup_files(fpath1)

    def test_main_case_control_exception(self):
        (_, fpath1) = tempfile.mkstemp()
        try:
            with open(fpath1, 'wt') as o:
                json.dump(self.variable_list, o)
            args = _mock_args(
                source_id=1,
                case_cohort_id=2,
                control_cohort_id=2,
                variables_json=fpath1,
                output="/path/to/fake.csv",
            )

            with captured_output() as (_, _), self.assertRaises(AssertionError) as e:
                MOD.main(args)
            self.assertEqual(
                "Case cohort ID can't be the same as the Control cohort ID: 2 2",
                str(e.exception),
            )
        finally:
            cleanup_files(fpath1)
