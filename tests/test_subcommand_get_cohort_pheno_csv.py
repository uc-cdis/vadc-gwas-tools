"""Tests for the ``vadc_gwas_tools.subcommands.GetCohortPheno`` subcommand"""
import csv
import gzip
import tempfile
import unittest
from typing import List, NamedTuple, Optional
from unittest import mock

from utils import captured_output, cleanup_files

from vadc_gwas_tools.common.cohort_middleware import CohortServiceClient
from vadc_gwas_tools.subcommands import GetCohortPheno as MOD


class _mock_args(NamedTuple):
    source_id: int
    case_cohort_id: int
    control_cohort_id: Optional[int]
    prefixed_concept_ids: List[str]
    output: str


class TestGetCohortPhenoSubcommand(unittest.TestCase):
    def test_process_continuous(self):
        client = CohortServiceClient()
        client.get_cohort_csv = mock.MagicMock(return_value=None)

        try:
            (fd1, fpath1) = tempfile.mkstemp()
            MOD._process_continuous(client, 1, 2, ["ID_1001", "ID_1002"], fpath1, None)
            client.get_cohort_csv.assert_called_with(
                1, 2, fpath1, ["ID_1001", "ID_1002"]
            )
        finally:
            cleanup_files(fpath1)

    def test_process_case_control(self):
        client = CohortServiceClient()
        client.get_cohort_csv = mock.MagicMock(return_value=None)

        (_, tmp_case_path) = tempfile.mkstemp()
        (_, tmp_control_path) = tempfile.mkstemp()
        (_, fpath1) = tempfile.mkstemp()

        with open(tmp_case_path, "wt") as o:
            writer = csv.writer(o)
            writer.writerow(["sample.id", "ID_1001", "ID_1002"])
            writer.writerow([1, 100.0, 300.0])
            writer.writerow([2, 300.0, 400.0])
        with open(tmp_control_path, "wt") as o:
            writer = csv.writer(o)
            writer.writerow(["sample.id", "ID_1001", "ID_1002"])
            writer.writerow([3, 200.0, 400.0])
            writer.writerow([4, 500.0, 300.0])
        try:
            with mock.patch("tempfile.mkstemp") as mock_tmpfile:
                mock_tmpfile.side_effect = [
                    ("a", tmp_case_path),
                    ("b", tmp_control_path),
                ]
                MOD._process_case_control(
                    client, 1, 2, 3, ["ID_1001", "ID_1002"], fpath1, None
                )
                self.assertEqual(client.get_cohort_csv.call_count, 2)
                exp_calls = [
                    mock.call(1, 2, tmp_case_path, ["ID_1001", "ID_1002"]),
                    mock.call(1, 3, tmp_control_path, ["ID_1001", "ID_1002"]),
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
                    ["sample.id", "ID_1001", "ID_1002", "CASE_CONTROL"],
                )
                curr = next(reader)
                self.assertEqual(curr["sample.id"], "1")
                self.assertEqual(curr["ID_1001"], "100.0")
                self.assertEqual(curr["CASE_CONTROL"], "1")

                curr = next(reader)
                self.assertEqual(curr["sample.id"], "2")
                self.assertEqual(curr["ID_1001"], "300.0")
                self.assertEqual(curr["CASE_CONTROL"], "1")

                curr = next(reader)
                self.assertEqual(curr["sample.id"], "3")
                self.assertEqual(curr["ID_1001"], "200.0")
                self.assertEqual(curr["CASE_CONTROL"], "0")

                curr = next(reader)
                self.assertEqual(curr["sample.id"], "4")
                self.assertEqual(curr["ID_1001"], "500.0")
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
            writer.writerow(["sample.id", "ID_1001", "ID_1002"])
            writer.writerow([1, 100.0, 300.0])
            writer.writerow([2, 300.0, 400.0])
        with open(tmp_control_path, "wt") as o:
            writer = csv.writer(o)
            writer.writerow(["sample.id", "ID_1001", "ID_1002"])
            writer.writerow([3, 200.0, 400.0])
            writer.writerow([4, 500.0, 300.0])
            writer.writerow([2, 300.0, 400.0])
        try:
            with mock.patch("tempfile.mkstemp") as mock_tmpfile:
                mock_tmpfile.side_effect = [
                    ("a", tmp_case_path),
                    ("b", tmp_control_path),
                ]
                with self.assertRaises(AssertionError) as e:
                    MOD._process_case_control(
                        client, 1, 2, 3, ["ID_1001", "ID_1002"], fpath1, None
                    )
                self.assertEqual(client.get_cohort_csv.call_count, 2)
                exp_calls = [
                    mock.call(1, 2, tmp_case_path, ["ID_1001", "ID_1002"]),
                    mock.call(1, 3, tmp_control_path, ["ID_1001", "ID_1002"]),
                ]
                self.assertEqual(client.get_cohort_csv.call_args_list, exp_calls)
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
            writer.writerow(["sample.id", "ID_1001", "ID_1002"])
            writer.writerow([1, 100.0, 300.0])
            writer.writerow([2, 300.0, 400.0])
        with open(tmp_control_path, "wt") as o:
            writer = csv.writer(o)
            writer.writerow(["sample.id", "ID_1001", "ID_1002"])
            writer.writerow([3, 200.0, 400.0])
            writer.writerow([4, 500.0, 300.0])
        try:
            with mock.patch("tempfile.mkstemp") as mock_tmpfile:
                mock_tmpfile.side_effect = [
                    ("a", tmp_case_path),
                    ("b", tmp_control_path),
                ]
                MOD._process_case_control(
                    client, 1, 2, 3, ["ID_1001", "ID_1002"], fpath1, None
                )
                self.assertEqual(client.get_cohort_csv.call_count, 2)
                exp_calls = [
                    mock.call(1, 2, tmp_case_path, ["ID_1001", "ID_1002"]),
                    mock.call(1, 3, tmp_control_path, ["ID_1001", "ID_1002"]),
                ]
                self.assertEqual(client.get_cohort_csv.call_args_list, exp_calls)

            with gzip.open(fpath1, "rt") as fh:
                reader = csv.DictReader(fh)
                self.assertEqual(
                    reader.fieldnames,
                    ["sample.id", "ID_1001", "ID_1002", "CASE_CONTROL"],
                )
                curr = next(reader)
                self.assertEqual(curr["sample.id"], "1")
                self.assertEqual(curr["ID_1001"], "100.0")
                self.assertEqual(curr["CASE_CONTROL"], "1")

                curr = next(reader)
                self.assertEqual(curr["sample.id"], "2")
                self.assertEqual(curr["ID_1001"], "300.0")
                self.assertEqual(curr["CASE_CONTROL"], "1")

                curr = next(reader)
                self.assertEqual(curr["sample.id"], "3")
                self.assertEqual(curr["ID_1001"], "200.0")
                self.assertEqual(curr["CASE_CONTROL"], "0")

                curr = next(reader)
                self.assertEqual(curr["sample.id"], "4")
                self.assertEqual(curr["ID_1001"], "500.0")
                self.assertEqual(curr["CASE_CONTROL"], "0")
        finally:
            cleanup_files([fpath1, tmp_case_path, tmp_control_path])


class TestGetCohortPhenoSubcommandMain(unittest.TestCase):
    def test_main_continuous(self):
        args = _mock_args(
            source_id=1,
            case_cohort_id=2,
            control_cohort_id=None,
            prefixed_concept_ids=["ID_1001", "ID_1002"],
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

    def test_main_case_control(self):
        args = _mock_args(
            source_id=1,
            case_cohort_id=2,
            control_cohort_id=3,
            prefixed_concept_ids=["ID_1001", "ID_1002"],
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

    def test_main_case_control_exception(self):
        args = _mock_args(
            source_id=1,
            case_cohort_id=2,
            control_cohort_id=2,
            prefixed_concept_ids=["ID_1001", "ID_1002"],
            output="/path/to/fake.csv",
        )

        with captured_output() as (_, _), self.assertRaises(AssertionError) as e:
            MOD.main(args)
        self.assertEqual(
            "Case cohort ID can't be the same as the Control cohort ID: 2 2",
            str(e.exception),
        )
