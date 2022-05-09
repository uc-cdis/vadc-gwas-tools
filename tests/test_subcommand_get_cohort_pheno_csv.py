"""Tests for the ``vadc_gwas_tools.subcommands.GetCohortPheno`` subcommand"""
import csv
import tempfile
import unittest
from typing import NamedTuple
from unittest import mock

from utils import captured_output, cleanup_files

from vadc_gwas_tools.common.cohort_middleware import CohortServiceClient
from vadc_gwas_tools.subcommands import GetCohortPheno as MOD

# class _mock_args(NamedTuple):
#    gds_filenames: List[str]
#    file_prefix: str
#    file_suffix: str
#    segment_file: str
#    output: Optional[str]


class TestGetCohortPhenoSubcommand(unittest.TestCase):
    def _return_generator(self, items):
        for item in items:
            yield item

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
