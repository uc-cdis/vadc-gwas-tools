"""Tests for the ``vadc_gwas_tools.subcommands.GetCohortAttritionTable`` subcommand."""
import json
import tempfile
import unittest
from typing import List, NamedTuple, Optional
from unittest import mock

from utils import cleanup_files

from vadc_gwas_tools.common.cohort_middleware import (
    CohortServiceClient,
    CustomDichotomousVariableObject,
)
from vadc_gwas_tools.subcommands import GetCohortAttritionTable as MOD


class MockArgs(NamedTuple):
    source_id: int
    case_cohort_id: int
    control_cohort_id: Optional[int]
    variables_json: str
    prefixed_breakdown_concept_id: str
    output_prefix: str


class TestGetCohortAttritionTableSubcommand(unittest.TestCase):
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
            args = MockArgs(
                source_id=2,
                case_cohort_id=9,
                control_cohort_id=None,
                variables_json=fpath1,
                prefixed_breakdown_concept_id="ID_3",
                output_prefix="/some/path/my_gwas_project",
            )
            variable_list_str = json.dumps(self.variable_list)
            variable_objects = json.loads(
                variable_list_str,
                object_hook=CohortServiceClient.decode_concept_variable_json,
            )

            with mock.patch(
                "vadc_gwas_tools.subcommands.get_attrition_csv.CohortServiceClient"
            ) as mock_client, mock.patch(
                "vadc_gwas_tools.subcommands.get_attrition_csv.json"
            ) as mock_json:
                instance = mock_client.return_value
                instance.get_attrition_breakdown_csv.return_value = None
                mock_json.load.return_value = variable_objects[:]
                MOD.main(args)
                instance.get_attrition_breakdown_csv.assert_called_once()
                instance.get_attrition_breakdown_csv.assert_called_with(
                    args.source_id,
                    args.case_cohort_id,
                    f"{args.output_prefix}.case_cohort.attrition_table.csv",
                    variable_objects,
                    args.prefixed_breakdown_concept_id,
                )
        finally:
            cleanup_files(fpath1)

    def test_main_case_control(self):
        (_, fpath1) = tempfile.mkstemp()
        try:
            with open(fpath1, 'wt') as o:
                json.dump(self.variable_list, o)
            args = MockArgs(
                source_id=2,
                case_cohort_id=9,
                control_cohort_id=4,
                variables_json=fpath1,
                prefixed_breakdown_concept_id="ID_3",
                output_prefix="/some/path/my_gwas_project",
            )
            variable_list_str = json.dumps(self.variable_list)
            variable_objects = json.loads(
                variable_list_str,
                object_hook=CohortServiceClient.decode_concept_variable_json,
            )
            # Add new variable that includes the inserted custom dichotomous
            # to handle overlaps and a *copy* of the variable_objects so the
            # test is more clear that it is handling this. Otherwise the
            # side effects of the insert in the command will mutate this variable
            variable_objects_with_case_control = [
                CustomDichotomousVariableObject(
                    variable_type="custom_dichotomous",
                    cohort_ids=[args.control_cohort_id, args.case_cohort_id],
                    provided_name="Added filter to remove case/control overlap (if any)",
                )
            ] + variable_objects[:]

            with mock.patch(
                "vadc_gwas_tools.subcommands.get_attrition_csv.CohortServiceClient"
            ) as mock_client, mock.patch(
                "vadc_gwas_tools.subcommands.get_attrition_csv.json"
            ) as mock_json:
                instance = mock_client.return_value
                instance.get_attrition_breakdown_csv.return_value = None
                mock_json.load.return_value = variable_objects

                MOD.main(args)

                self.assertEqual(instance.get_attrition_breakdown_csv.call_count, 2)
                instance.get_attrition_breakdown_csv.assert_has_calls(
                    [
                        mock.call(
                            args.source_id,
                            args.case_cohort_id,
                            f"{args.output_prefix}.case_cohort.attrition_table.csv",
                            variable_objects_with_case_control,
                            args.prefixed_breakdown_concept_id,
                        ),
                        mock.call(
                            args.source_id,
                            args.control_cohort_id,
                            f"{args.output_prefix}.control_cohort.attrition_table.csv",
                            variable_objects_with_case_control,
                            args.prefixed_breakdown_concept_id,
                        ),
                    ]
                )
        finally:
            cleanup_files(fpath1)
