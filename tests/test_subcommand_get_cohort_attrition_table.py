"""Tests for the ``vadc_gwas_tools.subcommands.GetCohortAttritionTable`` subcommand."""
import json
import tempfile
import unittest
from typing import List, NamedTuple, Optional
from unittest import mock

from utils import cleanup_files

from vadc_gwas_tools.common.cohort_middleware import (
    CohortServiceClient,
    ConceptVariableObject,
    CustomDichotomousVariableObject,
)
from vadc_gwas_tools.subcommands import GetCohortAttritionTable as MOD


class MockArgs(NamedTuple):
    source_id: int
    source_population_cohort: int
    variables_json: str
    outcome: str
    prefixed_breakdown_concept_id: str
    output_prefix: str


class TestGetCohortAttritionTableSubcommand(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.continuous_variable_list = [
            {"variable_type": "concept", "concept_id": 1001},
            {"variable_type": "concept", "concept_id": 1002},
            {
                "variable_type": "custom_dichotomous",
                "cohort_ids": [10, 20],
                "provided_name": "test123",
            },
        ]
        self.binary_variable_list = [
            {
                "variable_type": "custom_dichotomous",
                "cohort_ids": [10, 20],
                "provided_name": "test123",
            },
            {"variable_type": "concept", "concept_id": 1001},
            {"variable_type": "concept", "concept_id": 1002},
        ]
        self.continuous_outcome = self.continuous_variable_list[0]
        self.binary_outcome = self.binary_variable_list[0]

    def test_main_continuous(self):
        (_, fpath1) = tempfile.mkstemp()
        try:
            with open(fpath1, 'wt') as o:
                json.dump(self.continuous_variable_list, o)
            args = MockArgs(
                source_id=2,
                source_population_cohort=300,
                variables_json=fpath1,
                outcome=self.continuous_outcome,
                prefixed_breakdown_concept_id="ID_3",
                output_prefix="/some/path/my_gwas_project",
            )
            variable_list_str = json.dumps(self.continuous_variable_list)
            variable_objects = json.loads(
                variable_list_str,
                object_hook=CohortServiceClient.decode_concept_variable_json,
            )
            outcome_val = json.loads(
                json.dumps(self.continuous_outcome),
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
                mock_json.loads.return_value = outcome_val
                # Call main()
                MOD.main(args)

                instance.get_attrition_breakdown_csv.assert_called_once()
                instance.get_attrition_breakdown_csv.assert_called_with(
                    args.source_id,
                    args.source_population_cohort,
                    f"{args.output_prefix}.source_cohort.attrition_table.csv",
                    variable_objects,
                    args.prefixed_breakdown_concept_id,
                )
        finally:
            cleanup_files(fpath1)

    def test_main_case_control(self):
        (_, fpath1) = tempfile.mkstemp()
        try:
            with open(fpath1, 'wt') as o:
                json.dump(self.binary_variable_list, o)
            args = MockArgs(
                source_id=2,
                source_population_cohort=300,
                variables_json=fpath1,
                outcome=self.binary_outcome,
                prefixed_breakdown_concept_id="ID_3",
                output_prefix="/some/path/my_gwas_project",
            )
            variable_list_str = json.dumps(self.binary_variable_list)
            variable_objects = json.loads(
                variable_list_str,
                object_hook=CohortServiceClient.decode_concept_variable_json,
            )
            outcome_val = json.loads(
                json.dumps(self.binary_outcome),
                object_hook=CohortServiceClient.decode_concept_variable_json,
            )

            # Additional variable object needs to be inserted after case-cohort
            # variable object to get the overlap between case/control and
            # source cohort
            '''
            control_variable_list, case_variable_list = MOD._get_case_control_variable_lists_(
                variable_objects,
                outcome_val,
                300
            )
            '''
            control_variable_list = variable_objects[:]
            new_control_dvar = CustomDichotomousVariableObject(
                variable_type="custom_dichotomous",
                cohort_ids=[20, 300],
                provided_name="Control cohort only",
            )
            control_variable_list.insert(1, new_control_dvar)
            case_variable_list = variable_objects[:]
            new_case_dvar = CustomDichotomousVariableObject(
                variable_type="custom_dichotomous",
                cohort_ids=[10, 300],
                provided_name="Case cohort only",
            )
            case_variable_list.insert(1, new_case_dvar)
            #mock_binary_list.return_value = (control_variable_list, case_variable_list)

            with mock.patch(
                "vadc_gwas_tools.subcommands.get_attrition_csv.CohortServiceClient"
            ) as mock_client, mock.patch(
                "vadc_gwas_tools.subcommands.get_attrition_csv.json"
            ) as mock_json, mock.patch(
                "vadc_gwas_tools.subcommands.get_attrition_csv.GetCohortAttritionTable._get_case_control_variable_lists_"
            ) as mock_binary_list:
                instance = mock_client.return_value
                instance.get_attrition_breakdown_csv.return_value = None
                mock_json.load.return_value = variable_objects
                mock_json.loads.return_value = outcome_val
                mock_binary_list.return_value = (control_variable_list, case_variable_list)
                
                # call main()
                MOD.main(args)

                self.assertEqual(instance.get_attrition_breakdown_csv.call_count, 2)
                instance.get_attrition_breakdown_csv.assert_has_calls(
                    [
                        mock.call(
                            args.source_id,
                            args.source_population_cohort,
                            f"{args.output_prefix}.control_cohort.attrition_table.csv",
                            control_variable_list,
                            args.prefixed_breakdown_concept_id,
                        ),
                        mock.call(
                            args.source_id,
                            args.source_population_cohort,
                            f"{args.output_prefix}.case_cohort.attrition_table.csv",
                            case_variable_list,
                            args.prefixed_breakdown_concept_id,
                        ),
                    ]
                )
        finally:
            cleanup_files(fpath1)

    def test_get_control_case_variable_list(self):
        variable_list_str = json.dumps(self.binary_variable_list)
        variable_objects = json.loads(
            variable_list_str,
            object_hook=CohortServiceClient.decode_concept_variable_json,
        )
        outcome_val = json.loads(
            json.dumps(self.binary_outcome),
            object_hook=CohortServiceClient.decode_concept_variable_json,
        )
        (
            control_variable_list,
            case_variable_list,
        ) = MOD._get_case_control_variable_lists_(variable_objects, outcome_val, 300)
        self.assertEqual(control_variable_list[1].cohort_ids, [20, 300])
        self.assertEqual(case_variable_list[1].cohort_ids, [10, 300])
