"""Tests for the ``vadc_gwas_tools.subcommands.GetDescriptiveStatistics`` subcommand."""
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
from vadc_gwas_tools.common.const import CASE_COUNTS_VAR_ID, CONTROL_COUNTS_VAR_ID
from vadc_gwas_tools.subcommands import GetDescriptiveStatistics as MOD

# class MockArgs(NamedTuple):
#     source_id: int
#     source_population_cohort: int
#     variables_json: str
#     outcome: str
#     prefixed_breakdown_concept_id: str
#     output_csv_prefix: str
#     output_combined_json: str
#     hare_population: Optional[str] = None


# class TestDescriptiveStatisticsSubcommand(unittest.TestCase):
#     def setUp(self):
#         super().setUp()
#         self.continuous_variable_list = [
#             {"variable_type": "concept", "concept_id": 1001},
#             {"variable_type": "concept", "concept_id": 1002},
#             {
#                 "variable_type": "custom_dichotomous",
#                 "cohort_ids": [10, 20],
#                 "provided_name": "test123",
#             },
#         ]
#         self.binary_variable_list = [
#             {
#                 "variable_type": "custom_dichotomous",
#                 "cohort_ids": [10, 20],
#                 "provided_name": "test123",
#             },
#             {"variable_type": "concept", "concept_id": 1001},
#             {"variable_type": "concept", "concept_id": 1002},
#         ]
#         self.continuous_outcome = self.continuous_variable_list[0]
#         self.binary_outcome = self.binary_variable_list[0]

#     def test_main_continuous(self):
#         (_, fpath1) = tempfile.mkstemp()
#         (_, fpath2) = tempfile.mkstemp()
#         try:
#             with open(fpath1, 'wt') as o:
#                 json.dump(self.continuous_variable_list, o)
#             hare_population = "non-Hispanic Asian"
#             args = MockArgs(
#                 source_id=2,
#                 source_population_cohort=300,
#                 variables_json=fpath1,
#                 outcome=self.continuous_outcome,
#                 prefixed_breakdown_concept_id="ID_3",
#                 output_csv_prefix="/some/path/my_gwas_project",
#                 output_combined_json=fpath2,
#                 hare_population=hare_population,
#             )
#             variable_list_str = json.dumps(self.continuous_variable_list)
#             variable_objects = json.loads(
#                 variable_list_str,
#                 object_hook=CohortServiceClient.decode_concept_variable_json,
#             )
#             outcome_val = json.loads(
#                 json.dumps(self.continuous_outcome),
#                 object_hook=CohortServiceClient.decode_concept_variable_json,
#             )

#             # hare_population = "non-Hispanic Asian"
#             # hare_concept_id = 2000007029

#             with mock.patch(
#                 "vadc_gwas_tools.subcommands.get_attrition_csv.CohortServiceClient"
#             ) as mock_client, mock.patch(
#                 "vadc_gwas_tools.subcommands.get_attrition_csv.json.load"
#             ) as mock_json_load, mock.patch(
#                 "vadc_gwas_tools.subcommands.get_attrition_csv.json.loads"
#             ) as mock_json_loads:
#                 instance = mock_client.return_value
#                 # Mock the new get_concept_id_by_population method
#                 instance.get_concept_id_by_population.return_value = 2000007029
#                 # Mock the get_descriptive_statistics method
#                 instance.get_descriptive_statistics.return_value = [{"key": "value"}]
#                 mock_json_load.return_value = variable_objects[:]
#                 mock_json_loads.return_value = outcome_val

#                 # Call main()
#                 MOD.main(args)

#                 # Assertions for the new method
#                 instance.get_concept_id_by_population.assert_called_once_with(
#                     args.source_id, hare_population
#                 )
#                 instance.get_descriptive_statistics.assert_called_once_with(
#                     args.source_id,
#                     args.source_population_cohort,
#                     args.output_csv_prefix,
#                     variable_objects,
#                     args.prefixed_breakdown_concept_id,
#                     hare_population,
#                 )

#         finally:
#             cleanup_files([fpath1, fpath2])
class MockArgs:
    def __init__(
        self,
        source_id,
        source_population_cohort,
        variables_json,
        outcome,
        prefixed_breakdown_concept_id,
        output_csv_prefix,
        output_combined_json,
        hare_population=None,
    ):
        self.source_id = source_id
        self.source_population_cohort = source_population_cohort
        self.variables_json = variables_json
        self.outcome = outcome
        self.prefixed_breakdown_concept_id = prefixed_breakdown_concept_id
        self.output_csv_prefix = output_csv_prefix
        self.output_combined_json = output_combined_json
        self.hare_population = hare_population


class TestGetDescriptiveStatistics(unittest.TestCase):
    def setUp(self):
        self.client = CohortServiceClient()
        self.continuous_variable_list = [
            {"variable_type": "concept", "concept_id": 1001},
            {"variable_type": "concept", "concept_id": 1002},
            {
                "variable_type": "custom_dichotomous",
                "cohort_ids": [10, 20],
                "provided_name": "test123",
            },
        ]
        self.hare_population = "non-Hispanic Asian"
        self.hare_concept_id = 2000007029
        self.prefixed_breakdown_concept_id = "ID_3"
        self.mock_response = [
            [
                {'statistics': [{'key': 'value'}]},
                {'statistics': [{'key': 'value'}]},
                {},
            ],
            [
                {'statistics': [{'key': 'value'}]},
                {'statistics': [{'key': 'value'}]},
                {},
            ],
            [
                {'statistics': [{'key': 'value'}]},
                {'statistics': [{'key': 'value'}]},
                {},
            ],
        ]

    @mock.patch("vadc_gwas_tools.common.cohort_middleware.requests.post")
    @mock.patch(
        "vadc_gwas_tools.common.cohort_middleware.CohortServiceClient.get_concept_id_by_population"
    )
    @mock.patch(
        "vadc_gwas_tools.common.cohort_middleware.CohortServiceClient.get_header"
    )
    @mock.patch(
        "vadc_gwas_tools.common.cohort_middleware.CohortServiceClient.strip_concept_prefix"
    )
    def test_get_descriptive_statistics(
        self,
        mock_strip_concept_prefix,
        mock_get_header,
        mock_get_concept_id_by_population,
        mock_post,
    ):
        # Prepare test arguments
        (_, fpath1) = tempfile.mkstemp()
        (_, fpath2) = tempfile.mkstemp()
        try:
            with open(fpath1, 'wt') as o:
                json.dump(self.continuous_variable_list, o)

            args = MockArgs(
                source_id=2,
                source_population_cohort=300,
                variables_json=fpath1,
                outcome=self.continuous_variable_list[0],
                prefixed_breakdown_concept_id=self.prefixed_breakdown_concept_id,
                output_csv_prefix=fpath2,
                output_combined_json="/some/path/combined.json",
                hare_population=self.hare_population,
            )

            # Map variable list to correct objects
            variable_objects = []
            for var in self.continuous_variable_list:
                if var["variable_type"] == "concept":
                    variable_objects.append(ConceptVariableObject(**var))
                elif var["variable_type"] == "custom_dichotomous":
                    variable_objects.append(CustomDichotomousVariableObject(**var))

            # Mock behavior
            mock_get_concept_id_by_population.return_value = self.hare_concept_id
            mock_strip_concept_prefix.return_value = [1003]
            mock_get_header.return_value = {"Authorization": "Bearer test-token"}
            mock_post.return_value.status_code = 200
            mock_post.return_value.json.return_value = self.mock_response

            # Call the method
            response = self.client.get_descriptive_statistics(
                source_id=args.source_id,
                cohort_definition_id=args.source_population_cohort,
                local_path=args.output_csv_prefix,
                variable_objects=variable_objects,
                prefixed_breakdown_concept_id=args.prefixed_breakdown_concept_id,
                hare_population=args.hare_population,
            )

            # Assertions
            mock_get_concept_id_by_population.assert_called_once_with(
                args.source_id, args.hare_population, mock.ANY
            )
            mock_strip_concept_prefix.assert_called_once_with(
                args.prefixed_breakdown_concept_id
            )
            mock_post.assert_called_with(
                f"{self.client.service_url}/cohort-stats/by-source-id/{args.source_id}/by-cohort-definition-id/{args.source_population_cohort}/by-concept-id/1002",
                data=mock.ANY,
                headers={"Authorization": "Bearer test-token"},
                stream=True,
                timeout=(6.05, len(variable_objects) * 180),
            )
            self.assertEqual(response, self.mock_response)
        finally:
            cleanup_files([fpath1, fpath2])
