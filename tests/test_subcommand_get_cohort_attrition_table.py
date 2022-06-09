"""Tests for the ``vadc_gwas_tools.subcommands.GetCohortAttritionTable`` subcommand."""
import unittest
from typing import List, NamedTuple, Optional
from unittest import mock

from vadc_gwas_tools.subcommands import GetCohortAttritionTable as MOD


class MockArgs(NamedTuple):
    source_id: int
    case_cohort_id: int
    control_cohort_id: Optional[int]
    prefixed_concept_ids: List[str]
    prefixed_breakdown_concept_id: str
    output_prefix: str


class TestGetCohortAttritionTableSubcommand(unittest.TestCase):
    def test_main_continuous(self):
        args = MockArgs(
            source_id=2,
            case_cohort_id=9,
            control_cohort_id=None,
            prefixed_concept_ids=["ID_1", "ID_2"],
            prefixed_breakdown_concept_id="ID_3",
            output_prefix="/some/path/my_gwas_project",
        )

        with mock.patch(
            "vadc_gwas_tools.subcommands.get_attrition_csv.CohortServiceClient"
        ) as mock_client:
            instance = mock_client.return_value
            instance.get_attrition_breakdown_csv.return_value = None
            MOD.main(args)
            instance.get_attrition_breakdown_csv.assert_called_once()
            instance.get_attrition_breakdown_csv.assert_called_with(
                args.source_id,
                args.case_cohort_id,
                f"{args.output_prefix}.case_cohort.attrition_table.csv",
                args.prefixed_concept_ids,
                args.prefixed_breakdown_concept_id,
            )

    def test_main_case_control(self):
        args = MockArgs(
            source_id=2,
            case_cohort_id=9,
            control_cohort_id=4,
            prefixed_concept_ids=["ID_1", "ID_2"],
            prefixed_breakdown_concept_id="ID_3",
            output_prefix="/some/path/my_gwas_project",
        )

        with mock.patch(
            "vadc_gwas_tools.subcommands.get_attrition_csv.CohortServiceClient"
        ) as mock_client:
            instance = mock_client.return_value
            instance.get_attrition_breakdown_csv.return_value = None
            MOD.main(args)

            self.assertEqual(instance.get_attrition_breakdown_csv.call_count, 2)
            instance.get_attrition_breakdown_csv.assert_has_calls(
                [
                    mock.call(
                        args.source_id,
                        args.case_cohort_id,
                        f"{args.output_prefix}.case_cohort.attrition_table.csv",
                        args.prefixed_concept_ids,
                        args.prefixed_breakdown_concept_id,
                    ),
                    mock.call(
                        args.source_id,
                        args.control_cohort_id,
                        f"{args.output_prefix}.control_cohort.attrition_table.csv",
                        args.prefixed_concept_ids,
                        args.prefixed_breakdown_concept_id,
                    ),
                ]
            )
