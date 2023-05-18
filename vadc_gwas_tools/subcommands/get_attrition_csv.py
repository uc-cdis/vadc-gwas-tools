"""Communicates with cohort middleware service to extract the attribution
breakdown CSV(s) and separately a JSON version.

@author: Kyle Hernandez <kmhernan@uchicago.edu>
"""
import csv
import json
from argparse import ArgumentParser, Namespace
from typing import Any, Dict, List, Optional, Tuple, Union

from vadc_gwas_tools.common.cohort_middleware import (
    CohortServiceClient,
    ConceptVariableObject,
    CustomDichotomousVariableObject,
)
from vadc_gwas_tools.common.logger import Logger
from vadc_gwas_tools.subcommands import Subcommand


class GetCohortAttritionTable(Subcommand):
    @classmethod
    def __add_arguments__(cls, parser: ArgumentParser) -> None:
        """Add the subcommand params"""
        parser.add_argument(
            "--source_id", required=True, type=int, help="The cohort source ID."
        )
        parser.add_argument(
            "--source_population_cohort",
            required=True,
            type=int,
            default=None,
            help=(
                "The cohort ID for source population cohort. This is required for both "
                "dichotomous and continuous workflow."
            ),
        )
        parser.add_argument(
            "--outcome",
            required=True,
            type=str,
            help="JSON formatted string of outcome variable.",
        )
        parser.add_argument(
            "--variables_json",
            required=True,
            help="Path to the JSON file containing the variable objects.",
        )
        parser.add_argument(
            "--prefixed_breakdown_concept_id",
            required=True,
            type=str,
            help="Prefixed concept ID to use for stratification (e.g., HARE concept).",
        )
        parser.add_argument(
            "--output_csv_prefix",
            required=True,
            type=str,
            help="Prefix to use for outputs (1 csv for quantitative, 2 csvs for case-control).",
        )
        parser.add_argument(
            "--output_combined_json",
            required=True,
            type=str,
            help="Path to write the combined JSON attrition.",
        )

    @classmethod
    def main(cls, options: Namespace) -> None:
        """
        Entrypoint for GetCohortAttritionTable
        """
        logger = Logger.get_logger(cls.__tool_name__())
        logger.info(cls.__get_description__())

        is_case_control = False

        outcome_val = json.loads(
            options.outcome,
            object_hook=CohortServiceClient.decode_concept_variable_json,
        )
        if isinstance(outcome_val, CustomDichotomousVariableObject):
            is_case_control = True
            outcome_control_cohort, outcome_case_cohort = outcome_val.cohort_ids
            outcome_case_control_provided_name = outcome_val.provided_name
        else:
            pass

        # Load JSON object
        with open(options.variables_json, "rt") as fh:
            variables = json.load(
                fh, object_hook=CohortServiceClient.decode_concept_variable_json
            )
            # Sanity check if the first element
            # of variables list is the outcome
            assert outcome_val == variables[0], (
                "First element of variable list is not equal to the outcome\n"
                f"First element of variables: {variables[0]}\n"
                f"Outcome: {outcome_val}"
            )

        # Client
        client = CohortServiceClient()

        # Call attrition table
        if not is_case_control:  # Continuous workflow
            # log info
            logger.info("Continuous Design...")
            logger.info(
                (f"Source Population Cohort: {options.source_population_cohort}; ")
            )
            # Call cohort-middleware for continuous workflow
            continuous_csv = (
                f"{options.output_csv_prefix}.source_cohort.attrition_table.csv"
            )
            logger.info(
                f"Writing continuous workflow attrition table to {continuous_csv}"
            )
            client.get_attrition_breakdown_csv(
                options.source_id,
                options.source_population_cohort,
                continuous_csv,
                variables,
                options.prefixed_breakdown_concept_id,
            )
            # Generate JSON
            case_attrition_json = cls._format_attrition_for_json(continuous_csv, 'case')
            continuous_attrition_json = [case_attrition_json]

            with open(options.output_combined_json, 'wt') as o:
                json.dump(continuous_attrition_json, o, indent=2)

        else:  # Case-control workflow
            # logger info
            logger.info("Case-Control Design...")
            logger.info(
                (
                    f"Source Population Cohort: {options.source_population_cohort}; "
                    f"Case Cohort: {outcome_case_cohort}"
                    f"Control Cohort: {outcome_control_cohort}"
                )
            )

            (
                control_variable_list,
                case_variable_list,
            ) = cls._get_case_control_variable_lists_(
                variables, outcome_val, options.source_population_cohort
            )

            # Call cohort-middleware for control cohort
            control_csv = (
                f"{options.output_csv_prefix}.control_cohort.attrition_table.csv"
            )
            logger.info(
                f"Writing case-control control cohort attrition table to {control_csv}"
            )
            client.get_attrition_breakdown_csv(
                options.source_id,
                options.source_population_cohort,
                control_csv,
                control_variable_list,
                options.prefixed_breakdown_concept_id,
            )

            # Call cohort-middleware for case cohort
            case_csv = f"{options.output_csv_prefix}.case_cohort.attrition_table.csv"
            logger.info(
                f"Writing case-control case cohort attrition table to {case_csv}"
            )
            client.get_attrition_breakdown_csv(
                options.source_id,
                options.source_population_cohort,
                case_csv,
                case_variable_list,
                options.prefixed_breakdown_concept_id,
            )

            # Generate JSON
            case_attrition_json = cls._format_attrition_for_json(case_csv, 'case')
            control_attrition_json = cls._format_attrition_for_json(
                control_csv, 'control'
            )
            dichotomous_attrition_json = [case_attrition_json, control_attrition_json]

            with open(options.output_combined_json, 'wt') as o:
                json.dump(dichotomous_attrition_json, o, indent=2)

    @classmethod
    def _get_case_control_variable_lists_(
        cls,
        variables_list: List[
            Union[ConceptVariableObject, CustomDichotomousVariableObject]
        ],
        outcome: CustomDichotomousVariableObject,
        source_population_cohort: int,
    ) -> Tuple[List[Union[ConceptVariableObject, CustomDichotomousVariableObject]]]:
        """
        Create two new variable lists for attrition table
        calling in case-control use case.
        """
        case_variable_list = variables_list[:]
        control_variable_list = variables_list[:]
        # use the case cohort id and source cohort id to get control counts only
        control_call_cohort_ids = [outcome.cohort_ids[1], source_population_cohort]
        # use the control cohort id and source cohort id to get case counts only
        case_call_cohort_ids = [outcome.cohort_ids[0], source_population_cohort]
        new_control_dvar = CustomDichotomousVariableObject(
            variable_type="custom_dichotomous",
            cohort_ids=control_call_cohort_ids,
            provided_name="Control cohort only",
        )
        new_case_dvar = CustomDichotomousVariableObject(
            variable_type="custom_dichotomous",
            cohort_ids=case_call_cohort_ids,
            provided_name="Case cohort only",
        )
        control_variable_list.insert(0, new_control_dvar)
        case_variable_list.insert(0, new_case_dvar)
        return control_variable_list, case_variable_list

    @classmethod
    def _format_attrition_for_json(
        cls, attrition_csv: str, table_type: str
    ) -> Dict[str, Any]:
        """
        Converts a single attrition CSV into a JSON serializable object.
        """

        def format_row(row, rtype):
            hare_cols = [
                'non-Hispanic Black',
                'non-Hispanic Asian',
                'non-Hispanic White',
                'Hispanic',
            ]
            fmt_row = {
                "type": rtype,
                "name": row.get('Cohort', ''),
                "size": int(row.get('Size', 0)),
                "concept_breakdown": [],
            }
            for key in hare_cols:
                this_hare = {
                    "concept_value_name": key,
                    "persons_in_cohort_with_value": int(row.get(key, 0)),
                }
                fmt_row["concept_breakdown"].append(this_hare)
            return fmt_row

        table_types = (
            'case',
            'control',
        )
        assert (
            table_type in table_types
        ), f"Only {table_types} are supported but you provided {table_type}"

        ret = {"table_type": table_type, "rows": []}

        with open(attrition_csv, 'rt') as fh:
            reader = csv.reader(fh)
            header = next(reader)

            # First line is source
            row_dict = dict(zip(header, next(reader)))
            curr = format_row(row_dict, "cohort")
            ret["rows"].append(curr)

            # Helpers
            seen_outcome = False
            while True:
                try:
                    row_dict = dict(zip(header, next(reader)))
                    if row_dict.get('Cohort', '') in (
                        'Control cohort only',
                        'Case cohort only',
                    ):
                        continue
                    if not seen_outcome:
                        curr = format_row(row_dict, "outcome")
                        ret["rows"].append(curr)
                        seen_outcome = True
                    else:
                        curr = format_row(row_dict, "covariate")
                        ret["rows"].append(curr)
                except StopIteration:
                    break
        return ret

    @classmethod
    def __get_description__(cls) -> str:
        """
        Description of tool.
        """
        return (
            "Generates the attrition tables for a given set of variables and cohorts "
            "that are stratified by a particular breakdown concept (e.g., HARE population). "
            "Quatitative and case-control workflow will be differentiated by --outcome argument"
            "For quantitative phenotypes, only a single CSV will be generated. For case-control, "
            "two CSVs will be produced (one for case cohort and one for control cohort). "
            "A single combined JSON will be created for front-end purposes. "
            "Set the GEN3_ENVIRONMENT environment variable if the internal URL for a service "
            "utilizes an environment other than 'default'."
        )
