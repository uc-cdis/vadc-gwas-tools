"""Communicates with cohort middleware service to extract the attribution
breakdown CSV(s).

@author: Kyle Hernandez <kmhernan@uchicago.edu>
"""
import json
from argparse import ArgumentParser, Namespace

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
            help="JSON formatted string of outcome variable."
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
            "--output_prefix",
            required=True,
            type=str,
            help="Prefix to use for outputs (1 csv for quantitative, 2 csvs for case-control).",
        )

    @classmethod
    def main(cls, options: Namespace) -> None:
        """
        Entrypoint for GetCohortAttritionTable
        """
        logger = Logger.get_logger(cls.__tool_name__())
        logger.info(cls.__get_description__())

        is_case_control = False

        outcome_json = json.loads(options.outcome)
        if outcome_json['variable_type'] == "concept":
            outcome_val = ConceptVariableObject(**outcome_json)
        elif outcome_json['variable_type'] == "custom_dichotomous":
            outcome_val = CustomDichotomousVariableObject(**outcome_json)
            is_case_control = True
            outcome_control_cohort = outcome_val.cohort_ids[0]
            outcome_case_cohort = outcome_val.cohort_ids[1]
            outcome_case_control_provided_name = outcome_val.provided_name
        else:
            msg = {
                "Currently we only support 'concept' and 'custom_dichotomous' variable "
                "types, but you provided {}".format(outcome_json.get('variable_type'))
            }
            logger.error(msg)
            raise RuntimeError(msg)

        # Load JSON object
        with open(options.variables_json, 'rt') as fh:
            variables = json.load(
                fh, object_hook=CohortServiceClient.decode_concept_variable_json
            )
            # if case/control, add an extra filter on top of the given variables
            # to ensure that any person that belongs to _both_ cohorts
            # [options.case_cohort_id, options.control_cohort_id] also gets filtered out:
            assert outcome_val == variables[0], (
                "First element of variable list is not equal to the outcome variable object"
                f"First element of variables: {variables[0].__annotations__}"
                f"Outcome: {outcome_val.__annotations__}"
            )

        # Client
        client = CohortServiceClient()

        # Call attrition table
        if not is_case_control: # Continuous workflow
            # log info
            logger.info("Continuous Design...")
            logger.info(
                (
                    f"Source Population Cohort: {options.source_population_cohort}; "
                )
            )
            # Call cohort-middleware for continuous workflow
            continuous_csv = f"{options.output_prefix}.case_cohort.attrition_table.csv"   
            logger.info(f"Writing continuous workflow attrition table to {continuous_csv}")
            client.get_attrition_breakdown_csv(
                options.source_id,
                options.source_population_cohort,
                continuous_csv,
                variables,
                options.prefixed_breakdown_concept_id,
            )

        else:
            # logger info
            logger.info("Case-Control Design...")
            logger.info(
                (
                    f"Source Population Cohort: {options.source_population_cohort}; "
                    f"Case Cohort: {outcome_case_cohort}"
                    f"Control Cohort: {outcome_control_cohort}"
                )
            )

            # Call cohort-middleware for control cohort
            control_csv = f"{options.output_prefix}.control_cohort.attrition_table.csv"
            control_variable_list = variables[:]
            # use the case cohort id and source cohort id to get control counts only
            control_call_cohort_ids = [outcome_case_cohort, options.source_population_cohort]
            new_control_dvar = CustomDichotomousVariableObject(
                variable_type="custom_dichotomous",
                cohort_ids=control_call_cohort_ids,
                provided_name="Control cohort only"
            )
            control_variable_list.insert(1, new_control_dvar)
            logger.info(f"Writing case-control control cohort attrition table to {control_csv}")
            client.get_attrition_breakdown_csv(
                options.source_id,
                options.source_population_cohort,
                control_csv,
                control_variable_list,
                options.prefixed_breakdown_concept_id,
            )

            # Call cohort-middleware for case cohort
            case_csv = f"{options.output_prefix}.case_cohort.attrition_table.csv"
            case_variable_list = variables[:]
            # use the control cohort id and source cohort id to get case counts only
            case_call_cohort_ids = [outcome_control_cohort, options.source_population_cohort]
            new_case_dvar = CustomDichotomousVariableObject(
                variable_type="custom_dichotomous",
                cohort_ids=case_call_cohort_ids,
                provided_name="Case cohort only"
            )
            case_variable_list.insert(1, new_case_dvar)
            logger.info(f"Writing case-control case cohort attrition table to {case_csv}")
            client.get_attrition_breakdown_csv(
                options.source_id,
                options.source_population_cohort,
                case_csv,
                case_variable_list,
                options.prefixed_breakdown_concept_id,
            )

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
            "two CSVs will be produced (one for case cohort and one for control cohort)."
            "Set the GEN3_ENVIRONMENT environment variable if the internal URL for a service "
            "utilizes an environment other than 'default'."
        )
