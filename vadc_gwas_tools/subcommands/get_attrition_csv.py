"""Communicates with cohort middleware service to extract the attribution
breakdown CSV(s).

@author: Kyle Hernandez <kmhernan@uchicago.edu>
"""
import json
from argparse import ArgumentParser, Namespace

from vadc_gwas_tools.common.cohort_middleware import CohortServiceClient
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
            "--case_cohort_id",
            required=True,
            type=int,
            help=(
                "The cohort ID for 'cases'. For continuous phenotypes, this is the "
                "only cohort ID needed."
            ),
        )
        parser.add_argument(
            "--control_cohort_id",
            required=False,
            type=int,
            default=None,
            help=(
                "The cohort ID for 'controls'. Only relevant for case-control phenotypes."
            ),
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

        if options.control_cohort_id is not None:
            assert options.case_cohort_id != options.control_cohort_id, (
                "Case cohort ID can't be the same as the Control cohort ID: "
                f"{options.case_cohort_id} {options.control_cohort_id}"
            )
            is_case_control = True

            logger.info("Case-Control Design...")
            logger.info(
                (
                    f"Case Cohort: {options.case_cohort_id}; "
                    f"Control Cohort: {options.control_cohort_id}"
                )
            )

        else:
            logger.info("Continuous phenotype Design...")
            logger.info(f"Cohort: {options.case_cohort_id}")

        # Load JSON object
        with open(options.variables_json, 'rt') as fh:
            variables = json.load(
                fh, object_hook=CohortServiceClient.decode_concept_variable_json
            )

        # Client
        client = CohortServiceClient()

        # Case cohort
        case_csv = f"{options.output_prefix}.case_cohort.attrition_table.csv"
        logger.info(f"Writing case cohort attrition table to {case_csv}")
        client.get_attrition_breakdown_csv(
            options.source_id,
            options.case_cohort_id,
            case_csv,
            variables,
            options.prefixed_breakdown_concept_id,
        )

        # Control cohort
        if is_case_control:
            control_csv = f"{options.output_prefix}.control_cohort.attrition_table.csv"
            logger.info(f"Writing control cohort attrition table to {control_csv}")
            client.get_attrition_breakdown_csv(
                options.source_id,
                options.control_cohort_id,
                control_csv,
                variables,
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
            "For quantitative phenotypes, use --case_cohort_id and only a single CSV will be "
            "generated. For case-control, add both --case_cohort_id and --control_cohort_id; "
            "two CSVs will be produced (one for case cohort and one for control cohort). "
            "Set the GEN3_ENVIRONMENT environment variable if the internal URL for a service "
            "utilizes an environment other than 'default'."
        )
