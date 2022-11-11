"""Communicates with cohort middleware service to generate the phenotype
and covariate CSV file used in the GWAS analysis. Handles case and control
type cohorts and does some basic validation.

@author: Kyle Hernandez <kmhernan@uchicago.edu>
"""
import csv
import gzip
import json
import os
import tempfile
from argparse import ArgumentParser, Namespace
from typing import List, Set, Union

from vadc_gwas_tools.common.cohort_middleware import (
    CohortServiceClient,
    ConceptVariableObject,
    CustomDichotomousVariableObject,
)
from vadc_gwas_tools.common.logger import Logger
from vadc_gwas_tools.subcommands import Subcommand


class GetCohortPheno(Subcommand):
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
            help="Integer ID of the source population cohort",
        )
        parser.add_argument(
            "--variables_json",
            required=True,
            help="Path to the JSON file containing the variable objects.",
        )
        parser.add_argument(
            "-o",
            "--output",
            required=True,
            type=str,
            help="Path to write out the final phenotype CSV. If ends with '.gz' the file will be gzipped.",
        )

    @classmethod
    def main(cls, options: Namespace) -> None:
        """
        Entrypoint for GetCohortPheno
        """
        logger = Logger.get_logger(cls.__tool_name__())
        logger.info(cls.__get_description__())

        logger.info("GWAS unified workflow design...")
        logger.info(f"Source Population Cohort: {options.source_population_cohort}")

        # Load JSON object
        with open(options.variables_json, "rt") as fh:
            variables = json.load(
                fh, object_hook=CohortServiceClient.decode_concept_variable_json
            )

        # Client
        client = CohortServiceClient()
        cls._process_variables_csv(
            client,
            options.source_id,
            options.source_population_cohort,
            variables,
            options.output,
            logger,
        )

    @classmethod
    def _process_variables_csv(
        cls,
        client: CohortServiceClient,
        source_id: int,
        source_population_cohort: int,
        variables: List[Union[ConceptVariableObject, CustomDichotomousVariableObject]],
        output_path: str,
        logger: Logger,
    ) -> None:
        """
        Main logic flow for getting the variable CSV for continuous phenotype which only has 1 cohort to call.
        """
        # Make request
        client.get_cohort_csv(
            source_id, source_population_cohort, output_path, variables
        )

    @classmethod
    def __get_description__(cls) -> str:
        """
        Description of tool.
        """
        return (
            "Gets the CSV file used in the GENESIS workflow for the provided cohorts "
            "and phenotype concept IDs. --variable is a json file that includes both "
            "concept variable and custom dichotomous variable. Set the GEN3ENVIRONMENT"
            "environment variable if the internal URL for a service utilizes an environment other than 'default'."
        )
