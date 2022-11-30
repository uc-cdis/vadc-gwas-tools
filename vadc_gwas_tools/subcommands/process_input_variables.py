"""Processes the user-provided input variables.

* Inserts the HARE concept
* Validates objects
* Extracts space-delimited string of covariates for GENESIS

@author: Kyle Hernandez <kmhernan@uchicago.edu>
"""
import json
import os
from argparse import ArgumentParser, Namespace
from dataclasses import asdict
from typing import List, Union

from vadc_gwas_tools.common.cohort_middleware import (
    CohortServiceClient,
    ConceptVariableObject,
    CustomDichotomousVariableObject,
)
from vadc_gwas_tools.common.logger import Logger
from vadc_gwas_tools.subcommands import Subcommand


class ProcessInputVariables(Subcommand):
    @classmethod
    def __add_arguments__(cls, parser: ArgumentParser) -> None:
        """Add the subcommand params"""
        parser.add_argument(
            "--raw_variables_json",
            required=True,
            type=str,
            help="The variable list provided by user as a JSON file.",
        )
        parser.add_argument(
            "--hare_concept_id", required=True, type=int, help="The HARE concept ID."
        )
        parser.add_argument(
            "--outcome",
            required=True,
            type=str,
            help="JSON formatted string of the outcome variable.",
        )
        parser.add_argument(
            "--output_raw_variable_json",
            required=True,
            type=str,
            help="Output path to a validated raw variable json"
        )
        parser.add_argument(
            "--output_variable_json_w_hare",
            required=True,
            type=str,
            help="Output path to use for updated variable JSON with hare population concept id.",
        )
        parser.add_argument(
            "--output_other_json",
            required=True,
            type=str,
            help="Output path to use for the covariate string, outcome name, and outcome type JSON.",
        )

    @classmethod
    def main(cls, options: Namespace) -> None:
        """
        Entrypoint for ProcessInputVariables
        """
        logger = Logger.get_logger(cls.__tool_name__())
        logger.info(cls.__get_description__())

        # Load JSON variables object
        with open(options.raw_variables_json, 'rt') as fh:
            variables = json.load(
                fh, object_hook=CohortServiceClient.decode_concept_variable_json
            )

        # Load outcome variable object
        outcome = json.loads(
            options.outcome,
            object_hook=CohortServiceClient.decode_concept_variable_json,
        )

        # Determine type
        outcome_type = (
            "BINARY" if outcome.variable_type == "custom_dichotomous" else "CONTINUOUS"
        )
        logger.info(f"Outcome type is {outcome_type}")

        # Validate if outcome in variables
        assert (
            outcome in variables
        ), f"Outcome {outcome} is not found in variables list."

        # Make the outocme as the first item in variables if it's not the case
        if outcome != variables[0]:
            variables.insert(0, variables.pop(variables.index(outcome)))
        else:
            pass

        # Create validated variables
        output_raw_variables = [asdict(i) for i in variables]
        with open(options.output_raw_variable_json, 'wt') as o:
            json.dump(output_raw_variables, o)

        # Create variables with HARE
        hare_concept = ConceptVariableObject(
            variable_type="concept",
            concept_id=options.hare_concept_id,
        )
        output_with_hare = [asdict(i) for i in variables + [hare_concept]]
        with open(options.output_variable_json_w_hare, 'wt') as o:
            json.dump(output_with_hare, o)

        # Make covariate list
        covariates = []
        outcome_key = None
        for variable in variables:
            if variable == outcome:
                if variable.variable_type == "custom_dichotomous":
                    outcome_key = (
                        f"ID_{variable.cohort_ids[0]}_{variable.cohort_ids[1]}"
                    )
                else:
                    outcome_key = f"ID_{variable.concept_id}"
            elif variable.variable_type == "custom_dichotomous":
                key = f"ID_{variable.cohort_ids[0]}_{variable.cohort_ids[1]}"
                covariates.append(key)
            else:
                key = f"ID_{variable.concept_id}"
                covariates.append(key)
        # Make other json
        other_json = {
            "covariates": " ".join(covariates),
            "outcome": outcome_key,
            "outcome_type": outcome_type,
        }
        with open(options.output_other_json, 'wt') as o:
            json.dump(other_json, o)

    @classmethod
    def __get_description__(cls) -> str:
        """
        Description of tool.
        """
        return (
            "Processes and validates user-provided input variables "
            "for use in downstream tools."
        )
