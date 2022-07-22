"""Communicates with cohort middleware service to generate a metadata file
that provides users with the cohort(s) selected, variables selected, and other
workflow parameters.

@author: Kyle Hernandez <kmhernan@uchicago.edu>
"""
import dataclasses
import json
from argparse import ArgumentParser, Namespace
from typing import Any, Dict, List, Optional, Tuple, Union

import yaml

from vadc_gwas_tools.common.cohort_middleware import (
    CohortDefinitionResponse,
    CohortServiceClient,
    ConceptDescriptionResponse,
    ConceptVariableObject,
    CustomDichotomousVariableObject,
)
from vadc_gwas_tools.common.logger import Logger
from vadc_gwas_tools.subcommands import Subcommand


class GetGwasMetadata(Subcommand):
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
            "--outcome_concept_id",
            required=False,
            type=str,
            default=None,
            help=(
                "Concept ID for continuous outcome phenotype. "
                "Not required for case-control studies."
            ),
        )
        parser.add_argument(
            "--n_pcs",
            required=True,
            type=int,
            help="Number of population PCs used in workflow.",
        )
        parser.add_argument(
            "--maf_threshold",
            required=True,
            type=float,
            help="MAF threshold used for filtering markers.",
        )
        parser.add_argument(
            "--imputation_score_cutoff",
            required=True,
            type=float,
            help="Imputation score cutoff used to filter markers.",
        )
        parser.add_argument(
            "--hare_population",
            required=True,
            type=str,
            help="Selected HARE population for the GWAS analysis.",
        )
        parser.add_argument(
            "-o",
            "--output",
            required=True,
            type=str,
            help="Path to write out the final metadata output.",
        )

    @classmethod
    def main(cls, options: Namespace) -> None:
        """
        Entrypoint for GetGwasMetadata
        """
        logger = Logger.get_logger(cls.__tool_name__())
        logger.info(cls.__get_description__())

        is_case_control = False
        outcome_concept_id = None

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
            assert (
                options.outcome_concept_id is not None
            ), "You must provide --outcome_concept_id for continuous phenotypes."

            outcome_concept_id = CohortServiceClient.strip_concept_prefix(
                options.outcome_concept_id
            )[0]

        # Load variables
        with open(options.variables_json, 'rt') as fh:
            variables = json.load(
                fh, object_hook=CohortServiceClient.decode_concept_variable_json
            )

        # Check concepts
        concept_variables, custom_dichotomous_variables = cls._get_variable_lists(
            variables, outcome_concept_id
        )

        # Client
        client = CohortServiceClient()

        # Get cohort defs
        case_cohort_def = client.get_cohort_definition(options.case_cohort_id)
        control_cohort_def = (
            client.get_cohort_definition(options.control_cohort_id)
            if is_case_control
            else None
        )

        # Get concept variable data
        concept_data = client.get_concept_descriptions(
            options.source_id, [i.concept_id for i in concept_variables]
        )

        # Get custom dichotomous variable cohorts and metadata
        custom_dichotomous_cohort_metadata = (
            cls._get_custom_dichotomous_cohort_metadata(
                custom_dichotomous_variables, client
            )
        )

        # Format all metadata
        formatted_metadata = cls._format_metadata(
            case_cohort_def,
            control_cohort_def,
            concept_data,
            custom_dichotomous_variables,
            custom_dichotomous_cohort_metadata,
            outcome_concept_id,
            options,
        )

        # Export metadata
        with open(options.output, 'w') as o:
            yaml.dump(formatted_metadata, o, default_flow_style=False)

    @classmethod
    def _get_variable_lists(
        cls,
        variable_objects: List[
            Union[ConceptVariableObject, CustomDichotomousVariableObject]
        ],
        outcome_concept_id: Optional[int],
    ) -> Tuple[List[ConceptVariableObject], List[CustomDichotomousVariableObject]]:
        """
        Makes sure the outcome concept is part of the variable list.
        Adds it if necessary. Also separates the `ConceptVariableObject` from
        the `CustomDichotomousVariableObject`.
        """
        outcome_seen = False
        concept_variables = []
        custom_dichotomous_variables = []
        for variable in variable_objects:
            if isinstance(variable, ConceptVariableObject):
                concept_variables.append(variable)
                if (
                    outcome_concept_id is not None
                    and outcome_concept_id == variable.concept_id
                ):
                    outcome_seen = True
            elif isinstance(variable, CustomDichotomousVariableObject):
                custom_dichotomous_variables.append(variable)
        if outcome_concept_id is not None and not outcome_seen:
            outcome_variable = ConceptVariableObject(
                variable_type="concept", concept_id=outcome_concept_id
            )
            concept_variables.append(outcome_variable)
        return concept_variables, custom_dichotomous_variables

    @classmethod
    def _get_custom_dichotomous_cohort_metadata(
        cls,
        variables: List[CustomDichotomousVariableObject],
        client: CohortServiceClient,
    ) -> Dict[int, CohortDefinitionResponse]:
        """
        Gets the unique set of cohorts from all custom dichotomous variables and
        uses the cohort middleware to get the metadata associated with the cohorts.
        """
        cohort_ids = []
        for variable in variables:
            cohort_ids.extend(variable.cohort_ids)
        cohort_ids = list(set(cohort_ids))

        cohort_meta = {}
        for cohort in cohort_ids:
            res = client.get_cohort_definition(cohort)
            cohort_meta[cohort] = res
        return cohort_meta

    @classmethod
    def _format_metadata(
        cls,
        case_cohort_def: CohortDefinitionResponse,
        control_cohort_def: Optional[CohortDefinitionResponse],
        concept_data: List[ConceptDescriptionResponse],
        custom_dichotomous_variables: List[CustomDichotomousVariableObject],
        custom_dichotomous_cohort_metadata: Dict[int, CohortDefinitionResponse],
        outcome_concept_id: Optional[int],
        options: Namespace,
    ) -> Dict[str, Union[List[Dict[str, str]], Dict[str, Any]]]:
        """
        Combines all the different metadata from cohorts and concepts
        into a single object. Handles separating out covariates from phenotypes.
        """
        # Cohort section
        cohorts = {
            "case_cohort": dataclasses.asdict(case_cohort_def),
            "control_cohort": None
            if control_cohort_def is None
            else dataclasses.asdict(control_cohort_def),
        }

        # Clinical variable section (also separate phenotype from covariates)
        phenotype = {}
        covariates = []
        if outcome_concept_id is None:
            phenotype = {"concept_id": None, "concept_name": "CASE-CONTROL"}
            for record in concept_data:
                covariates.append(dataclasses.asdict(record))
        else:
            # split outcome
            for record in concept_data:
                if record.concept_id == outcome_concept_id:
                    phenotype = dataclasses.asdict(record)
                else:
                    covariates.append(dataclasses.asdict(record))

        # Add custom dichotomous
        for variable in custom_dichotomous_variables:
            record = []
            for n, cohort in enumerate(variable.cohort_ids):
                cohort = dataclasses.asdict(
                    custom_dichotomous_cohort_metadata[variable.cohort_ids[n]]
                )
                cohort["value"] = n
                record.append(cohort)
            cd_dict = {"custom_dichotomous": {"cohorts": record}}
            covariates.append(cd_dict)

        # add other runtime parameters
        parameters = {
            "n_population_pcs": options.n_pcs,
            "maf_threshold": options.maf_threshold,
            "imputation_score_cutoff": options.imputation_score_cutoff,
            "hare_population": options.hare_population,
        }

        # Put it all together and return dict
        data = {
            "cohorts": cohorts,
            "phenotype": phenotype,
            "covariates": covariates,
            "parameters": parameters,
        }

        return data

    @classmethod
    def __get_description__(cls) -> str:
        """
        Description of tool.
        """
        return (
            "Generates a metadata file based on the GWAS variables, cohorts, and "
            "parameters used in the workflow. For continuous variables only --case_cohort_id is "
            "necessary along with the --outcome_concept_id. For case-control, add both "
            "--case_cohort_id and --control_cohort_id. Set the GEN3_ENVIRONMENT "
            "environment variable if the internal URL for a service utilizes an environment "
            "other than 'default'."
        )
