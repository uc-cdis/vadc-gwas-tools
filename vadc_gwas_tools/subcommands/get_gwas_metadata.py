"""Communicates with cohort middleware service to generate a metadata file
that provides users with the cohort(s) selected, variables selected, and other
workflow parameters.

@author: Kyle Hernandez <kmhernan@uchicago.edu>
"""
import dataclasses
from argparse import ArgumentParser, Namespace
from typing import Any, Dict, List, Optional, Union

import yaml

from vadc_gwas_tools.common.cohort_middleware import (
    CohortDefinitionResponse,
    CohortServiceClient,
    ConceptDescriptionResponse,
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
            "--prefixed_concept_ids",
            required=True,
            nargs="+",
            help="Prefixed concept IDs",
        )
        parser.add_argument(
            "--prefixed_outcome_concept_id",
            required=False,
            type=str,
            default=None,
            help=(
                "Prefixed concept ID for continuous outcome phenotype. "
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
        outcome_pfx = None

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
                options.prefixed_outcome_concept_id is not None
            ), "You must provide --prefixed_outcome_concept_id for continuous phenotypes."

            outcome_pfx = options.prefixed_outcome_concept_id

        # Check concepts
        pfx_concept_ids = cls._get_concept_list(
            options.prefixed_concept_ids, options.prefixed_outcome_concept_id
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

        # Get variable data
        concept_data = client.get_concept_description(
            options.source_id, pfx_concept_ids
        )

        # Format all metadata
        formatted_metadata = cls._format_metadata(
            case_cohort_def,
            control_cohort_def,
            concept_data,
            options.prefixed_outcome_concept_id,
            options,
        )

        # Export metadata
        with open(options.output, 'w') as o:
            yaml.dump(formatted_metadata, o, default_flow_style=False)

    @classmethod
    def _get_concept_list(
        cls, prefixed_concept_ids: List[str], prefixed_outcome_concept_id: Optional[str]
    ) -> List[str]:
        """
        Makes sure the outcome concept is part of the concept id list.
        Adds it if necessary.
        """
        if prefixed_outcome_concept_id is None:
            return prefixed_concept_ids
        if prefixed_outcome_concept_id not in prefixed_concept_ids:
            return prefixed_concept_ids + [prefixed_outcome_concept_id]
        return prefixed_concept_ids

    @classmethod
    def _format_metadata(
        cls,
        case_cohort_def: CohortDefinitionResponse,
        control_cohort_def: Optional[CohortDefinitionResponse],
        concept_data: List[ConceptDescriptionResponse],
        prefixed_outcome_concept_id: Optional[int],
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
        if prefixed_outcome_concept_id is None:
            phenotype = {"concept_id": None, "concept_name": "CASE-CONTROL"}
            for record in concept_data:
                covariates.append(dataclasses.asdict(record))
        else:
            # split outcome
            for record in concept_data:
                if record.prefixed_concept_id == prefixed_outcome_concept_id:
                    phenotype = dataclasses.asdict(record)
                else:
                    covariates.append(dataclasses.asdict(record))

        # add other runtime parameters
        parameters = {
            "n_population_pcs": options.n_pcs,
            "maf_threshold": options.maf_threshold,
            "imputation_score_cutoff": options.imputation_score_cutoff,
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
            "necessary along with the --outcome_prefixed_concept_id. For case-control, add both "
            "--case_cohort_id and --control_cohort_id. Set the GEN3_ENVIRONMENT "
            "environment variable if the internal URL for a service utilizes an environment "
            "other than 'default'."
        )
