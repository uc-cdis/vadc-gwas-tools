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
    SchemaVersionResponse,
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
            "--source_population_cohort",
            required=True,
            type=int,
            help=(
                "The cohort ID for source population. This is required for both "
                "case-control and quantitative workflow."
            ),
        )
        parser.add_argument(
            "--variables_json",
            required=True,
            help="Path to the JSON file containing the variable objects.",
        )
        parser.add_argument(
            "--outcome",
            required=True,
            type=str,
            default=None,
            help=(
                "JSON formatted string of outcome variable."
                "Required for both case-control and quantitative workflow."
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
            "--pvalue_cutoff",
            default=5e-8,
            type=float,
            help="P-value cutoff to use for extracting 'significant' hits. [5e-8]",
        )
        parser.add_argument(
            "--top_n_hits",
            default=100,
            type=int,
            help="Number of top hits to extract, regardless of P-value. [100]",
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

        outcome = json.loads(
            options.outcome,
            object_hook=CohortServiceClient.decode_concept_variable_json,
        )

        # Decide the category of workflow
        if isinstance(outcome, CustomDichotomousVariableObject):
            is_case_control = True
            outcome_control_cohort, outcome_case_cohort = outcome.cohort_ids
            outcome_case_control_provided_name = outcome.provided_name
            logger.info("Case-Control Design...")
            logger.info(
                (
                    f"Source Cohort: {options.source_population_cohort} "
                    f"Case Cohort: {outcome_case_cohort}; "
                    f"Control Cohort: {outcome_control_cohort}"
                )
            )
        else:
            logger.info("Continuous Design...")
            logger.info((f"Source Cohort: {options.source_population_cohort} "))

        # Load variables
        with open(options.variables_json, "rt") as fh:
            variables = json.load(
                fh, object_hook=CohortServiceClient.decode_concept_variable_json
            )

        # Check concepts
        # Only covariates are included in the vairable lists
        concept_variables, custom_dichotomous_variables = cls._get_variable_lists(
            variables, outcome
        )

        # Client
        client = CohortServiceClient()

        # Get Atlas and CDM/OMOP DB versions
        logger.info("Fetching Atlas and CDM/OMOP DB versions...")
        schema_versions = client.get_schema_versions()

        # Get source population cohort defs
        logger.info("Fetching source population cohort definition...")
        source_cohort_def = client.get_cohort_definition(
            options.source_population_cohort
        )

        # Get cohort def in case-control workflow case
        # Get outcome concept def in continuous workflow case
        if is_case_control:
            logger.info("Fetching case and control cohort definitions...")
            case_cohort_def = client.get_cohort_definition(outcome.cohort_ids[1])
            control_cohort_def = client.get_cohort_definition(outcome.cohort_ids[0])
        else:
            logger.info("Fetching continuous outcome definition...")
            outcome_data_request = client.get_concept_descriptions(
                options.source_id, [outcome.concept_id]
            )
            outcome_data = outcome_data_request[0]

        # Get concept variable data
        logger.info("Fetching covariates metadata...")
        if concept_variables:
            concept_data = client.get_concept_descriptions(
                options.source_id, [i.concept_id for i in concept_variables]
            )
        else:
            concept_data = []

        # Get custom dichotomous variable cohorts and metadata
        custom_dichotomous_cohort_metadata = (
            cls._get_custom_dichotomous_cohort_metadata(
                custom_dichotomous_variables, client
            )
        )

        # Format all metadata
        logger.info("Formatting GWAS metadata...")
        if is_case_control:
            formatted_metadata = cls._format_metadata(
                options=options,
                schema_versions=schema_versions,
                source_cohort_def=source_cohort_def,
                outcome=outcome,
                concept_data=concept_data,
                custom_dichotomous_variables=custom_dichotomous_variables,
                custom_dichotomous_cohort_metadata=custom_dichotomous_cohort_metadata,
                case_cohort_def=case_cohort_def,
                control_cohort_def=control_cohort_def
            )
        else:
            formatted_metadata = cls._format_metadata(
                options=options,
                schema_versions=schema_versions,
                source_cohort_def=source_cohort_def,
                outcome=outcome,
                concept_data=concept_data,
                custom_dichotomous_variables=custom_dichotomous_variables,
                custom_dichotomous_cohort_metadata=custom_dichotomous_cohort_metadata,
                outcome_data=outcome_data
            )

        # Export metadata
        logger.info("Writing GWAS metadata...")
        logger.info((f"Output: {options.output} "))
        with open(options.output, "w") as o:
            yaml.dump(formatted_metadata, o, default_flow_style=False, sort_keys=False)

    @classmethod
    def _get_variable_lists(
        cls,
        variable_objects: List[
            Union[ConceptVariableObject, CustomDichotomousVariableObject]
        ],
        outcome: Union[ConceptVariableObject, CustomDichotomousVariableObject],
    ) -> Tuple[List[ConceptVariableObject], List[CustomDichotomousVariableObject]]:
        """
        Makes sure the outcome concept is the first element
        of the variable list. Also separates the `ConceptVariableObject`
        from the `CustomDichotomousVariableObject`. Outputs only include
        covariates, excluding outcome
        """
        # Check if the first item in variable_objects equals to outcome
        assert outcome == variable_objects[0], (
            "First element of variable list is not equal to the outcome\n"
            f"First element of variables: {variable_objects[0]}\n"
            f"Outcome: {outcome}"
        )
        concept_variables = []
        custom_dichotomous_variables = []
        for variable in variable_objects[1:]:  # skip the first item, aka, outcome
            if isinstance(variable, ConceptVariableObject):
                # check if the concept variable is the hare population
                if variable.concept_id != 2000007027:
                    concept_variables.append(variable)
                else:
                    pass
            elif isinstance(variable, CustomDichotomousVariableObject):
                custom_dichotomous_variables.append(variable)
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
        options: Namespace,
        schema_versions: SchemaVersionResponse,
        source_cohort_def: CohortDefinitionResponse,
        # outcome provides outcome.provded_name in case-control case
        outcome: Union[CustomDichotomousVariableObject, ConceptVariableObject],
        concept_data: List[ConceptDescriptionResponse],
        custom_dichotomous_variables: List[CustomDichotomousVariableObject],
        custom_dichotomous_cohort_metadata: Dict[int, CohortDefinitionResponse],
        # case_cohort_def and control_cohort_def needed in case-control case
        case_cohort_def: Optional[CohortDefinitionResponse] = None,
        control_cohort_def: Optional[CohortDefinitionResponse] = None,
        # outcome_data provides concept metadata in continuous case
        outcome_data: Optional[ConceptDescriptionResponse] = None,
    ) -> Dict[str, Union[List[Dict[str, str]], Dict[str, Any]]]:
        # database schema version
        schema_versions = dataclasses.asdict(schema_versions)

        # source cohort section
        source_cohort = dataclasses.asdict(source_cohort_def)

        # gwas runtime paramters section
        parameters = {
            "n_population_pcs": options.n_pcs,
            "maf_threshold": options.maf_threshold,
            "imputation_score_cutoff": options.imputation_score_cutoff,
            "hare_population": options.hare_population,
            "pvalue_cutoff": options.pvalue_cutoff,
            "top_n_hits": options.top_n_hits
        }

        # Outcome section
        if isinstance(outcome, ConceptVariableObject):  # continuous workflow
            outcome_section = dataclasses.asdict(outcome_data)
            # Insert the workflow type as the first element
            outcome_section_items = list(outcome_section.items())
            outcome_section_items.insert(0, ("type", "CONTINUOUS"))
            outcome_section = dict(outcome_section_items)
        else:  # case-control workflow 
            outcome_section = {
                "type": "CASE-CONTROL",
                "concept_name": outcome.provided_name,
                "concept_cohorts" : {
                    "case_cohort": dataclasses.asdict(case_cohort_def),
                    "control_cohort": dataclasses.asdict(control_cohort_def)
                }
            }

        # Clinical covariables section
        covariates = []
        for record in concept_data:
            covariates.append(dataclasses.asdict(record))

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
        
        # Put it all together and return dict
        data = {
            "source_cohort": source_cohort,
            "covariates": covariates,
            "parameters": parameters,
            "outcome": outcome_section,
            "schema_versions": schema_versions
        }
        return data

    @classmethod
    def __get_description__(cls) -> str:
        """
        Description of tool.
        """
        return (
            "Generates a metadata file based on the GWAS variables, cohorts,"
            "and parameters used in the workflow. --outcome, a json formatted"
            "str, and --source_population_cohort are required for both "
            "contunuous and case-control workflows. Set the GEN3_ENVIRONMENT"
            "environment variable if the internal URL for a service utilizes"
            "an environment other than 'default'."
        )
