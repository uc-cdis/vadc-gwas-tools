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
from typing import List

from vadc_gwas_tools.common.cohort_middleware import CohortServiceClient
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
            "--case_cohort_id",
            required=True,
            type=int,
            help=(
                "The cohort ID for 'cases'. For continuous phenotypes, this is the "
                "only cohort ID needed. For case-control phenotypes, these samples "
                "will be considered '1'."
            ),
        )
        parser.add_argument(
            "--control_cohort_id",
            required=False,
            type=int,
            default=None,
            help=(
                "The cohort ID for 'controls'. Only relevant for case-control phenotypes. "
                "These samples will be considered '0'."
            ),
        )
        parser.add_argument(
            "--prefixed_concept_ids",
            required=True,
            nargs="+",
            help="Prefixed concept IDs",
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

        is_case_control = False

        if options.control_cohort_id is not None:
            assert (
                options.case_cohort_id != options.control_cohort_id
            ), f"Case cohort ID can't be the same as the Control cohort ID: {options.case_cohort_id} {options.control_cohort_id}"
            is_case_control = True

            logger.info("Case-Control Design...")
            logger.info(
                f"Case Cohort: {options.case_cohort_id}; Control Cohort: {options.control_cohort_id}"
            )

        else:
            logger.info("Continuous phenotype Design...")
            logger.info(f"Cohort: {options.case_cohort_id}")

        # Client
        client = CohortServiceClient()

        if is_case_control:
            cls._process_case_control(
                client,
                options.source_id,
                options.case_cohort_id,
                options.control_cohort_id,
                options.prefixed_concept_ids,
                options.output,
                logger,
            )
        else:
            cls._process_continuous(
                client,
                options.source_id,
                options.case_cohort_id,
                options.prefixed_concept_ids,
                options.output,
                logger,
            )

    @classmethod
    def _process_continuous(
        cls,
        client: CohortServiceClient,
        source_id: int,
        case_cohort_id: int,
        prefixed_concept_ids: List[str],
        output_path: str,
        logger: Logger,
    ) -> None:
        """
        Main logic flow for getting the variable CSV for continuous phenotype which only has 1 cohort to call.
        """
        # Make request
        client.get_cohort_csv(
            source_id, case_cohort_id, output_path, prefixed_concept_ids
        )

    @classmethod
    def _process_case_control(
        cls,
        client: CohortServiceClient,
        source_id: int,
        case_cohort_id: int,
        control_cohort_id: int,
        prefixed_concept_ids: List[str],
        output_path: str,
        logger: Logger,
    ) -> None:
        """
        Main logic flow for getting the variable CSV for case/control phenotype which has 2 separate cohorts.
        """
        # Get cases
        (_, tmp_case_path) = tempfile.mkstemp()
        client.get_cohort_csv(
            source_id, case_cohort_id, tmp_case_path, prefixed_concept_ids
        )

        # Get controls
        (_, tmp_control_path) = tempfile.mkstemp()
        client.get_cohort_csv(
            source_id, control_cohort_id, tmp_control_path, prefixed_concept_ids
        )

        # Process and validate
        seen_ids = set()
        open_func = gzip.open if output_path.endswith('.gz') else open
        with open_func(output_path, "wt") as o:
            with open(tmp_case_path, "rt") as fh:
                reader = csv.DictReader(fh)

                # Setup header and writer
                fnames = reader.fieldnames
                fnames.append("CASE_CONTROL")
                writer = csv.DictWriter(o, fieldnames=fnames)
                writer.writeheader()

                for row in reader:
                    cid = row["sample.id"]
                    row["CASE_CONTROL"] = 1

                    if cid in seen_ids:
                        msg = "Case ID present multiple times!"
                        raise AssertionError(msg)
                    seen_ids.add(cid)

                    writer.writerow(row)

            with open(tmp_control_path, "rt") as fh:
                reader = csv.DictReader(fh)

                for row in reader:
                    cid = row["sample.id"]
                    row["CASE_CONTROL"] = 0

                    if cid in seen_ids:
                        msg = "Control ID present multiple times!"
                        raise AssertionError(msg)
                    seen_ids.add(cid)
                    writer.writerow(row)

    @classmethod
    def __get_description__(cls) -> str:
        """
        Description of tool.
        """
        return (
            "Gets the CSV file used in the GENESIS workflow for the provided cohorts "
            "and phenotype concept IDs. For continuous phenotypes, only provide --case_cohort_id."
            "For case-control, add both --case_cohort_id and --control_cohort_id. Set the GEN3ENVIRONMENT "
            "environment variable if the internal URL for a service utilizes an environment other than 'default'."
        )
