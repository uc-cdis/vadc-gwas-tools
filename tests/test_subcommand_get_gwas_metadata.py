"""Tests for the ``vadc_gwas_tools.subcommands.GetGwasMetadata`` subcommand"""
import dataclasses
import json
import tempfile
import unittest
from typing import List, NamedTuple, Optional
from unittest import mock

import yaml
from utils import captured_output, cleanup_files

from vadc_gwas_tools.common.cohort_middleware import (
    CohortDefinitionResponse,
    ConceptDescriptionResponse,
)
from vadc_gwas_tools.subcommands import GetGwasMetadata as MOD


class MockArgs(NamedTuple):
    source_id: int
    case_cohort_id: int
    control_cohort_id: Optional[int]
    prefixed_concept_ids: List[str]
    prefixed_outcome_concept_id: Optional[str]
    n_pcs: int
    maf_threshold: float
    imputation_score_cutoff: float
    hare_population: str
    output: str


class TestGetGwasMetadataSubcommand(unittest.TestCase):
    def make_cohort_def(self, cohort_definition_id, cohort_name, cohort_description):
        return CohortDefinitionResponse(
            cohort_definition_id=cohort_definition_id,
            cohort_name=cohort_name,
            cohort_description=cohort_description,
        )

    def make_concept_def(
        self, concept_id, prefixed_concept_id, concept_name, domain_id, domain_name
    ):
        return ConceptDescriptionResponse(
            concept_id=concept_id,
            prefixed_concept_id=prefixed_concept_id,
            concept_name=concept_name,
            domain_id=domain_id,
            domain_name=domain_name,
        )

    def test_get_concept_list(self):
        pfx_concepts = ["ID_1", "ID_2"]
        out_pfx_concept = "ID_2"
        res = MOD._get_concept_list(pfx_concepts, out_pfx_concept)
        self.assertEqual(res, pfx_concepts)

        pfx_concepts = ["ID_1", "ID_2"]
        out_pfx_concept = "ID_3"
        res = MOD._get_concept_list(pfx_concepts, out_pfx_concept)
        self.assertEqual(res, pfx_concepts + ["ID_3"])

        pfx_concepts = ["ID_1", "ID_2"]
        out_pfx_concept = None
        res = MOD._get_concept_list(pfx_concepts, out_pfx_concept)
        self.assertEqual(res, pfx_concepts)

    def test_format_metadata_continuous(self):
        args = MockArgs(
            source_id=2,
            case_cohort_id=9,
            control_cohort_id=None,
            prefixed_concept_ids=["ID_1", "ID_2", "ID_3"],
            prefixed_outcome_concept_id="ID_3",
            n_pcs=3,
            maf_threshold=0.01,
            imputation_score_cutoff=0.3,
            hare_population="Hispanic",
            output="/some/path",
        )

        case_cohort_def = self.make_cohort_def(args.case_cohort_id, "CASE", "Fake")
        control_cohort_def = None
        concept_defs = [
            self.make_concept_def(1, "ID_1", "Var A", "Obsevation", "Observation"),
            self.make_concept_def(2, "ID_2", "Var B", "Obsevation", "Observation"),
            self.make_concept_def(3, "ID_3", "Var C", "Obsevation", "Observation"),
        ]

        expected = {
            "cohorts": {
                "case_cohort": dataclasses.asdict(case_cohort_def),
                "control_cohort": None,
            },
            "phenotype": dataclasses.asdict(concept_defs[-1]),
            "covariates": [dataclasses.asdict(i) for i in concept_defs[:-1]],
            "parameters": {
                "n_population_pcs": args.n_pcs,
                "maf_threshold": args.maf_threshold,
                "imputation_score_cutoff": args.imputation_score_cutoff,
                "hare_population": args.hare_population,
            },
        }

        res = MOD._format_metadata(
            case_cohort_def,
            control_cohort_def,
            concept_defs,
            args.prefixed_outcome_concept_id,
            args,
        )
        self.assertEqual(
            json.dumps(res, sort_keys=True), json.dumps(expected, sort_keys=True)
        )

    def test_format_metadata_case_control(self):
        args = MockArgs(
            source_id=2,
            case_cohort_id=9,
            control_cohort_id=10,
            prefixed_concept_ids=["ID_1", "ID_2", "ID_3"],
            prefixed_outcome_concept_id=None,
            n_pcs=3,
            maf_threshold=0.01,
            imputation_score_cutoff=0.3,
            hare_population="Hispanic",
            output="/some/path",
        )

        case_cohort_def = self.make_cohort_def(args.case_cohort_id, "CASE", "Fake")
        control_cohort_def = self.make_cohort_def(
            args.control_cohort_id, "CONTROL", "Fake"
        )
        concept_defs = [
            self.make_concept_def(1, "ID_1", "Var A", "Obsevation", "Observation"),
            self.make_concept_def(2, "ID_2", "Var B", "Obsevation", "Observation"),
            self.make_concept_def(3, "ID_3", "Var C", "Obsevation", "Observation"),
        ]

        expected = {
            "cohorts": {
                "case_cohort": dataclasses.asdict(case_cohort_def),
                "control_cohort": dataclasses.asdict(control_cohort_def),
            },
            "phenotype": {"concept_id": None, "concept_name": "CASE-CONTROL"},
            "covariates": [dataclasses.asdict(i) for i in concept_defs],
            "parameters": {
                "n_population_pcs": args.n_pcs,
                "maf_threshold": args.maf_threshold,
                "imputation_score_cutoff": args.imputation_score_cutoff,
                "hare_population": args.hare_population,
            },
        }

        res = MOD._format_metadata(
            case_cohort_def,
            control_cohort_def,
            concept_defs,
            args.prefixed_outcome_concept_id,
            args,
        )
        self.assertEqual(
            json.dumps(res, sort_keys=True), json.dumps(expected, sort_keys=True)
        )

    def test_main_continuous_no_outcome_concept(self):
        args = MockArgs(
            source_id=2,
            case_cohort_id=9,
            control_cohort_id=None,
            prefixed_concept_ids=["ID_1", "ID_2"],
            prefixed_outcome_concept_id=None,
            n_pcs=3,
            maf_threshold=0.01,
            imputation_score_cutoff=0.3,
            hare_population="Hispanic",
            output="/some/path",
        )

        with captured_output() as (_, _):
            with self.assertRaises(AssertionError) as e:
                MOD.main(args)

    def test_main_continuous_params(self):
        (_, outpath) = tempfile.mkstemp(suffix='.yaml')
        args = MockArgs(
            source_id=2,
            case_cohort_id=9,
            control_cohort_id=None,
            prefixed_concept_ids=["ID_1", "ID_2"],
            prefixed_outcome_concept_id="ID_2",
            n_pcs=3,
            maf_threshold=0.01,
            imputation_score_cutoff=0.3,
            hare_population="Hispanic",
            output=outpath,
        )

        case_cohort_def = self.make_cohort_def(args.case_cohort_id, "CASE", "Fake")
        control_cohort_def = None
        concept_defs = [
            self.make_concept_def(1, "ID_1", "Var A", "Obsevation", "Observation"),
            self.make_concept_def(2, "ID_2", "Var B", "Obsevation", "Observation"),
        ]

        expected = {
            "cohorts": {
                "case_cohort": dataclasses.asdict(case_cohort_def),
                "control_cohort": None,
            },
            "phenotype": dataclasses.asdict(concept_defs[-1]),
            "covariates": [dataclasses.asdict(i) for i in concept_defs[:-1]],
            "parameters": {
                "n_population_pcs": args.n_pcs,
                "maf_threshold": args.maf_threshold,
                "imputation_score_cutoff": args.imputation_score_cutoff,
                "hare_population": args.hare_population,
            },
        }

        try:
            with mock.patch(
                "vadc_gwas_tools.subcommands.get_gwas_metadata.CohortServiceClient"
            ) as mock_client:
                instance = mock_client.return_value
                instance.get_cohort_definition.return_value = case_cohort_def
                instance.get_concept_descriptions.return_value = concept_defs

                MOD._format_metadata = mock.MagicMock(return_value=expected)
                MOD.main(args)
                instance.get_cohort_definition.assert_called_once()
                instance.get_cohort_definition.assert_called_with(args.case_cohort_id)

                instance.get_concept_descriptions.assert_called_once()
                instance.get_concept_descriptions.assert_called_with(
                    args.source_id, args.prefixed_concept_ids
                )

                MOD._format_metadata.assert_called_with(
                    case_cohort_def,
                    control_cohort_def,
                    concept_defs,
                    args.prefixed_outcome_concept_id,
                    args,
                )

            with open(outpath, 'r') as fh:
                res = yaml.safe_load(fh)

            self.assertEqual(
                json.dumps(res, sort_keys=True), json.dumps(expected, sort_keys=True)
            )
        finally:
            cleanup_files(outpath)

    def test_main_case_control_params(self):
        (_, outpath) = tempfile.mkstemp(suffix='.yaml')
        args = MockArgs(
            source_id=2,
            case_cohort_id=9,
            control_cohort_id=10,
            prefixed_concept_ids=["ID_1", "ID_2", "ID_3"],
            prefixed_outcome_concept_id=None,
            n_pcs=3,
            maf_threshold=0.01,
            imputation_score_cutoff=0.3,
            hare_population="Hispanic",
            output=outpath,
        )

        case_cohort_def = self.make_cohort_def(args.case_cohort_id, "CASE", "Fake")
        control_cohort_def = self.make_cohort_def(
            args.control_cohort_id, "CONTROL", "Fake"
        )
        concept_defs = [
            self.make_concept_def(1, "ID_1", "Var A", "Obsevation", "Observation"),
            self.make_concept_def(2, "ID_2", "Var B", "Obsevation", "Observation"),
            self.make_concept_def(3, "ID_3", "Var C", "Obsevation", "Observation"),
        ]

        expected = {
            "cohorts": {
                "case_cohort": dataclasses.asdict(case_cohort_def),
                "control_cohort": dataclasses.asdict(control_cohort_def),
            },
            "phenotype": {"concept_id": None, "concept_name": "CASE-CONTROL"},
            "covariates": [dataclasses.asdict(i) for i in concept_defs],
            "parameters": {
                "n_population_pcs": args.n_pcs,
                "maf_threshold": args.maf_threshold,
                "imputation_score_cutoff": args.imputation_score_cutoff,
                "hare_population": args.hare_population,
            },
        }

        try:
            with mock.patch(
                "vadc_gwas_tools.subcommands.get_gwas_metadata.CohortServiceClient"
            ) as mock_client:
                instance = mock_client.return_value
                instance.get_cohort_definition.side_effect = [
                    case_cohort_def,
                    control_cohort_def,
                ]
                instance.get_concept_descriptions.return_value = concept_defs

                MOD._format_metadata = mock.MagicMock(return_value=expected)
                MOD.main(args)

                self.assertEqual(instance.get_cohort_definition.call_count, 2)
                instance.get_cohort_definition.assert_has_calls(
                    [mock.call(args.case_cohort_id), mock.call(args.control_cohort_id)]
                )

                instance.get_concept_descriptions.assert_called_once()
                instance.get_concept_descriptions.assert_called_with(
                    args.source_id, args.prefixed_concept_ids
                )

                MOD._format_metadata.assert_called_with(
                    case_cohort_def,
                    control_cohort_def,
                    concept_defs,
                    args.prefixed_outcome_concept_id,
                    args,
                )

            with open(outpath, 'r') as fh:
                res = yaml.safe_load(fh)

            self.assertEqual(
                json.dumps(res, sort_keys=True), json.dumps(expected, sort_keys=True)
            )
        finally:
            cleanup_files(outpath)
