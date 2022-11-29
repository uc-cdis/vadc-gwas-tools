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
    CohortServiceClient,
    ConceptDescriptionResponse,
    ConceptVariableObject,
    CustomDichotomousVariableObject,
)
from vadc_gwas_tools.subcommands import GetGwasMetadata as MOD

class MockArgs(NamedTuple):
    source_id: int
    source_population_cohort: int
    variables_json: str
    outcome: str
    n_pcs: int
    maf_threshold: float
    imputation_score_cutoff: float
    hare_population: str
    pvalue_cutoff: Optional[float]
    top_n_hits: Optional[int]
    output: str


def make_cohort_def(cohort_definition_id, cohort_name, cohort_description):
    return CohortDefinitionResponse(
        cohort_definition_id=cohort_definition_id,
        cohort_name=cohort_name,
        cohort_description=cohort_description,
    )


def make_concept_def(
    concept_id, prefixed_concept_id, concept_name, concept_code, concept_type
):
    return ConceptDescriptionResponse(
        concept_id=concept_id,
        prefixed_concept_id=prefixed_concept_id,
        concept_name=concept_name,
        concept_code=concept_code,
        concept_type=concept_type,
    )


class TestGetGwasMetadataSubcommand_GetVariableLists(unittest.TestCase):
    def test_get_variable_lists(self):
        variables = [
            ConceptVariableObject(variable_type="concept", concept_id=1001),
            ConceptVariableObject(variable_type="concept", concept_id=1002),
            CustomDichotomousVariableObject(
                variable_type="custom_dichotomous", cohort_ids=[1, 2]
            ),
        ]
        outcome = ConceptVariableObject(
            variable_type="concept", concept_id=1001
            )
        concept_variables, custom_variables = MOD._get_variable_lists(variables, outcome)
        self.assertEqual(concept_variables, [variables[1]])
        self.assertEqual(custom_variables, [variables[2]])


class TestGetGwasMetadataSubcommand_CustomDichotomousCohortMetadata(unittest.TestCase):
    def test_get_custom_dichotomous_cohort_metadata(self):
        variables = [
            CustomDichotomousVariableObject(
                variable_type="custom_dichotomous", cohort_ids=[1, 2]
            ),
            CustomDichotomousVariableObject(
                variable_type="custom_dichotomous", cohort_ids=[1, 4]
            ),
        ]

        cohort_defs = [
            make_cohort_def(1, "A", "Something A"),
            make_cohort_def(2, "B", "Something B"),
            make_cohort_def(4, "C", "Something C"),
        ]
        mock_client = mock.MagicMock(spec_set=CohortServiceClient)
        mock_client.get_cohort_definition.side_effect = cohort_defs
        res = MOD._get_custom_dichotomous_cohort_metadata(variables, mock_client)

        mock_client.get_cohort_definition.assert_has_calls(
            [mock.call(1), mock.call(2), mock.call(4)], any_order=True
        )

        self.assertEqual(mock_client.get_cohort_definition.call_count, 3)

        expected = {1: cohort_defs[0], 2: cohort_defs[1], 4: cohort_defs[2]}
        self.assertEqual(res, expected)

        mock_client.reset_mock()
        res = MOD._get_custom_dichotomous_cohort_metadata([], mock_client)
        self.assertEqual(res, {})
        mock_client.get_cohort_definition.assert_not_called()


class GetGwasMetadataSubcommand_SharedObjects(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.source_id = 2
        self.source_population_cohort = 9
        self.outcome_continuous_str = '{"variable_type": "concept", "concept_id": 1003}'
        self.outcome_case_control_str = '{"variable_type":"custom_dichotomous", "cohort_ids":[1, 2], "provided_name":"test123"}'
        self.outcome_continuous = json.loads(
            self.outcome_continuous_str, object_hook=CohortServiceClient.decode_concept_variable_json
        )
        self.outcome_case_control = json.loads(
            self.outcome_case_control_str, object_hook=CohortServiceClient.decode_concept_variable_json
        )
        self.concept_defs = [
            make_concept_def(1003, "ID_3", "Var C", "VALUE", "MVP Type"),
            make_concept_def(1001, "ID_1", "Var A", "VALUE", "MVP Type"),
            make_concept_def(1002, "ID_2", "Var B", "VALUE", "MVP Type"),
            
        ]
        self.custom_dichotomous_variables = [
            CustomDichotomousVariableObject(
                variable_type="custom_dichotomous", cohort_ids=[1, 2], provided_name="test123"
            ),
            CustomDichotomousVariableObject(
                variable_type="custom_dichotomous", cohort_ids=[1, 4], provided_name="test456"
            ),
        ]
        self.custom_dichotomous_cohort_meta = {
            1: make_cohort_def(1, "A", "Something A"),
            2: make_cohort_def(2, "B", "Something B"),
            4: make_cohort_def(4, "C", "Something C"),
        }

    def get_mock_args(self, variables_json, outcome, output, case_control=False):
        return MockArgs(
            source_id=self.source_id,
            source_population_cohort=self.source_population_cohort,
            variables_json=variables_json,
            outcome=outcome,
            n_pcs=3,
            maf_threshold=0.01,
            imputation_score_cutoff=0.3,
            hare_population="Hispanic",
            pvalue_cutoff=5e-8,
            top_n_hits=100,
            output=output,
        )

    def get_exp_custom_dichotomous(self):
        cd_0_0 = dataclasses.asdict(self.custom_dichotomous_cohort_meta[1])
        cd_0_0["value"] = 0
        cd_0_1 = dataclasses.asdict(self.custom_dichotomous_cohort_meta[2])
        cd_0_1["value"] = 1

        cd_1_0 = dataclasses.asdict(self.custom_dichotomous_cohort_meta[1])
        cd_1_0["value"] = 0
        cd_1_1 = dataclasses.asdict(self.custom_dichotomous_cohort_meta[4])
        cd_1_1["value"] = 1
        return [
            {
                "custom_dichotomous": {
                    "cohorts": [cd_0_0, cd_0_1],
                }
            },
            {
                "custom_dichotomous": {
                    "cohorts": [cd_1_0, cd_1_1],
                }
            },
        ]


class GetGwasMetadataSubcommand_FormatMetadata(GetGwasMetadataSubcommand_SharedObjects):
    def test_format_metadata_continuous(self):
        args = self.get_mock_args("/path/fake.json", self.outcome_continuous_str, "/path/fake.yaml")

        source_population_cohort_def = make_cohort_def(args.source_population_cohort, "SourceCohort", "Fake")
        outcome_section = dataclasses.asdict(self.concept_defs[0])
        outcome_section["type"] = "CONTINUOUS"
        expected = {
            "source_cohort": dataclasses.asdict(source_population_cohort_def),
            "outcome": outcome_section,
            "covariates": [dataclasses.asdict(i) for i in self.concept_defs[1:]]
            + self.get_exp_custom_dichotomous(),
            "parameters": {
                "n_population_pcs": args.n_pcs,
                "maf_threshold": args.maf_threshold,
                "imputation_score_cutoff": args.imputation_score_cutoff,
                "hare_population": args.hare_population,
                "pvalue_cutoff": 5e-8,
                "top_n_hits": 100,
            },
        }

        res = MOD._format_metadata(
            options=args,
            source_cohort_def=source_population_cohort_def,
            outcome=self.outcome_continuous,
            concept_data=self.concept_defs[1:],
            custom_dichotomous_variables=self.custom_dichotomous_variables,
            custom_dichotomous_cohort_metadata=self.custom_dichotomous_cohort_meta,
            outcome_data=self.concept_defs[0]
        )

        self.assertEqual(
            json.dumps(res, sort_keys=True), json.dumps(expected, sort_keys=True)
        )

    def test_format_metadata_case_control(self):
        print(self.outcome_case_control)
        args = self.get_mock_args(
            "/path/fake.json", self.outcome_case_control_str, "/path/fake.yaml", case_control=True
        )
        source_population_cohort_def = make_cohort_def(args.source_population_cohort, "SourceCohort", "Fake")
        outcome =  self.outcome_case_control
        case_cohort_def = make_cohort_def(outcome.cohort_ids[1], "CASE", "Fake")
        control_cohort_def = make_cohort_def(outcome.cohort_ids[0], "CONTROL", "Fake")
        outcome_section = {
            "type":"CASE-CONTROL",
            "concept_name": "test123",
            "concept_cohorts": {
                "case_cohort": dataclasses.asdict(case_cohort_def),
                "control_cohort": dataclasses.asdict(control_cohort_def)
            }
        }

        expected = {
            "source_cohort": dataclasses.asdict(source_population_cohort_def),
            "outcome": outcome_section,
            "covariates": [dataclasses.asdict(i) for i in self.concept_defs]
            + self.get_exp_custom_dichotomous()[1:],
            "parameters": {
                "n_population_pcs": args.n_pcs,
                "maf_threshold": args.maf_threshold,
                "imputation_score_cutoff": args.imputation_score_cutoff,
                "hare_population": args.hare_population,
                "pvalue_cutoff": 5e-8,
                "top_n_hits": 100,
            },
        }

        res = MOD._format_metadata(
            options=args,
            source_cohort_def=source_population_cohort_def,
            outcome=self.outcome_case_control,
            concept_data=self.concept_defs,
            custom_dichotomous_variables=self.custom_dichotomous_variables[1:],
            custom_dichotomous_cohort_metadata=self.custom_dichotomous_cohort_meta,
            case_cohort_def=case_cohort_def,
            control_cohort_def=control_cohort_def,
        )

        self.assertEqual(
            json.dumps(res, sort_keys=True), json.dumps(expected, sort_keys=True)
        )


class GetGwasMetadataSubcommand_Main(GetGwasMetadataSubcommand_SharedObjects):
    def setUp(self):
        super().setUp()
        self.concept_variables = [
            ConceptVariableObject(variable_type="concept", concept_id=1003),
            ConceptVariableObject(variable_type="concept", concept_id=1001),
            ConceptVariableObject(variable_type="concept", concept_id=1002),
        ]

    def test_main_continuous_params(self):
        (_, outpath) = tempfile.mkstemp(suffix='.yaml')
        (_, vjsonpath) = tempfile.mkstemp(suffix='.json')

        args = self.get_mock_args(vjsonpath, self.outcome_continuous_str, outpath, case_control=False)

        with open(vjsonpath, 'wt') as o:
            json.dump(
                [dataclasses.asdict(i) for i in self.concept_variables]
                + [dataclasses.asdict(i) for i in self.custom_dichotomous_variables],
                o,
            )
        source_population_cohort_def = make_cohort_def(
            args.source_population_cohort, "SourceCohort", "Fake"
            )
        outcome_section = dataclasses.asdict(self.concept_defs[0])
        outcome_section["type"] = "CONTINUOUS"

        expected = {
            "source_cohort": dataclasses.asdict(source_population_cohort_def),
            "outcome": outcome_section,
            "covariates": [dataclasses.asdict(i) for i in self.concept_defs[1:]]
            + self.get_exp_custom_dichotomous(),
            "parameters": {
                "n_population_pcs": args.n_pcs,
                "maf_threshold": args.maf_threshold,
                "imputation_score_cutoff": args.imputation_score_cutoff,
                "hare_population": args.hare_population,
                "pvalue_cutoff": 5e-8,
                "top_n_hits": 100,
            },
        }

        try:
            with mock.patch.object(
                CohortServiceClient, "get_cohort_definition"
            ) as mock_cohort_def, mock.patch.object(
                CohortServiceClient, "get_concept_descriptions"
            ) as mock_concept_def, mock.patch.object(
                MOD, "_get_custom_dichotomous_cohort_metadata"
            ) as mock_get_custom_dichotomous:
                mock_cohort_def.return_value = source_population_cohort_def
                mock_concept_def.side_effect = [[self.concept_defs[0]], self.concept_defs[1:]]

                MOD._format_metadata = mock.MagicMock(return_value=expected)
                mock_get_custom_dichotomous.return_value = (
                    self.custom_dichotomous_cohort_meta
                )
                MOD.main(args)

                mock_cohort_def.assert_called_once_with(self.source_population_cohort)
                assert mock_concept_def.call_count == 2

                mock_get_custom_dichotomous.assert_called_once()
                name, _args, kwargs = mock_get_custom_dichotomous.mock_calls[0]
                self.assertEqual(self.custom_dichotomous_variables, _args[0])

                MOD._format_metadata.assert_called_with(
                    options=args,
                    source_cohort_def=source_population_cohort_def,
                    outcome=self.outcome_continuous,
                    concept_data=self.concept_defs[1:],
                    custom_dichotomous_variables=self.custom_dichotomous_variables,
                    custom_dichotomous_cohort_metadata=self.custom_dichotomous_cohort_meta,
                    outcome_data=self.concept_defs[0]
                )

            with open(outpath, 'r') as fh:
                res = yaml.safe_load(fh)

            self.assertEqual(
                json.dumps(res, sort_keys=True), json.dumps(expected, sort_keys=True)
            )
        finally:
            cleanup_files([outpath, vjsonpath])

    def test_main_case_control_params(self):
        (_, outpath) = tempfile.mkstemp(suffix='.yaml')
        (_, vjsonpath) = tempfile.mkstemp(suffix='.json')

        args = self.get_mock_args(vjsonpath, self.outcome_case_control_str, outpath, case_control=True)

        with open(vjsonpath, 'wt') as o:
            json.dump(
                [dataclasses.asdict(i) for i in self.custom_dichotomous_variables]
                +[dataclasses.asdict(i) for i in self.concept_variables],
                o,
            )

        source_population_cohort_def = make_cohort_def(args.source_population_cohort, "SourceCohort", "Fake")
        outcome =  self.outcome_case_control
        case_cohort_def = make_cohort_def(outcome.cohort_ids[1], "CASE", "Fake")
        control_cohort_def = make_cohort_def(outcome.cohort_ids[0], "CONTROL", "Fake")
        outcome_section = {
            "type":"CASE-CONTROL",
            "concept_name": "Fake",
            "concept_cohorts": {
                "case_cohort": dataclasses.asdict(case_cohort_def),
                "control_cohort": dataclasses.asdict(control_cohort_def)
            }
        }

        expected = {
            "source_cohort": dataclasses.asdict(source_population_cohort_def),
            "outcome": outcome_section,
            "covariates": [dataclasses.asdict(i) for i in self.concept_defs]
            + self.get_exp_custom_dichotomous()[1:],
            "parameters": {
                "n_population_pcs": args.n_pcs,
                "maf_threshold": args.maf_threshold,
                "imputation_score_cutoff": args.imputation_score_cutoff,
                "hare_population": args.hare_population,
                "pvalue_cutoff": 5e-8,
                "top_n_hits": 100,
            },
        }

        try:
            with mock.patch.object(
                CohortServiceClient, "get_cohort_definition"
            ) as mock_cohort_def, mock.patch.object(
                CohortServiceClient, "get_concept_descriptions"
            ) as mock_concept_def, mock.patch.object(
                MOD, "_get_custom_dichotomous_cohort_metadata"
            ) as mock_get_custom_dichotomous:

                # mock_cohort_def.side_effect = [case_cohort_def, control_cohort_def]
                mock_cohort_def.side_effect = [
                    source_population_cohort_def,
                    case_cohort_def,
                    control_cohort_def
                    ]
                mock_concept_def.return_value = self.concept_defs

                MOD._format_metadata = mock.MagicMock(return_value=expected)
                mock_get_custom_dichotomous.return_value = (
                    self.custom_dichotomous_cohort_meta
                )
                MOD.main(args)

                self.assertEqual(mock_cohort_def.call_count, 3)
                mock_cohort_def.assert_has_calls(
                    [
                        mock.call(args.source_population_cohort),
                        mock.call(outcome.cohort_ids[1]), 
                        mock.call(outcome.cohort_ids[0]),

                    ]
                )

                mock_concept_def.assert_called_once_with(
                    args.source_id, [i.concept_id for i in self.concept_defs]
                )

                mock_get_custom_dichotomous.assert_called_once()
                name, _args, kwargs = mock_get_custom_dichotomous.mock_calls[0]
                self.assertEqual(self.custom_dichotomous_variables[1], _args[0][0])

                MOD._format_metadata.assert_called_with(
                    options=args,
                    source_cohort_def=source_population_cohort_def,
                    outcome=self.outcome_case_control,
                    concept_data=self.concept_defs,
                    custom_dichotomous_variables=self.custom_dichotomous_variables[1:],
                    custom_dichotomous_cohort_metadata=self.custom_dichotomous_cohort_meta,
                    case_cohort_def=case_cohort_def,
                    control_cohort_def=control_cohort_def,
                )

            with open(outpath, 'r') as fh:
                res = yaml.safe_load(fh)

            self.assertEqual(
                json.dumps(res, sort_keys=True), json.dumps(expected, sort_keys=True)
            )
        finally:
            cleanup_files([outpath, vjsonpath])
