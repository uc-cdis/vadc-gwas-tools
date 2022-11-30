"""Tests for the ``vadc_gwas_tools.subcommands.ProcessInputVariables`` subcommand."""
import json
import tempfile
import unittest
from dataclasses import asdict
from typing import NamedTuple

from utils import cleanup_files

from vadc_gwas_tools.common.cohort_middleware import (
    CohortServiceClient,
    ConceptVariableObject,
    CustomDichotomousVariableObject,
)
from vadc_gwas_tools.common.logger import Logger
from vadc_gwas_tools.subcommands import ProcessInputVariables as MOD


class _mock_args(NamedTuple):
    raw_variables_json: str
    outcome: str
    output_raw_variable_json: str
    output_variable_json_w_hare: str
    output_other_json: str
    hare_concept_id: int


class TestProcessInputVariablesSubcommand(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.variable_list = [
            {"variable_type": "concept", "concept_id": 1000},
            {"variable_type": "concept", "concept_id": 1001},
            {"variable_type": "custom_dichotomous", "cohort_ids": [10, 20]},
        ]

    def make_hare_concept(self, concept_id: int):
        hare_variable = ConceptVariableObject(
            variable_type="concept", concept_id=concept_id
        )
        return hare_variable

    def test_continuous_outcome(self):
        (_, rv_path) = tempfile.mkstemp()
        (_, out_vj) = tempfile.mkstemp()
        (_, out_oj) = tempfile.mkstemp()
        (_, out_vvj) = tempfile.mkstemp()

        outcome = ConceptVariableObject(variable_type="concept", concept_id=1000)

        hare_concept_id = 10
        hare_variable = self.make_hare_concept(hare_concept_id)

        all_variables = json.loads(
            json.dumps(self.variable_list),
            object_hook=CohortServiceClient.decode_concept_variable_json,
        )
        all_variables_w_hare = all_variables + [hare_variable]

        exp_other = {
            "covariates": "ID_1001 ID_10_20",
            "outcome": "ID_1000",
            "outcome_type": "CONTINUOUS",
        }

        try:

            with open(rv_path, 'wt') as o:
                json.dump(self.variable_list, o)

            args = _mock_args(
                raw_variables_json=rv_path,
                outcome=json.dumps(asdict(outcome)),
                output_raw_variable_json=out_vvj,
                output_variable_json_w_hare=out_vj,
                output_other_json=out_oj,
                hare_concept_id=hare_concept_id,
            )

            MOD.main(args)

            with open(out_vvj, 'rt') as fh:
                validated_vars = json.load(
                    fh, object_hook=CohortServiceClient.decode_concept_variable_json
                )
            self.assertEqual(all_variables, validated_vars)

            with open(out_vj, 'rt') as fh:
                obs_vars_w_hare = json.load(
                    fh, object_hook=CohortServiceClient.decode_concept_variable_json
                )

            self.assertEqual(all_variables_w_hare, obs_vars_w_hare)

            with open(out_oj, 'rt') as fh:
                obs_other = json.load(fh)
            self.assertEqual(exp_other, obs_other)

        finally:
            cleanup_files([rv_path, out_vj, out_oj, out_vvj])

    def test_dichotomous_outcome(self):
        (_, rv_path) = tempfile.mkstemp()
        (_, out_vj) = tempfile.mkstemp()
        (_, out_oj) = tempfile.mkstemp()
        (_, out_vvj) = tempfile.mkstemp()

        outcome = CustomDichotomousVariableObject(
            variable_type="custom_dichotomous", cohort_ids=[10, 20]
        )

        hare_concept_id = 10
        hare_variable = self.make_hare_concept(hare_concept_id)

        all_variables = json.loads(
            json.dumps(self.variable_list),
            object_hook=CohortServiceClient.decode_concept_variable_json,
        )
        all_variables_w_hare = all_variables + [hare_variable]

        exp_other = {
            "covariates": "ID_1000 ID_1001",
            "outcome": "ID_10_20",
            "outcome_type": "BINARY",
        }
        
        exp_validated_vars_list = [
            {"variable_type": "custom_dichotomous", "cohort_ids": [10, 20]},
            {"variable_type": "concept", "concept_id": 1000},
            {"variable_type": "concept", "concept_id": 1001}
        ]
        exp_validated_vars = json.loads(
            json.dumps(exp_validated_vars_list),
            object_hook=CohortServiceClient.decode_concept_variable_json
        )
        all_variables_w_hare = exp_validated_vars + [hare_variable]

        try:

            with open(rv_path, 'wt') as o:
                json.dump(self.variable_list, o)

            args = _mock_args(
                raw_variables_json=rv_path,
                outcome=json.dumps(asdict(outcome)),
                output_raw_variable_json=out_vvj,
                output_variable_json_w_hare=out_vj,
                output_other_json=out_oj,
                hare_concept_id=hare_concept_id,
            )

            MOD.main(args)

            with open(out_vvj, 'rt') as fh:
                obs_raw_vars = json.load(
                    fh, object_hook=CohortServiceClient.decode_concept_variable_json
                )
            # This also checks if the validated variable list has outcome as the first item
            self.assertEqual(obs_raw_vars, exp_validated_vars)

            with open(out_vj, 'rt') as fh:
                obs_vars_w_hare = json.load(
                    fh, object_hook=CohortServiceClient.decode_concept_variable_json
                )

            self.assertEqual(all_variables_w_hare, obs_vars_w_hare)

            with open(out_oj, 'rt') as fh:
                obs_other = json.load(fh)
            self.assertEqual(exp_other, obs_other)

        finally:
            cleanup_files([rv_path, out_vj, out_oj, out_vvj])

    def test_outcome_not_in_variables(self):
        (_, rv_path) = tempfile.mkstemp()

        outcome = ConceptVariableObject(variable_type="concept", concept_id=100)

        hare_concept_id = 10
        hare_variable = self.make_hare_concept(hare_concept_id)

        try:

            with open(rv_path, 'wt') as o:
                json.dump(self.variable_list, o)

            args = _mock_args(
                raw_variables_json=rv_path,
                outcome=json.dumps(asdict(outcome)),
                output_raw_variable_json="fake1.json",
                output_variable_json_w_hare="fake2.json",
                output_other_json="fake3.json",
                hare_concept_id=hare_concept_id,
            )

            with self.assertRaises(AssertionError) as _:
                MOD.main(args)

        finally:
            cleanup_files(rv_path)
