"""Tests for the ``vadc_gwas_tools.subcommands.GetDescriptiveStatistics`` subcommand."""
import json
import tempfile
import unittest
from typing import List, NamedTuple, Optional
from unittest import mock

from utils import cleanup_files

from vadc_gwas_tools.common.cohort_middleware import (
    CohortServiceClient,
    ConceptVariableObject,
    CustomDichotomousVariableObject,
)
from vadc_gwas_tools.common.const import CASE_COUNTS_VAR_ID, CONTROL_COUNTS_VAR_ID
from vadc_gwas_tools.subcommands import GetDescriptiveStatistics as MOD


@mock.patch(
    "vadc_gwas_tools.subcommands.get_descriptive_stats.CohortServiceClient.get_descriptive_statistics"
)
@mock.patch("vadc_gwas_tools.subcommands.get_descriptive_stats.json.load")
@mock.patch("vadc_gwas_tools.subcommands.get_descriptive_stats.json.loads")
def test_get_descriptive_statistics(
    self, mock_json_loads, mock_json_load, mock_get_descriptive_statistics
):
    (_, fpath1) = tempfile.mkstemp()
    (_, fpath2) = tempfile.mkstemp()

    try:
        # Prepare the JSON file with variable definitions
        with open(fpath1, 'wt') as o:
            json.dump(self.continuous_variable_list, o)

        args = MockArgs(
            source_id=2,
            source_population_cohort=300,
            variables_json=fpath1,
            outcome=json.dumps(self.continuous_variable_list[0]),
            prefixed_breakdown_concept_id=self.prefixed_breakdown_concept_id,
            output_csv_prefix=fpath2,
            output_combined_json="/some/path/combined.json",
            hare_population=self.hare_population,
        )

        # Setup mocked responses
        mock_json_load.return_value = [
            ConceptVariableObject(**var)
            if var["variable_type"] == "concept"
            else CustomDichotomousVariableObject(**var)
            for var in self.continuous_variable_list
        ]

        mock_json_loads.return_value = ConceptVariableObject(
            **self.continuous_variable_list[0]
        )

        # Correctly structured mock response (no extra nesting)
        mock_get_descriptive_statistics.return_value = {
            "statistics": [{"key": "value"}]
        }

        # Execute the method being tested (MOD.main)
        MOD.main(args)

        # Verify the client call was made correctly
        mock_get_descriptive_statistics.assert_called_once_with(
            args.source_id,
            args.source_population_cohort,
            f"{args.output_csv_prefix}.descriptive_stats.json",
            mock_json_load.return_value,
            args.prefixed_breakdown_concept_id,
            args.hare_population,
        )

        # Verify the output file is correctly written
        output_json_path = f"{args.output_csv_prefix}.descriptive_stats.json"
        with open(output_json_path, 'rt') as f:
            result = json.load(f)

        # Assert that the output JSON matches your mock
        self.assertEqual(result, {"statistics": [{"key": "value"}]})

    finally:
        cleanup_files([fpath1, fpath2, output_json_path])
