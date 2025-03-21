"""Wraps useful PheWeb subcommands that generate files needed for
producing Manhattan and QQ plots.

@author: Pieter Lukasse <plukasse@uchicago.edu>
"""
import dataclasses
import json
from argparse import ArgumentParser, Namespace
from typing import Any, Dict, List, Optional, Tuple, Union

import yaml
from pheweb.load.manhattan import make_manhattan_json_file_explicit
from pheweb.load.qq import make_json_file_explicit

from vadc_gwas_tools.common.logger import Logger
from vadc_gwas_tools.subcommands import Subcommand


class GetPheWebPlotJson(Subcommand):
    @classmethod
    def __add_arguments__(cls, parser: ArgumentParser) -> None:
        """Add the subcommand params"""

        parser.add_argument(
            "--in_tsv",
            required=True,
            type=str,
            help="Path to the TSV input file with all variants.",
        )
        parser.add_argument(
            "--out_json",
            required=True,
            type=str,
            help=(
                "Path to the output JSON file to be made. The JSON file can be "
                "used by PheWeb frontend code to generate an interactive Manhattan "
                "or QQ plot, depending on -out_plot_type argument."
            ),
        )
        parser.add_argument(
            "--out_plot_type",
            required=True,
            choices=['manhattan', 'qq'],
            help="Type of desired output plot type",
        )

    @classmethod
    def main(cls, options: Namespace) -> None:
        """
        Entrypoint for GetPheWebPlotJson
        """
        logger = Logger.get_logger(cls.__tool_name__())
        logger.info(cls.__get_description__())

        if options.out_plot_type == 'manhattan':
            # read TSV and convert to PheWeb Manhattan Plot JSON file format:
            make_manhattan_json_file_explicit(in_filepath=options.in_tsv, out_filepath=options.out_json)
        elif options.out_plot_type == 'qq':
            # read tsv and convert to PheWeb qq plot json file format:
            make_json_file_explicit(in_filepath=options.in_tsv, out_filepath=options.out_json, pheno={})

    @classmethod
    def __get_description__(cls) -> str:
        """
        Description of tool.
        """
        return (
            "Generates a PheWeb Manhattan or QQ plot json file "
            "based on given input tsv file."
        )
