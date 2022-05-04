"""Filters segments based on:

* GDS Filenames
* file_prefix output from SplitFilenameByChr
* file_suffix output from SplitFilenameByChr

This was adapted from Saiju's (VA) CWL tools. It is not elegant and makes
assumptions, in the future we likely will re-think the workflow.

@author: Kyle Hernandez <kmhernan@uchicago.edu>
"""
import json
import os
import sys
from argparse import ArgumentParser, Namespace

from vadc_gwas_tools.common.logger import Logger
from vadc_gwas_tools.subcommands import Subcommand


class FilterSegments(Subcommand):
    @classmethod
    def __add_arguments__(cls, parser: ArgumentParser) -> None:
        """Add the subcommand params"""
        parser.add_argument(
            "gds_filenames",
            nargs="+",
            help="List of GDS filenames for each chromosome.",
        )
        parser.add_argument(
            "--file_prefix",
            required=True,
            type=str,
            help="file prefix before 'chr'",
        )
        parser.add_argument(
            "--file_suffix",
            required=True,
            type=str,
            help="file suffix",
        )
        parser.add_argument(
            "--segment_file", required=True, type=str, help="Path to segment file."
        )
        parser.add_argument(
            "-o",
            "--output",
            required=False,
            type=str,
            default=None,
            help="Path to write out JSON containing 'chromosomes' and 'segments'. "
            "By default will be stdout.",
        )

    @classmethod
    def main(cls, options: Namespace) -> None:
        """
        Entrypoint for FilterSegments
        """
        logger = Logger.get_logger(cls.__tool_name__())
        logger.info(cls.__get_description__())
        logger.info("Processing gds files {}...".format(options.gds_filenames))

        gds_files = set([os.path.basename(i) for i in options.gds_filenames])

        logger.info("Processing segment file {}...".format(options.segment_file))
        chromosomes_present = set()
        segments = []
        with open(options.segment_file, "rt") as fh:
            for n, line in enumerate(fh.readlines()):
                chrom = line.split()[0]
                if f"{options.file_prefix}{chrom}{options.file_suffix}" in gds_files:
                    chromosomes_present.add(chrom)
                    segments += [n]

        dat = {"chromosomes": sorted(list(chromosomes_present)), "segments": segments}
        if not options.output:
            logger.info("Writing JSON to stdout")
            json.dump(dat, sys.stdout, sort_keys=True)
        else:
            logger.info("Writing JSON to {}".format(options.output))
            with open(options.output, "wt") as o:
                json.dump(dat, o, sort_keys=True)

    @classmethod
    def __get_description__(cls) -> str:
        """
        Description of tool.
        """
        return (
            "Filters segments to only those on chromosomes present in the analysis. "
            "Uses the prefix and suffix produced by the SplitFilenameByChr tool. "
            "A JSON object with 'chromosomes' and 'segments' properties "
            "is output to stdout by default or a file if the path is provided."
        )
