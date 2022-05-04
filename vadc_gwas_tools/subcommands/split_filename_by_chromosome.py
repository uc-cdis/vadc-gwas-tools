"""Splits GDS filenames on the 'chr' string and outputs a JSON string
containing 'file_prefix' and 'file_suffix' properties. Adapted from
Saiju's (VA) CWL tools. It is not elegant and makes assumptions, in the
future we likely will re-think the workflow.

@author: Kyle Hernandez <kmhernan@uchicago.edu>
"""
import json
import os
import sys
from argparse import ArgumentParser, Namespace
from typing import TextIO

from vadc_gwas_tools.common.logger import Logger
from vadc_gwas_tools.subcommands import Subcommand


class SplitFilenameByChr(Subcommand):
    @classmethod
    def __add_arguments__(cls, parser: ArgumentParser) -> None:
        """Add the subcommand params"""
        parser.add_argument(
            "--gds_file",
            required=True,
            help="Path to GDS file (file doesn't have to exist).",
        )
        parser.add_argument(
            "-o",
            "--output",
            required=False,
            type=str,
            default=None,
            help="Path to write out JSON containing 'file_prefix' and 'file_suffix'. "
            "By default will be stdout.",
        )

    @classmethod
    def main(cls, options: Namespace) -> None:
        """
        Entrypoint for SplitFilenameByChr
        """
        logger = Logger.get_logger(cls.__tool_name__())
        logger.info(cls.__get_description__())
        logger.info("Processing gds file {}...".format(options.gds_file))

        bname = os.path.basename(options.gds_file)

        if "chr" not in bname:
            raise AssertionError("The filename must contain 'chr': {}".format(bname))

        split_ext_res = os.path.splitext(bname)
        if not split_ext_res[1] or "." not in split_ext_res[0]:
            raise AssertionError(
                "The filename must contain '.' before extension: {}".format(bname)
            )

        pfx = bname.split("chr")[0] + "chr"
        sfx = "." + ".".join(bname.split("chr")[1].split(".")[1:])
        dat = {"file_prefix": pfx, "file_suffix": sfx}

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
            "Takes a string path of a GDS file and splits into "
            "prefix/suffix around the 'chr' string. Assumes '.' are used in filename. "
            "A JSON object with 'file_prefix' and 'file_suffix' properties "
            "is output to stdout by default or a file if the path is provided."
        )
