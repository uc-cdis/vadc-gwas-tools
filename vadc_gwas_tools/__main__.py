"""
Main entrypoint for all vadc-gwas-tools.
"""
import argparse
import datetime
import sys
from signal import SIG_DFL, SIGPIPE, signal

from vadc_gwas_tools.common.logger import Logger
from vadc_gwas_tools.subcommands import (
    FilterSegments,
    GetCohortAttritionTable,
    GetCohortPheno,
    GetGwasMetadata,
    SplitFilenameByChr,
)

signal(SIGPIPE, SIG_DFL)


def main(args=None, extra_subparser=None) -> None:
    """
    The main method for vadc-gwas-tools.
    """
    # Setup logger
    Logger.setup_root_logger()

    logger = Logger.get_logger("main")

    # Print header
    logger.info("-" * 75)
    logger.info("Program Args: vadc-gwas-tools " + " ".join(sys.argv[1::]))
    logger.info("Date/time: {0}".format(datetime.datetime.now()))
    logger.info("-" * 75)
    logger.info("-" * 75)

    # Get args
    p = argparse.ArgumentParser("VADC GWAS Tools")
    subparsers = p.add_subparsers(dest="subcommand")
    subparsers.required = True

    # Subcommands
    SplitFilenameByChr.add(subparsers=subparsers)
    FilterSegments.add(subparsers=subparsers)
    GetCohortPheno.add(subparsers=subparsers)
    GetGwasMetadata.add(subparsers=subparsers)
    GetCohortAttritionTable.add(subparsers=subparsers)

    if extra_subparser:
        extra_subparser.add(subparsers=subparsers)

    options = p.parse_args(args)

    # Run
    cls = options.func(options)

    # Finish
    logger.info("Finished!")


if __name__ == "__main__":
    main()
