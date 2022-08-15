"""A subcommond that curates the summary GWAS statistics CSV files
to extract:
1) top N hits (based on P-value)
2) hits below user-provided P-value cutoff

@author: Kyle Hernandez <kmhernan@uchicago.edu>
"""
import csv
import gzip
import os
from argparse import ArgumentParser, Namespace
from typing import List, TextIO, Tuple

from vadc_gwas_tools.common.logger import Logger
from vadc_gwas_tools.common.top_hits_heap import GwasHit, TopHitsHeap
from vadc_gwas_tools.subcommands import Subcommand


class CurateGwasHits(Subcommand):
    @classmethod
    def __add_arguments__(cls, parser: ArgumentParser) -> None:
        """Add subcommand params."""
        parser.add_argument(
            "--summary_stats_dir",
            required=True,
            type=str,
            help="Path to directory with per-chromosome GWAS summary statistic CSVs.",
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
            "--out_prefix",
            required=True,
            type=str,
            help="Output prefix to use for both outputs: <prefix>.top_X_hits.csv.gz, <prefix>.below_cutoff_hits.csv.gz",
        )

    @classmethod
    def main(cls, options: Namespace) -> None:
        """
        Entrypoint for CurateGwasHits.
        """
        logger = Logger.get_logger(cls.__tool_name__())
        logger.info(cls.__get_description__())

        # Load the list of summary statistics.
        sdir = os.path.normpath(options.summary_stats_dir)
        summary_stats_list = sorted(glob.glob(sdir + '/*.csv.gz'))
        if len(summary_stats_list) == 0:
            msg = f"Error! No GWAS summary statistics files found in {sdir}!"
            logger.error(msg)
            raise RuntimeError(msg)

        logger.info(
            f"Found {len(summary_stats_list)} summary stats files in directory {sdir}."
        )

        # Setup output paths
        out_top_hits = f"{options.out_prefix}.top_{options.top_n_hits}_hits.csv.gz"
        out_cutoff = f"{options.out_prefix}.below_cutoff_hits.csv.gz"

        # Setup heap
        top_hits_heap = TopHitsHeap(options.top_n_hits)

        logger.info(f"Hits below cutoff will be output to {out_cutoff}.")
        with gzip.open(out_cutoff, 'wt') as o_cutoff:
            oheader, total, below_cutoff = cls._process_summary_csvs(
                cutoff=options.pvalue_cutoff,
                top_hits_heap=top_hits_heap,
                csv_files=summary_stats_list,
                sig_hits_ofh=o_cutoff,
                logger=logger,
            )

        # Write out top hits
        logger.info(f"Top hits will be output to {out_top_hits}.")
        with gzip.open(out_top_hits, 'wt') as o_hits:
            cls._process_top_hits(
                top_hits_heap=top_hits_heap, header=oheader, top_hits_ofh=o_hits
            )

        logger.info(f"Processed a total of {total} records.")
        logger.info(f"Number of records below cutoff: {below_cutoff}")

    @classmethod
    def _process_summary_csvs(
        cls,
        cutoff: float,
        top_hits_heap: TopHitsHeap,
        csv_files: List[str],
        sig_hits_ofh: TextIO,
        logger: Logger,
    ) -> Tuple[List[str], int, int]:
        """
        Main loading logic of summary CSV files. Initializes the top hits
        heap object, loops over each CSV, adds records to the heap, and
        writes out any hits that are below the cutoff.
        """
        oheader = None
        writer = csv.writer(sig_hits_ofh)
        total = 0
        below_cutoff = 0
        for csv_file in csv_files:
            logger.info(f"Processing GWAS summary statistics file: {csv_file}")
            with gzip.open(csv_file, 'rt') as fh:
                reader = csv.reader(fh)
                header = next(reader)
                if oheader is None:
                    oheader = header
                    writer.writerow(oheader)

                for row in reader:
                    row = dict(zip(header, row))
                    pval = float(row['Score.pval'])
                    record = GwasHit(pvalue=-1.0 * pval, item=row)
                    top_hits_heap += record
                    if pval <= cutoff:
                        below_cutoff += 1
                        writer.writerow([row.get(i, '') for i in oheader])

                    total += 1
                    if total % 1_000_000 == 0:
                        logger.info(f"Processed {total} records...")
        return oheader, total, below_cutoff

    @classmethod
    def _process_top_hits(
        cls, top_hits_heap: TopHitsHeap, header: List[str], top_hits_ofh: TextIO
    ) -> None:
        """
        Sorts the top hits object and writes out to CSV.
        """
        writer = csv.writer(top_hits_ofh)
        writer.writerow(header)
        for item in sorted(top_hits_heap._items, reverse=True):
            row = [item.item.get(i, '') for i in header]
            writer.writerow(row)

    @classmethod
    def __get_description__(cls) -> str:
        """
        Description of tool.
        """
        return (
            "Curates the millions of summary statistics output by the GENESIS workflow. "
            "The per-chromosome CSVs are curated and concatenated into 2 separate files: "
            "1) The hits below user-defined P-value cutoff and 2) the top N hits."
        )
