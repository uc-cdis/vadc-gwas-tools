"""Tests for `vadc_gwas_tools.subcommands.CurateGwasHits`."""
import csv
import glob
import gzip
import os
import random
import tempfile
import unittest
from io import StringIO
from typing import NamedTuple

from utils import captured_output, cleanup_files

from vadc_gwas_tools.common.const import STATS_COLUMN_PVAL, STATS_COLUMN_SPA_PVAL
from vadc_gwas_tools.common.logger import Logger
from vadc_gwas_tools.common.top_hits_heap import GwasHit, TopHitsHeap
from vadc_gwas_tools.subcommands import CurateGwasHits as MOD


class TestCurateGwasHits_process_summary_csvs(unittest.TestCase):
    Cutoff = 5e-8
    Nhits = 5

    def generate_test_csvs(self, pval_col=STATS_COLUMN_PVAL):
        odir = tempfile.mkdtemp()
        ofils = []
        header = ['key', pval_col]

        for i in range(5):
            (_, opath) = tempfile.mkstemp(dir=odir, text=True)
            ofils.append(opath)
            with gzip.open(opath, 'wt') as o:
                writer = csv.writer(o)
                writer.writerow(header)
                if i == 3:
                    writer.writerow([f"{i}.0", "0.01"])
                    writer.writerow([f"{i}.1", "5e-10"])
                    writer.writerow([f"{i}.2", "5e-8"])
                else:
                    for j in range(3):
                        row = [f"{i}.{j}", str(random.uniform(1e-5, 0.05))]
                        writer.writerow(row)
        return ofils

    def test__process_summary_csvs(self):

        csv_files = self.generate_test_csvs()
        th_heap = TopHitsHeap(TestCurateGwasHits_process_summary_csvs.Nhits)
        out_sig_hits = StringIO()

        try:
            with captured_output() as (sout, serr):
                logger = Logger.get_logger("test_curate_gwas_hits")
                oheader, total, below_cutoff = MOD._process_summary_csvs(
                    cutoff=TestCurateGwasHits_process_summary_csvs.Cutoff,
                    top_hits_heap=th_heap,
                    csv_files=csv_files,
                    sig_hits_ofh=out_sig_hits,
                    logger=logger,
                )

                self.assertEqual(['key', STATS_COLUMN_PVAL], oheader)
                self.assertEqual(15, total)
                self.assertEqual(2, below_cutoff)
            csv_records = [i for i in out_sig_hits.getvalue().split("\n") if i]
            self.assertEqual(3, len(csv_records))
            self.assertEqual(f"key,{STATS_COLUMN_PVAL}", csv_records[0].rstrip())
            self.assertEqual("3.1,5e-10", csv_records[1].rstrip())
            self.assertEqual("3.2,5e-8", csv_records[2].rstrip())

        finally:
            cleanup_files(csv_files)

    def test__process_summary_csvs_spa(self):

        csv_files = self.generate_test_csvs(pval_col=STATS_COLUMN_SPA_PVAL)
        th_heap = TopHitsHeap(TestCurateGwasHits_process_summary_csvs.Nhits)
        out_sig_hits = StringIO()

        try:
            with captured_output() as (sout, serr):
                logger = Logger.get_logger("test_curate_gwas_hits")
                oheader, total, below_cutoff = MOD._process_summary_csvs(
                    cutoff=TestCurateGwasHits_process_summary_csvs.Cutoff,
                    top_hits_heap=th_heap,
                    csv_files=csv_files,
                    sig_hits_ofh=out_sig_hits,
                    logger=logger,
                )

                self.assertEqual(['key', STATS_COLUMN_SPA_PVAL], oheader)
                self.assertEqual(15, total)
                self.assertEqual(2, below_cutoff)
            csv_records = [i for i in out_sig_hits.getvalue().split("\n") if i]
            self.assertEqual(3, len(csv_records))
            self.assertEqual(f"key,{STATS_COLUMN_SPA_PVAL}", csv_records[0].rstrip())
            self.assertEqual("3.1,5e-10", csv_records[1].rstrip())
            self.assertEqual("3.2,5e-8", csv_records[2].rstrip())

        finally:
            cleanup_files(csv_files)

    def test__process_summary_csvs_exception(self):

        csv_files = self.generate_test_csvs(pval_col='pval')
        th_heap = TopHitsHeap(TestCurateGwasHits_process_summary_csvs.Nhits)
        out_sig_hits = StringIO()

        try:
            with self.assertRaises(AssertionError):
                logger = Logger.get_logger("test_curate_gwas_hits")
                oheader, total, below_cutoff = MOD._process_summary_csvs(
                    cutoff=TestCurateGwasHits_process_summary_csvs.Cutoff,
                    top_hits_heap=th_heap,
                    csv_files=csv_files,
                    sig_hits_ofh=out_sig_hits,
                    logger=logger,
                )
        finally:
            cleanup_files(csv_files)


class TestCurateGwasHits_process_top_hits(unittest.TestCase):
    Cutoff = 5e-8
    Nhits = 3

    def generate_test_records(self, pval_col=STATS_COLUMN_PVAL):
        records = [
            ['key', pval_col],
            ["0", "0.01"],
            ["1", "5e-10"],
            ["2", "5e-8"],
            ["3", "0.01"],
        ]
        return records

    def test__process_top_hits(self):
        test_records = self.generate_test_records()
        th_heap = TopHitsHeap(TestCurateGwasHits_process_top_hits.Nhits)
        out_top_hits = StringIO()

        for record in test_records[1:]:
            rec = dict(zip(test_records[0], record))
            pval = float(rec[STATS_COLUMN_PVAL])
            ghit = GwasHit(pvalue=-1.0 * pval, item=rec)
            th_heap += ghit

        MOD._process_top_hits(
            top_hits_heap=th_heap, header=test_records[0], top_hits_ofh=out_top_hits
        )
        csv_records = [i for i in out_top_hits.getvalue().split("\n") if i]
        self.assertEqual(4, len(csv_records))
        self.assertEqual(f"key,{STATS_COLUMN_PVAL}", csv_records[0].rstrip())
        self.assertEqual("1,5e-10", csv_records[1].rstrip())
        self.assertEqual("2,5e-8", csv_records[2].rstrip())
        self.assertEqual("3,0.01", csv_records[3].rstrip())

    def test__process_top_hits_spa(self):
        test_records = self.generate_test_records(pval_col=STATS_COLUMN_SPA_PVAL)
        th_heap = TopHitsHeap(TestCurateGwasHits_process_top_hits.Nhits)
        out_top_hits = StringIO()

        for record in test_records[1:]:
            rec = dict(zip(test_records[0], record))
            pval = float(rec[STATS_COLUMN_SPA_PVAL])
            ghit = GwasHit(pvalue=-1.0 * pval, item=rec)
            th_heap += ghit

        MOD._process_top_hits(
            top_hits_heap=th_heap, header=test_records[0], top_hits_ofh=out_top_hits
        )
        csv_records = [i for i in out_top_hits.getvalue().split("\n") if i]
        self.assertEqual(4, len(csv_records))
        self.assertEqual(f"key,{STATS_COLUMN_SPA_PVAL}", csv_records[0].rstrip())
        self.assertEqual("1,5e-10", csv_records[1].rstrip())
        self.assertEqual("2,5e-8", csv_records[2].rstrip())
        self.assertEqual("3,0.01", csv_records[3].rstrip())


class _mock_args(NamedTuple):
    summary_stats_dir: str
    pvalue_cutoff: float
    top_n_hits: int
    out_prefix: str


class TestCurateGwasHits_main(unittest.TestCase):
    def generate_test_csvs(self, pval_col=STATS_COLUMN_PVAL):
        odir = tempfile.mkdtemp()
        ofils = []
        header = ['key', pval_col]

        for i in range(5):
            (_, opath) = tempfile.mkstemp(dir=odir, text=True, suffix=".csv.gz")
            ofils.append(opath)
            with gzip.open(opath, 'wt') as o:
                writer = csv.writer(o)
                writer.writerow(header)
                if i == 3:
                    writer.writerow([f"{i}.0", "0.01"])
                    writer.writerow([f"{i}.1", "5e-10"])
                    writer.writerow([f"{i}.2", "5e-8"])
                else:
                    for j in range(3):
                        row = [f"{i}.{j}", str(random.uniform(1e-5, 0.05))]
                        writer.writerow(row)
        return odir, ofils

    def test_main(self):
        odir = tempfile.mkdtemp()
        input_dir, input_csvs = self.generate_test_csvs()
        args = _mock_args(
            summary_stats_dir=input_dir,
            pvalue_cutoff=1e-5,
            top_n_hits=3,
            out_prefix=os.path.join(odir, 'test'),
        )

        try:
            MOD.main(args)
            out_files = glob.glob(os.path.join(odir, 'test*'))
            self.assertEqual(2, len(out_files))
        finally:
            out_files = glob.glob(os.path.join(odir, 'test*'))
            cleanup_files(out_files + input_csvs)

    def test_main_spa(self):
        odir = tempfile.mkdtemp()
        input_dir, input_csvs = self.generate_test_csvs(pval_col=STATS_COLUMN_SPA_PVAL)
        args = _mock_args(
            summary_stats_dir=input_dir,
            pvalue_cutoff=1e-5,
            top_n_hits=3,
            out_prefix=os.path.join(odir, 'test'),
        )

        try:
            MOD.main(args)
            out_files = glob.glob(os.path.join(odir, 'test*'))
            self.assertEqual(2, len(out_files))
        finally:
            out_files = glob.glob(os.path.join(odir, 'test*'))
            cleanup_files(out_files + input_csvs)

    def test_main_raise_exception(self):
        args = _mock_args(
            summary_stats_dir="/fake/path",
            pvalue_cutoff=1e-5,
            top_n_hits=3,
            out_prefix="test",
        )

        with self.assertRaises(RuntimeError) as _:
            MOD.main(args)
