"""This modules tests `vadc_gwas_tools.common.p_hits_heap.TopHitsHeap` class."""
import heapq
import os
import unittest

from vadc_gwas_tools.common.top_hits_heap import GwasHit
from vadc_gwas_tools.common.top_hits_heap import TopHitsHeap as MOD


class TestTopHitsHeap(unittest.TestCase):
    def test_init(self):
        obj = MOD()
        self.assertEqual(obj.n_hits, 100)

    def test__set_min_max(self):
        obj = MOD()
        obj._items = [-1, -10, -30, -2]
        heapq.heapify(obj._items)

        obj._set_min_max()
        self.assertEqual(obj._min, -30)
        self.assertEqual(obj._max, -1)

    def test_collecting(self):
        pval_list = [0.5, 100.0, 1.0, 0.23, 0.5]
        records = [GwasHit(pvalue=-1.0 * i, item={'a': 'b'}) for i in pval_list]
        obj = MOD(n_hits=3)

        obj += records[0]
        self.assertEqual(obj._min, None)
        self.assertEqual(obj._max, None)
        self.assertEqual(obj._items, [records[0]])
        self.assertFalse(obj._filled)

        obj += records[1]
        self.assertEqual(obj._min, None)
        self.assertEqual(obj._max, None)
        self.assertEqual(obj._items[0], records[1])
        self.assertFalse(obj._filled)

        obj += records[2]
        self.assertEqual(obj._min, None)
        self.assertEqual(obj._max, None)
        self.assertEqual(obj._items[0], records[1])
        self.assertFalse(obj._filled)

        obj += records[3]
        self.assertEqual(obj._min, records[2])
        self.assertEqual(obj._max, records[3])
        self.assertEqual(obj._items[0], records[2])
        self.assertTrue(obj._filled)

        obj += records[4]
        self.assertEqual(obj._min, records[0])
        self.assertEqual(obj._max, records[3])
        self.assertEqual(obj._items, [records[0], records[4], records[3]])
        self.assertTrue(obj._filled)
