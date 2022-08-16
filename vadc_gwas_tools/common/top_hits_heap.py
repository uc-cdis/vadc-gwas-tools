"""This structure is used to collect the top N hits
of the GWAS summary statistics based on p-values.

@author: Kyle M. Hernandez <kmhernan@uchicago.edu>
"""
import heapq
from dataclasses import dataclass, field
from typing import Dict


@dataclass(order=True)
class GwasHit:
    pvalue: float
    item: Dict[str, str] = field(compare=False)


class TopHitsHeap:
    def __init__(self, n_hits: int = 100):
        self.n_hits = n_hits
        self._min = None
        self._max = None
        self._items = []
        self._filled = False

    def __iadd__(self, record: GwasHit) -> "TopHitsHeap":
        """
        Yes, I am using this in not the best way, but it
        works just fine and I see no other confusion happening.
        Since heapq is a min heap, it's easier to simply multiply
        the pvalues by -1.0 and then use it like a max heap.
        """
        # Be for we fill up to n_hits size, just append.
        if len(self._items) < self.n_hits:
            heapq.heappush(self._items, record)
            return self
        elif not self._filled:
            self._set_min_max()
            self._filled = True

        # We are at capacity, so now we do the real work

        # When the negative pvalue is less than current min, we do nothing
        if record < self._min:
            return self
        # When negative pvalue ge current max (low pval) pop smallest and add
        elif record >= self._max:
            heapq.heapreplace(self._items, record)
            self._set_min_max()
            return self
        # When negative pvalue ge current min (high pval) pop smallest and add
        elif record >= self._min:
            heapq.heapreplace(self._items, record)
            self._set_min_max()
            return self
        return self

    def _set_min_max(self) -> None:
        """Simply sets min and max values"""
        self._min = self._items[0]
        self._max = max(self._items)
