import tempfile
import unittest
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from trading.core import InstrumentVersions
from trading.files import metadata_changes
from trading.reference import instruments_as_of


def write(path, columns):
    pq.write_table(pa.table(columns), path)
    return path


class InstrumentVersionsTests(unittest.TestCase):
    def test_nothing_known_before_the_first_version(self):
        versions = InstrumentVersions()
        versions.add("A", 1_000, "X1", "EUR")
        self.assertEqual(versions.as_of("A", 1_000), ("", ""))
        self.assertEqual(versions.as_of("B", 9_999), ("", ""))

    def test_latest_version_strictly_before_the_cutoff_wins(self):
        versions = InstrumentVersions()
        versions.add("A", 1_000, "X1", "EUR")
        versions.add("A", 1_000, "X2", "EUR")
        versions.add("A", 2_000, "X3", "USD")
        self.assertEqual(versions.as_of("A", 1_001), ("X2", "EUR"))
        self.assertEqual(versions.as_of("A", 2_000), ("X2", "EUR"))
        self.assertEqual(versions.as_of("A", 2_001), ("X3", "USD"))
        self.assertEqual(versions.as_of("A", 9_999), ("X3", "USD"))


class MetadataChangesTests(unittest.TestCase):
    def test_forwards_only_changes_and_carries_empty_fields_forward(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = write(Path(tmp, "events.parquet"), {
                "seq": [5, 1, 2, 3, 4, 6],
                "symbol": ["A", "A", "B", "A", "A", "B"],
                "isin": ["X2", "X1", "", "X1", "X1", "Y"],
                "currency": ["EUR", "EUR", "", "", "EUR", "USD"],
            })
            self.assertEqual(list(metadata_changes(path)), [(1, "A", "X1", "EUR"), (5, "A", "X2", "EUR"),
                                                            (6, "B", "Y", "USD")])


class ReferenceAsOfTests(unittest.TestCase):
    def test_metadata_is_stamped_with_the_feed_clock_and_joined_as_of_the_cutoff(self):
        with tempfile.TemporaryDirectory() as tmp:
            ticks = write(Path(tmp, "ticks.parquet"), {
                "seq": [1, 3, 5, 7],
                "symbol": ["A", "A", "A", "A"],
                "last": [1.0, 1.0, 1.0, 1.0],
                "event_ts": [1_000, 2_000, 9_000_000, 5_000],
                "sys_ts": [1_000, 2_000, 9_000_000 + 7_200_000, 5_000],
            })
            events = write(Path(tmp, "events.parquet"), {
                "seq": [2, 4, 6, 8],
                "symbol": ["A", "A", "B", "C"],
                "isin": ["X1", "X2", "Y", "Z"],
                "currency": ["EUR", "", "USD", "SEK"],
            })
            advisories = [("A", 1_000), ("A", 1_500), ("A", 2_001), ("B", 2_000), ("B", 2_001), ("C", 5_000),
                          ("C", 5_001)]
            found = instruments_as_of(duckdb.connect(), [ticks], events, advisories, 0, 3_600_000)
            self.assertEqual(found, {("A", 1_500): ("X1", "EUR"), ("A", 2_001): ("X2", "EUR"),
                                     ("B", 2_001): ("Y", "USD"), ("C", 5_001): ("Z", "SEK")})
            later = instruments_as_of(duckdb.connect(), [ticks], events, [("A", 600)], 500, 3_600_000)
            self.assertEqual(later, {("A", 600): ("X1", "EUR")})


if __name__ == "__main__":
    unittest.main()
