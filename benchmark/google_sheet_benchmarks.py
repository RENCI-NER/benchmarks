# We store NER benchmarks in Google Sheets at
# https://docs.google.com/spreadsheets/d/10gtARdmoOGSZBObMtuC__wmEQYjoDhkbJLb78cJK-QE/edit
#
# This library contains classes and methods for accessing those test cases.
import csv
import io
from collections import Counter

import pytest
import requests
from _pytest.mark import ParameterSet

from benchmark.model.benchmark import Benchmark


class GoogleSheetBenchmarks:
    """
    A class wrapping a Google Sheet that contains benchmarks.
    """

    def __str__(self):
        return f"Google Sheet Benchmarks ({len(self.rows)} benchmarks from {self.google_sheet_id})"

    def __init__(self, google_sheet_id="10gtARdmoOGSZBObMtuC__wmEQYjoDhkbJLb78cJK-QE"):
        """ Create a Google Sheet test case.

        :param google_sheet_id The Google Sheet identifier to download test cases from.
        """

        self.google_sheet_id = google_sheet_id
        csv_url = f"https://docs.google.com/spreadsheets/d/{google_sheet_id}/gviz/tq?tqx=out:csv&sheet=Benchmarks"
        response = requests.get(csv_url)
        self.csv_content = response.text

        self.rows = []
        with io.StringIO(self.csv_content) as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.rows.append(row)

    def benchmarks(self) -> list[Benchmark]:
        """
        self.rows is the raw list of rows we got back from the Google Sheets. This method transforms that into
        a list of Benchmarks.

        :return: A list of TestRows for the rows in this file.
        """
        def has_nonempty_value(d: dict):
            return not all(not s for s in d.values())

        trows = []
        for count, row in enumerate(self.rows):
            # Note that count is off by two: presumably one for the header row and one because we count from zero
            # but Google Sheets counts from one.

            if has_nonempty_value(row):
                benchmark = GoogleSheetBenchmarks.to_benchmark(row)

                trows.append(benchmark)

        return trows

    def categories(self):
        """ Return a dict of all the categories of tests available with their counts. """
        return Counter(map(lambda t: t.get('Category', ''), self.rows))

    @staticmethod
    def to_benchmark(row):
        """
        Create a Benchmark from a data row from a Google Sheet.

        :param row: A dictionary of column names and values as from a CSV file.
        :return: A GoogleSheetBenchmark containing the information from the provided row.
        """

        # Extract all the
        notes = dict()
        for key in row:
            if key.lower().endswith(' notes'):
                notes[key[:-6]] = row[key]

        return Benchmark(
            Query=row.get('query', ''),
            Source=row.get('source', ''),
            Type=row.get('type', ''),
            CorrectType=row.get('correct_type', ''),
            CorrectID=row.get('correct_id', ''),
            CorrectLabel=row.get('correct_label', ''),
            Notes=notes
        )
