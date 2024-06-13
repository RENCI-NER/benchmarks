#!/usr/bin/python3

#
# benchmark/cli.py - Provide a command-line interface to the NER benchmark tool.
#

import logging
import csv
import time

import click
import requests

from requests.adapters import HTTPAdapter, Retry

from benchmark.google_sheet_benchmarks import GoogleSheetBenchmarks
from engines.nameres import NameResNEREngine
from engines.sapbert import SAPBERTNEREngine

# CONFIGURATION
# - The Google Sheet ID containing Benchmarking information.
BENCHMARK_GOOGLE_SHEET_ID = '10gtARdmoOGSZBObMtuC__wmEQYjoDhkbJLb78cJK-QE'
SLEEP_BETWEEN_ROWS = 2

# Set up basic logging.
logging.basicConfig(level=logging.INFO)


@click.command(name="benchmark")
@click.option('--output', type=click.File('w'), default='-', help='The file where results should be written as a CSV.')
@click.option('--google-sheet-id', default=BENCHMARK_GOOGLE_SHEET_ID, help='The Google Sheet ID containing the ')
@click.option('engines', '--engine', '-e', type=str, multiple=True, help='The engines to benchmark')
def benchmark(output, google_sheet_id, engines):
    """
    Run one or more NERs on our standard benchmark results and produce benchmarking results.
    """

    # Set up a repeatable session.
    session = requests.Session()
    retries = Retry(total=5,
                    backoff_factor=0.1,
                    status_forcelist=[ 500, 502, 503, 504, 403 ]
                    )
    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))

    # Set up GoogleSheetBenchmarks.
    gsm = GoogleSheetBenchmarks(google_sheet_id=google_sheet_id)
    logging.info(f"Loaded {gsm}")
    benchmarks = gsm.benchmarks()

    if len(benchmarks) > 0:
        exit(0)

    # Set up engines.
    # TODO

    # Set up engine.
    nameres = NameResNEREngine(session)
    header.extend(['nameres_id', 'nameres_label', 'nameres_type', 'nameres_score', 'nameres_time_sec'])

    sapbert = SAPBERTNEREngine(session)
    header.extend(['sapbert_id', 'sapbert_label', 'sapbert_type', 'sapbert_score', 'sapbert_time_sec'])

    csv_writer = csv.DictWriter(output, fieldnames=header)
    csv_writer.writeheader()

    for row in reader:
        if query not in row:
            logging.error(f"Query field '{query}' not found in CSV row: {row}")
            continue
        text = row[query]

        # Do we have this text cached?
        if text in result_cache:
            csv_writer.writerow(result_cache[text])
            continue

        if biolink_type not in row:
            logging.warning(f"Type field '{biolink_type}' not found in CSV row: {row}")
            continue
        text_type = row.get(biolink_type, '')
        if text_type.strip().lower() in {'na', 'none', 'entity', 'biolink:entity', 'namedthing', 'biolink:namedthing'}:
            text_type = ''

        # Get top NameRes result.
        nameres_results = []
        nameres_start = time.time_ns()
        try:
            nameres_results = nameres.annotate(text, {
                'biolink_type': text_type,
            }, limit=10)
        except Exception as inst:
            logging.error(f"Could not look up {text}: {inst}")

        if len(nameres_results) > 0:
            row['nameres_id'] = nameres_results[0]['id']
            row['nameres_label'] = nameres_results[0]['label']
            row['nameres_type'] = nameres_results[0]['biolink_type']
            row['nameres_score'] = nameres_results[0]['score']
        row['nameres_time_sec'] = f"{float(time.time_ns() - nameres_start)/1000_000_000:.5f}"
        logging.info(f"Found NameRes results for '{text}' in {row['nameres_time_sec']} seconds: {nameres_results}")

        # Get top SAPBERT result.
        sapbert_results = []
        sapbert_start = time.time_ns()
        try:
            sapbert_results = sapbert.annotate(text, {
                'biolink_type': text_type,
            }, limit=10)
        except Exception as inst:
            logging.error(f"Could not look up {text}: {inst}")

        if len(sapbert_results) > 0:
            row['sapbert_id'] = sapbert_results[0]['id']
            row['sapbert_label'] = sapbert_results[0]['label']
            row['sapbert_type'] = sapbert_results[0]['biolink_type']
            row['sapbert_score'] = sapbert_results[0]['score']
        row['sapbert_time_sec'] = f"{float(time.time_ns() - sapbert_start)/1000_000_000:.5f}"
        logging.info(f"Found SAPBERT results for '{text}' in {row['sapbert_time_sec']} seconds: {sapbert_results}")

        csv_writer.writerow(row)

        # Add a sleep to make sure we don't overload Sterling's ingresses.
        time.sleep(SLEEP_BETWEEN_ROWS)


if __name__ == '__main__':
    comparator()
