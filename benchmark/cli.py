#!/usr/bin/python3

#
# benchmark/cli.py - Provide a command-line interface to the NER benchmark tool.
#

import logging
import csv
import time
from collections import defaultdict

import click
import requests

from requests.adapters import HTTPAdapter, Retry

from benchmark.google_sheet_benchmarks import GoogleSheetBenchmarks
from engines.nameres import NameResNEREngine
from engines.sapbert import SAPBERTNEREngine

# CONFIGURATION
# - The Google Sheet ID containing Benchmarking information.
BENCHMARK_GOOGLE_SHEET_ID = '10gtARdmoOGSZBObMtuC__wmEQYjoDhkbJLb78cJK-QE'
RESULTS_TO_DOWNLOAD = 10
SLEEP_BETWEEN_ROWS = 2

# Set up basic logging.
logging.basicConfig(level=logging.INFO)


# Helper methods
def rank_str_in_list(query, items):
    """ Finds a string in a list, but returns 'NA' if it can't be found. """
    logging.info(f"Searching for {query} in {items}")
    try:
        return items.index(query) + 1  # Start with a rank of 1 instead of an index of 0
    except ValueError:
        return None


def divide_to_str(n, d):
    if d == 0:
        return f"NA ({n}/{d})"
    return f"{float(n)/d:.3f} ({n}/{d})"


@click.command(name="benchmark")
@click.option('--output', type=click.File('w'), default='-', help='The file where results should be written as a CSV.')
@click.option('--google-sheet-id', default=BENCHMARK_GOOGLE_SHEET_ID, help='The Google Sheet ID containing the ')
@click.option('--skip-types', is_flag=True, default=False, help='Do not use input types when querying terms')
@click.option('engines', '--engine', '-e', type=str, multiple=True, help='The engines to benchmark')
def benchmark(output, google_sheet_id, skip_types, engines):
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

    header = ['query', 'source', 'provided_type', 'queried_type', 'correct_type', 'correct_id', 'correct_label', 'notes']

    # Set up engines.
    # TODO

    nameres = NameResNEREngine(session)
    header.extend(['nameres_id_rank', 'nameres_label_rank', 'nameres_time_sec',
                   'nameres_no_type_id_rank', 'nameres_no_type_label_rank', 'nameres_no_type_time_sec'])

    sapbert = SAPBERTNEREngine(session)
    header.extend(['sapbert_id_rank', 'sapbert_label_rank', 'sapbert_time_sec',
                   'sapbert_no_type_id_rank', 'sapbert_no_type_label_rank', 'sapbert_no_type_time_sec'])

    # Stats.
    stats = defaultdict(int)

    # Prepare CSV file.
    csv_writer = csv.DictWriter(output, fieldnames=header)
    csv_writer.writeheader()

    for benchm in benchmarks:
        if benchm.Query == '':
            logging.error(f"Query not found in benchmark: {benchm}")
            continue
        text = benchm.Query

        if benchm.CorrectID == '' and benchm.CorrectLabel == '':
            logging.error(f"No correct ID or label for benchmark, skipping: {benchm}")
            continue

        text_type = benchm.Type
        if text_type.strip().lower() in {'na', 'none', 'entity', 'biolink:entity', 'namedthing', 'biolink:namedthing'}:
            text_type = ''

        if skip_types:
            text_type = ''

        # Record notes.
        notes = []
        for note_field in benchm.Notes:
            if benchm.Notes[note_field]:
                notes.append(f"{note_field}: {benchm.Notes[note_field]}")

        # Start a result row.
        row = {
            'query': benchm.Query,
            'source': benchm.Source,
            'provided_type': benchm.Type,
            'queried_type': text_type,
            'correct_type': benchm.CorrectType,
            'correct_id': benchm.CorrectID,
            'correct_label': benchm.CorrectLabel,
            'notes': ';'.join(notes)
        }

        # Get top NameRes result with type.
        nameres_results = []
        nameres_start = time.time_ns()
        try:
            nameres_results = nameres.annotate(text, {
                'biolink_type': text_type,
            }, limit=RESULTS_TO_DOWNLOAD)
        except Exception as inst:
            logging.error(f"Could not look up {text} in NameRes: {inst}")

        row['nameres_time_sec'] = f"{float(time.time_ns() - nameres_start)/1000_000_000:.5f}"

        # Did we get the expected ID in the top 10 results?
        result_ids = list(map(lambda nr: nr['id'], nameres_results))
        row['nameres_id_rank'] = rank_str_in_list(benchm.CorrectID, result_ids)
        if row['nameres_id_rank']:
            stats['nameres_total_id_rank'] += row['nameres_id_rank']
            stats['nameres_count_id_rank'] += 1

        result_labels = list(map(lambda nr: nr.get('label', '').lower(), nameres_results))
        row['nameres_label_rank'] = rank_str_in_list(benchm.CorrectLabel.lower(), result_labels)
        if row['nameres_label_rank']:
            stats['nameres_total_label_rank'] += row['nameres_label_rank']
            stats['nameres_count_label_rank'] += 1

        logging.info(f"Found NameRes results for '{text}' (type {text_type}) in {row['nameres_time_sec']} seconds: {nameres_results}")

        # Get top NameRes result without type.
        nameres_results = []
        nameres_start = time.time_ns()
        try:
            nameres_results = nameres.annotate(text, {}, limit=RESULTS_TO_DOWNLOAD)
        except Exception as inst:
            logging.error(f"Could not look up {text} in NameRes: {inst}")

        row['nameres_no_type_time_sec'] = f"{float(time.time_ns() - nameres_start)/1000_000_000:.5f}"

        # Did we get the expected ID in the top 10 results?
        result_ids = list(map(lambda nr: nr['id'], nameres_results))
        row['nameres_no_type_id_rank'] = rank_str_in_list(benchm.CorrectID, result_ids)
        if row['nameres_no_type_id_rank']:
            stats['nameres_no_type_total_id_rank'] += row['nameres_no_type_id_rank']
            stats['nameres_no_type_count_id_rank'] += 1

        result_labels = list(map(lambda nr: nr.get('label', '').lower(), nameres_results))
        row['nameres_no_type_label_rank'] = rank_str_in_list(benchm.CorrectLabel.lower(), result_labels)
        if row['nameres_no_type_label_rank']:
            stats['nameres_no_type_total_label_rank'] += row['nameres_no_type_label_rank']
            stats['nameres_no_type_count_label_rank'] += 1

        logging.info(f"Found NameRes results for '{text}' (without type) in {row['nameres_no_type_time_sec']} seconds: {nameres_results}")


        # Get top SAPBERT result.
        sapbert_results = []
        sapbert_start = time.time_ns()
        try:
            sapbert_results = sapbert.annotate(text, {
                'biolink_type': text_type,
            }, limit=RESULTS_TO_DOWNLOAD)
        except Exception as inst:
            logging.error(f"Could not look up {text} in SAPBERT: {inst}")

        row['sapbert_time_sec'] = f"{float(time.time_ns() - sapbert_start)/1000_000_000:.5f}"

        # Did we get the expected ID in the top 10 results?
        result_ids = list(map(lambda nr: nr['id'], sapbert_results))
        row['sapbert_id_rank'] = rank_str_in_list(benchm.CorrectID, result_ids)
        if row['sapbert_id_rank']:
            stats['sapbert_total_id_rank'] += row['sapbert_id_rank']
            stats['sapbert_count_id_rank'] += 1

        result_labels = list(map(lambda nr: nr.get('label', '').lower(), sapbert_results))
        row['sapbert_label_rank'] = rank_str_in_list(benchm.CorrectLabel.lower(), result_labels)
        if row['sapbert_label_rank']:
            stats['sapbert_total_label_rank'] += row['sapbert_label_rank']
            stats['sapbert_count_label_rank'] += 1

        logging.info(f"Found SAPBERT results for '{text}' (type {text_type}) in {row['sapbert_time_sec']} seconds: {nameres_results}")

        # Get top SAPBERT result.
        sapbert_results = []
        sapbert_start = time.time_ns()
        try:
            sapbert_results = sapbert.annotate(text, {}, limit=RESULTS_TO_DOWNLOAD)
        except Exception as inst:
            logging.error(f"Could not look up {text} in SAPBERT: {inst}")

        row['sapbert_no_type_time_sec'] = f"{float(time.time_ns() - sapbert_start)/1000_000_000:.5f}"

        # Did we get the expected ID in the top 10 results?
        result_ids = list(map(lambda nr: nr['id'], sapbert_results))
        row['sapbert_no_type_id_rank'] = rank_str_in_list(benchm.CorrectID, result_ids)
        if row['sapbert_no_type_id_rank']:
            stats['sapbert_no_type_total_id_rank'] += row['sapbert_no_type_id_rank']
            stats['sapbert_no_type_count_id_rank'] += 1

        result_labels = list(map(lambda nr: nr.get('label', '').lower(), sapbert_results))
        row['sapbert_no_type_label_rank'] = rank_str_in_list(benchm.CorrectLabel.lower(), result_labels)
        if row['sapbert_no_type_label_rank']:
            stats['sapbert_no_type_total_label_rank'] += row['sapbert_no_type_label_rank']
            stats['sapbert_no_type_count_label_rank'] += 1

        logging.info(f"Found SAPBERT results for '{text}' (type {text_type}) in {row['sapbert_time_sec']} seconds: {nameres_results}")

        csv_writer.writerow(row)

        # Add a sleep to make sure we don't overload Sterling's ingresses.
        time.sleep(SLEEP_BETWEEN_ROWS)

    # Write out statistics.
    print("Ranked ID results:")
    print(f"\tNameRes: {divide_to_str(stats['nameres_count_id_rank'], len(benchmarks))}")
    print(f"\tNameRes without type: {divide_to_str(stats['nameres_no_type_count_id_rank'], len(benchmarks))}")
    print(f"\tSAPBERT: {divide_to_str(stats['sapbert_count_id_rank'], len(benchmarks))}")
    print(f"\tSAPBERT without type: {divide_to_str(stats['sapbert_no_type_count_id_rank'], len(benchmarks))}")
    print("Ranked label results:")
    print(f"\tNameRes: {divide_to_str(stats['nameres_count_label_rank'], len(benchmarks))}")
    print(f"\tNameRes without type: {divide_to_str(stats['nameres_no_type_count_label_rank'], len(benchmarks))}")
    print(f"\tSAPBERT: {divide_to_str(stats['sapbert_count_label_rank'], len(benchmarks))}")
    print(f"\tSAPBERT without type: {divide_to_str(stats['sapbert_no_type_count_label_rank'], len(benchmarks))}")
    print("Average ID rank:")
    print(f"\tNameRes: {divide_to_str(stats['nameres_total_id_rank'], stats['nameres_count_id_rank'])}")
    print(f"\tNameRes without type: {divide_to_str(stats['nameres_no_type_total_id_rank'], stats['nameres_no_type_count_id_rank'])}")
    print(f"\tSAPBERT: {divide_to_str(stats['nameres_total_id_rank'], stats['nameres_count_id_rank'])}")
    print(f"\tSAPBERT without type: {divide_to_str(stats['nameres_no_type_total_id_rank'], stats['nameres_no_type_count_id_rank'])}")
    print("Average label rank:")
    print(f"\tNameRes: {divide_to_str(stats['nameres_total_label_rank'], stats['nameres_count_label_rank'])}")
    print(f"\tNameRes without type: {divide_to_str(stats['nameres_no_type_total_label_rank'], stats['nameres_no_type_count_label_rank'])}")
    print(f"\tSAPBERT: {divide_to_str(stats['nameres_total_label_rank'], stats['nameres_count_label_rank'])}")
    print(f"\tSAPBERT without type: {divide_to_str(stats['nameres_no_type_total_label_rank'], stats['nameres_no_type_count_label_rank'])}")


if __name__ == '__main__':
    benchmark()
