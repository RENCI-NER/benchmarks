#!/usr/bin/python3

#
# comparator/cli.py - Provide a command-line interface to the comparator.
#

import logging
import csv
import time

import click
import requests

from requests.adapters import HTTPAdapter, Retry

from comparator.engines.nameres import NameResNEREngine
from comparator.engines.sapbert import SAPBERTNEREngine

# Configuration
SLEEP_BETWEEN_ROWS = 2

# Set up basic logging.
logging.basicConfig(level=logging.INFO)


@click.command(name="comparator")
@click.argument('input-file', type=click.File('r'), default='-')
@click.option('--output', type=click.File('w'), default='-', help='The file where results should be written as a CSV.')
@click.option('--query', type=str, default='query', help='The CSV field containing the query text.')
@click.option('--biolink-type', type=str, default='biolink_type', help='The CSV field containing the Biolink type of the query.')
@click.option('engines', '--engine', '-e', type=str, multiple=True, help='The engines to compare')
@click.option('--csv-dialect', type=click.Choice(csv.list_dialects(), case_sensitive=False),
              help='The CSV dialect to use (see the Python `csv` module for options).')
@click.option('continue_filename', '--continue', type=click.Path(exists=True, readable=True),
              help='An output file which we can continue working on by loading already completed work.')
def comparator(input_file, output, query, biolink_type, engines, csv_dialect, continue_filename):
    """
    Run one or more NERs on an input file and produce comparative results.

    Arguments:

    INPUT_FILE: the file to process. Use '-' to use the standard input stream.
    """

    # Read the continue filename. We need to read this and then close the file, because it is probably the
    # same file as the output file!
    result_cache = dict()
    with open(click.format_filename(continue_filename), 'r') as continuef:
        continue_file_reader = csv.DictReader(continuef, dialect=csv_dialect)
        for row in continue_file_reader:
            if query not in row:
                # This is the cache, anyway. Ignore.
                continue
            result_cache[row[query]] = row

    # Set up a repeatable session.
    session = requests.Session()
    retries = Retry(total=5,
                    backoff_factor=0.1,
                    status_forcelist=[ 500, 502, 503, 504, 403 ]
                    )
    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))

    # Set up engines.
    # TODO

    # Read the CSV file input_file with the CSV DictReader.
    reader = csv.DictReader(input_file, dialect=csv_dialect)
    header = list(reader.fieldnames)

    for engine in engines:
        # TODO: add columns for results from each engine.
        pass

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
