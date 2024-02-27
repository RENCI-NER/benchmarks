#!/usr/bin/python3

#
# comparator/cli.py - Provide a command-line interface to the comparator.
#

import logging
import csv

import click
from comparator.engines.nameres import NameResNEREngine
from comparator.engines.sapbert import SAPBERTNEREngine

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
def comparator(input_file, output, query, biolink_type, engines, csv_dialect):
    """
    Run one or more NERs on an input file and produce comparative results.

    Arguments:

    INPUT_FILE: the file to process. Use '-' to use the standard input stream.
    """

    # Set up engines.
    # TODO

    # Read the CSV file input_file with the CSV DictReader.
    reader = csv.DictReader(input_file, dialect=csv_dialect)
    header = list(reader.fieldnames)

    for engine in engines:
        # TODO: add columns for results from each engine.
        pass

    # Set up engine.
    nameres = NameResNEREngine()
    header.extend(['nameres_id', 'nameres_label', 'nameres_type', 'nameres_score'])

    sapbert = SAPBERTNEREngine()
    header.extend(['sapbert_id', 'sapbert_label', 'sapbert_type', 'sapbert_score'])

    csv_writer = csv.DictWriter(output, fieldnames=header)
    csv_writer.writeheader()

    for row in reader:
        if query not in row:
            logging.error(f"Query field '{query}' not found in CSV row: {row}")
            continue
        text = row[query]

        if biolink_type not in row:
            logging.warning(f"Type field '{biolink_type}' not found in CSV row: {row}")
            continue
        text_type = row.get(biolink_type, '')

        # Get top NameRes result.
        nameres_results = nameres.annotate(text, {
            'biolink_type': text_type,
        }, limit=10)
        logging.info(f"Found NameRes results for '{text}': {nameres_results}")

        if len(nameres_results) > 0:
            row['nameres_id'] = nameres_results[0]['id']
            row['nameres_label'] = nameres_results[0]['label']
            row['nameres_type'] = nameres_results[0]['biolink_type']
            row['nameres_score'] = nameres_results[0]['score']

        # Get top SAPBERT result.
        sapbert_results = sapbert.annotate(text, {
            'biolink_type': text_type,
        }, limit=10)
        logging.info(f"Found SAPBERT results for '{text}': {sapbert_results}")

        if len(sapbert_results) > 0:
            row['sapbert_id'] = sapbert_results[0]['id']
            row['sapbert_label'] = sapbert_results[0]['label']
            row['sapbert_type'] = sapbert_results[0]['biolink_type']
            row['sapbert_score'] = sapbert_results[0]['score']

        csv_writer.writerow(row)


if __name__ == '__main__':
    comparator()