#!/usr/bin/python3

#
# comparator/cli.py - Provide a command-line interface to the comparator.
#

import logging
import csv

import click

# Set up basic logging.
logging.basicConfig(level=logging.INFO)


@click.command(name="comparator")
@click.argument('input-file', type=click.File('r'), default='-')
@click.option('--query', type=str, default='query', help='The CSV field containing the query text.')
@click.option('--biolink-type', type=str, default='biolink_type', help='The CSV field containing the Biolink type of the query.')
@click.option('engines', '--engine', '-e', type=str, multiple=True, help='The engines to compare')
@click.option('--csv-dialect', type=click.Choice(csv.list_dialects(), case_sensitive=False),
              help='The CSV dialect to use (see the Python `csv` module for options).')
def comparator(input_file, query, biolink_type, engines, csv_dialect):
    """
    Run one or more NERs on an input file and produce comparative results.

    Arguments:

    INPUT_FILE: the file to process. Use '-' to use the standard input stream.
    """

    # Set up engines.
    # TODO

    # Read the CSV file input_file with the CSV DictReader.
    reader = csv.DictReader(input_file, dialect=csv_dialect)
    for row in reader:
        if query not in row:
            logging.error(f"Query field '{query}' not found in CSV row: {row}")
            continue
        text = row[query]

        if biolink_type not in row:
            logging.warning(f"Type field '{biolink_type}' not found in CSV row: {row}")
            continue
        text_type = row.get(biolink_type, '')

        print(f"Querying '{text}' with type {text_type}.")


if __name__ == '__main__':
    comparator()