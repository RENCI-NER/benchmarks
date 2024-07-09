#!/usr/bin/python3

#
# comparator/cli.py - Provide a command-line interface to the comparator.
#

import logging
import csv
import os.path
import time

import click
import requests

from requests.adapters import HTTPAdapter, Retry

from engines.nameres import NameResNEREngine
from engines.sapbert import SAPBERTNEREngine

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
@click.option('continue_filename', '--continue', type=click.Path(),
              help='An output file which we can continue working on by loading already completed work.')
def comparator(input_file, output, query, biolink_type, engines, csv_dialect, continue_filename):
    """
    Run one or more NERs on an input file and produce comparative results.

    Arguments:

    INPUT_FILE: the file to process. Use '-' to use the standard input stream.
    """

    # TODO: move this into its own file and make it into a command line option.
    llm_type_mappings = {
        "Anatomical Entity": "AnatomicalEntity",
        "anatomical entity": "AnatomicalEntity",
        "Anatomical Entity/Biological Process": "AnatomicalEntity",
        "Anatomical entity": "AnatomicalEntity",
        "Biological Process": "BiologicalProcess",
        "Biological entity": "BiologicalEntity",
        "Biological process": "BiologicalProcess",
        "biological process": "BiologicalProcess",
        "Cell": "Cell",
        "cell": "Cell",
        "Cell line": "CellLine",
        "Chemical": "ChemicalEntity",
        "chemical": "ChemicalEntity",
        "Disease": "Disease",
        "disease": "Disease",
        "Disease/Phenotype": "DiseaseOrPhenotypicFeature",
        "Gene": "Gene",
        "gene": "Gene",
        "Gene/Protein": "GeneOrGeneProduct",
        "Protein and Gene": "GeneOrGeneProduct",
        "Protein": "Protein",
        "protein": "Protein",
        "Phenotype": "PhenotypicFeature",
        "phenotype": "PhenotypicFeature",
        "Physiological Process": "PhysiologicalProcess",
        "Physiological process": "PhysiologicalProcess",
        "physiological process": "PhysiologicalProcess",
        "cohort": "Cohort",
        "device": "Device",
        "procedure": "Procedure",
        # TODO: review these
        "Behavior, Disease": "DiseaseOrPhenotypicFeature",
        "Biological process, Phenotype, Disease": "DiseaseOrPhenotypicFeature",
        "Event, Sequence variant": "SequenceVariant",
        "Phenotype, Biological Process, Phenotype": "PhenotypicFeature",
        "Phenotype, Chemical, Protein, Protein": "PhenotypicFeature",
        "Phenotype, Protein, Biological process, Protein": "PhenotypicFeature",
        "Procedure, Chemical, Chemical, Chemical, Treatment": "ChemicalOrDrugOrTreatment",
        "Procedure, Device, Other": "Device",
        "Protein, Chemical, Biological Process": "ChemicalOrDrugOrTreatment",
        "Treatment, Event, Behavior": "ChemicalOrDrugOrTreatment",
        "Behavior": "Behavior",
        "behavior": "Behavior",
        "Behavior and Disease": "BiologicalEntity",
        "Behavior and Phenotype": "BiologicalEntity",
        "Biological process and Chemical": "BiologicalEntity",
        "Biological process/Disease": "BiologicalEntity",
        "Chemical and Protein": "Protein",
        "Chemical/Treatment": "ChemicalOrDrugOrTreatment",
        "Cognitive Process": "BiologicalProcess",
        "Cohort": "Cohort",
        "Device": "Device",
        "Device and Protein": "Protein",
        "Device/Procedure": "Procedure",
        "Disease and Phenotype": "DiseaseOrPhenotypicFeature",
        "Disease and Physiological Process": "BiologicalProcess",
        "Disease/Behavior": "BiologicalEntity",
        "Enzyme": "Protein",
        "Event": "Event",
        "event": "Event",
        "Event and Behavior": "Event",
        "Food": "ChemicalEntity",
        "Food and Chemical": "ChemicalEntity",
        "Food/Behavior": "ChemicalEntity",
        "Pathway": "Pathway",
        "Procedure": "Procedure",
        "Time": "Event",
        "treatment": "Treatment",
        "Treatment": "Treatment",
        "Sequence Variant": "SequenceVariant",
        "Sequence Variant/Mutation": "SequenceVariant",
        "Mutation": "SequenceVariant",
        "mutation": "SequenceVariant",
        "Sequence variant": "SequenceVariant",
        "sequence variant": "SequenceVariant",
        "Measurement": "ClinicalMeasurement",
        "Organism": "OrganismTaxon",
        "organism": "OrganismTaxon",
        "Organization": "Agent",
        "Phenomenon": "Phenomenon",
        "Procedure/Behavior": "ActivityAndBehavior",
        "mRNA": "GeneProductMixin",
        "phenomenon": "Phenomenon",
        # Shrugs
        "other": "",
        "Other": "",
        "Other/Disease": "Disease",
        "Location": "",
        "Device and Cohort": "",
    }

    # Read the continue filename. We need to read this and then close the file, because it is probably the
    # same file as the output file!
    result_cache = dict()
    if continue_filename and os.path.exists(click.format_filename(continue_filename)):
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
    header.extend(['nameres_id', 'nameres_label', 'nameres_type', 'nameres_score', 'nameres_time_sec',
                   'nameres_no_type_id', 'nameres_no_type_label', 'nameres_no_type_type', 'nameres_no_type_score',
                   'nameres_no_type_time_sec'])

    sapbert = SAPBERTNEREngine(session)
    header.extend(['sapbert_id', 'sapbert_label', 'sapbert_type', 'sapbert_score', 'sapbert_time_sec',
                   'sapbert_no_type_id', 'sapbert_no_type_label', 'sapbert_no_type_type', 'sapbert_no_type_score',
                   'sapbert_no_type_time_sec'])

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
        if text_type not in llm_type_mappings:
            logging.warning(f"No mapping available for '{text_type}', using '' instead.")
        text_type = llm_type_mappings.get(text_type, '')

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
        logging.info(f"Found NameRes results for '{text}' with type {text_type} in {row['nameres_time_sec']} seconds: {nameres_results}")

        # Get top NameRes result without type.
        nameres_results = []
        nameres_start = time.time_ns()
        try:
            nameres_results = nameres.annotate(text, {
                # 'biolink_type': text_type,
            }, limit=10)
        except Exception as inst:
            logging.error(f"Could not look up {text}: {inst}")

        if len(nameres_results) > 0:
            row['nameres_no_type_id'] = nameres_results[0]['id']
            row['nameres_no_type_label'] = nameres_results[0]['label']
            row['nameres_no_type_type'] = nameres_results[0]['biolink_type']
            row['nameres_no_type_score'] = nameres_results[0]['score']
        row['nameres_no_type_time_sec'] = f"{float(time.time_ns() - nameres_start)/1000_000_000:.5f}"
        logging.info(f"Found NameRes results for '{text}' without type in {row['nameres_no_type_time_sec']} seconds: {nameres_results}")

        # Get top SAPBERT result without type
        sapbert_results = []
        sapbert_start = time.time_ns()
        try:
            sapbert_results = sapbert.annotate(text, {
                # 'biolink_type': text_type,
            }, limit=10)
        except Exception as inst:
            logging.error(f"Could not look up {text}: {inst}")

        if len(sapbert_results) > 0:
            row['sapbert_no_type_id'] = sapbert_results[0]['id']
            row['sapbert_no_type_label'] = sapbert_results[0]['label']
            row['sapbert_no_type_type'] = sapbert_results[0]['biolink_type']
            row['sapbert_no_type_score'] = sapbert_results[0]['score']
        row['sapbert_no_type_time_sec'] = f"{float(time.time_ns() - sapbert_start)/1000_000_000:.5f}"
        logging.info(f"Found SAPBERT results for '{text}' without type in {row['sapbert_no_type_time_sec']} seconds: {sapbert_results}")

        # Get top SAPBERT result with type.
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
        logging.info(f"Found SAPBERT results for '{text}' with type '{text_type}' in {row['sapbert_time_sec']} seconds: {sapbert_results}")

        csv_writer.writerow(row)

        # Add a sleep to make sure we don't overload Sterling's ingresses.
        time.sleep(SLEEP_BETWEEN_ROWS)


if __name__ == '__main__':
    comparator()
