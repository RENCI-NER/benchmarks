#!/usr/bin/python3

#
# comparator.py - Compare results from multiple NER tools.
#

import logging

import click

# Set up basic logging.
logging.basicConfig(level=logging.INFO)

@click.command()
def comparator():
    print("Comparator CLI")

if __name__ == '__main__':
    comparator()
