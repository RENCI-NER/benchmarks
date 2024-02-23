#!/usr/bin/python3

#
# comparator/cli.py - Provide a command-line interface to the comparator.
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
