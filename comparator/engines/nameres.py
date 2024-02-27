import os
import logging

import requests

from comparator.engines.base import BaseNEREngine

# Configuration: NameRes
NAMERES_ENDPOINT = os.getenv('NAMERES_ENDPOINT', 'https://name-resolution-sri-dev.apps.renci.org/lookup')

# Configuration: the `/get_normalized_nodes` endpoint on a Node Normalization instance to use.
NODE_NORM_ENDPOINT = os.getenv('NODE_NORM_ENDPOINT', 'https://nodenormalization-sri.renci.org/get_normalized_nodes')


class NameResNEREngine(BaseNEREngine):
    def annotate(self, text, props, limit=1):
        annotations = []

        biolink_type = props.get('biolink_type', '')

        skip_umls = False
        if props.get('skip_umls', ''):
            skip_umls = True

        # Make a request to Nemo-Serve.
        nameres_options = {
            'autocomplete': 'false',
            'offset': 0,
            'limit': limit,
            'string': text,
            'biolink_type': biolink_type,
        }

        if skip_umls:
            nameres_options['exclude_prefixes'] = 'UMLS'


        response = requests.get(NAMERES_ENDPOINT, params=nameres_options)
        logging.debug(f"Response from NameRes: {response.content}")
        if not response.ok:
            raise RuntimeError(f"Could not contact NameRes: {response}")

        results = response.json()
        annotations = []

        for result in results:
            biolink_type = 'biolink:NamedThing'
            biolink_types = result.get('types', [])
            if len(biolink_types) > 0:
                biolink_type = biolink_types[0]

            annotation = {
                'text': text,
                'span': {
                    'begin': 0,
                    'end': len(text)
                },
                'id': result.get('curie', ''),
                'label': result.get('label', ''),
                'biolink_type': biolink_type,
                'score': result.get('score', ''),
                'props': {
                    'clique_identifier_count': result.get('clique_identifier_count', ''),
                }
            }

            annotations.append(annotation)

        return annotations