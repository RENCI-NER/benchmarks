import os
import logging
import urllib

import requests

from comparator.engines.base import BaseNEREngine

# Configuration: get the SAPBERT URL and figure out the annotate path.
SAPBERT_URL = os.getenv('SAPBERT_URL', 'https://sap-qdrant.apps.renci.org/')
SAPBERT_ANNOTATE_ENDPOINT = urllib.parse.urljoin(SAPBERT_URL, '/annotate/')
SAPBERT_MODEL_NAME = "sapbert"
SAPBERT_COUNT = 1000  # We've found that 1000 is about the minimum you need for reasonable results.


class SAPBERTNEREngine(BaseNEREngine):
    def __init__(self, requests_session):
        """
        Create a SAPBERTNEREngine.

        :param requests_session: A Requests session to use for HTTP/HTTPS requests.
        """
        if requests_session:
            self.requests_session = requests_session
        else:
            self.requests_session = requests.Session()

    def annotate(self, text, props, limit=1):
        biolink_type = props.get('biolink_type', '')

        # SAPBERT-Qdrant requires Biolink types that start with 'biolink:'
        if biolink_type != '' and not biolink_type.startswith('biolink:'):
            biolink_type = f"biolink:{biolink_type}"

        # Make a request to Nemo-Serve.
        request = {
            "text": text,
            "model_name": SAPBERT_MODEL_NAME,
            "count": SAPBERT_COUNT,
            "args": {
                "bl_type": biolink_type,
            }
        }

        logging.debug(f"Request to {SAPBERT_MODEL_NAME}: {request}")
        response = self.requests_session.post(SAPBERT_ANNOTATE_ENDPOINT, json=request)
        logging.debug(f"Response from {SAPBERT_MODEL_NAME}: {response.content}")
        if not response.ok:
            raise RuntimeError(f"Server error from SAPBERT for text '{text}': {response}")

        results = response.json()
        annotations = []

        for result in results:
            annotation = {
                'text': text,
                'span': {
                    'begin': 0,
                    'end': len(text)
                },
                'id': result.get('curie', ''),
                'label': result.get('name', ''),
                'biolink_type': '',
                'score': result.get('score', ''),
            }

            annotations.append(annotation)

        return annotations
