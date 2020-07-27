# -*- coding: utf-8 -*-
"""Simple wrapper for suggestions from Google Knowledge Graph."""

import os

from googleapiclient.discovery import build


class Replacer(object):
    """Simple wrapper for suggestions from Google Knowledge Graph."""

    def __init__(self, google_api_key: str, min_result_score: int = 1000):
        """Create an instance of the Replacer class.

        Parameters
        ----------
        google_api_key : str
            API Key for Google Knowledge Graph.
            https://developers.google.com/knowledge-graph/prereqs
        min_result_score : int
            Minimum result score the response from the Google Knowledge Graph
            has to have.

        Raises
        ------
        TypeError
            If `google_api_key` is empty and env variable `GKG_API_KEY` is not
            set or empty.
        """
        api_key_env = os.environ.get('GKG_API_KEY')
        effective_api_key = google_api_key if google_api_key else api_key_env
        if not effective_api_key:
            raise TypeError('No API key provided for Google Knowledge Graph!')
        self.kg_search = build(
            'kgsearch', 'v1', developerKey=effective_api_key,
        )
        self.min_result_score = min_result_score

    def suggest(self, query: str) -> str:
        """Suggest a value from the Google Knowledge Graph.

        Parameters
        ----------
        query : str
            Query string for the Google Knowledge Graph.

        Returns
        -------
        str
            Result from Google Knowledge Graph if different from query and
            fulfilling minimum result score.

        """
        response = self.kg_search.entities().search(
            query=query,
            limit=1,
        ).execute()
        if response.get('itemListElement', []):
            result_score = response['itemListElement'][0].get('resultScore', 0)
            if result_score > self.min_result_score:
                return response['itemListElement'][0].get('result', {}).get(
                    'name',
                )
        return query
