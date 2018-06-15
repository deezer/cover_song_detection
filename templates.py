# -*- coding: utf-8 -*-
"""
A set of custom query DSL templates for elasticsearch search post-json requests for various tasks

Check elasticsearch documentation for more details
https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html

Albin Andrew Correya
R&D Intern
@2017
"""

import os

assert os.environ["MSDES_HOST"]
assert os.environ["MSDES_PORT"]
assert os.environ["MSDES_INDEX"]
assert os.environ["MSDES_TYPE"]

SCHEME = "http"
URI = os.environ["MSDES_HOST"]
PORT = os.environ["MSDES_PORT"]
ES_INDEX = os.environ["MSDES_INDEX"]
ES_TYPE = os.environ["MSDES_TYPE"]


uri_config = {
    'host': URI,
    'port': PORT,
    'scheme': SCHEME,
    'index': ES_INDEX,
    'type': ES_TYPE
}

# for string search with song title
# https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html
query_string = {
    "query": {
        "bool": {
            "must": [
                {
                    "query_string": {
                        "default_field": "msd_title",
                        "query": "sample_query_here"
                    }
                }
            ],
            "must_not": [
                {
                    "query_string": {
                        "default_field": "_id",
                        "query": "msd_track_id_here"
                    }
                }
            ]
        }
    },
    "from": 0,
    "size": 100
}

# for string search with title
# https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-simple-query-string-query.html
simple_query_string = {
    "query": {
        "bool": {
            "must": [
                {
                    "simple_query_string": {
                        "fields": ["msd_title"],
                        "query": "sample_query_here"
                    }
                }
            ],
            "must_not": [
                {
                    "query_string": {
                        "default_field": "_id",
                        "query": "msd_track_id_here"
                    }
                }

            ]
        }
    },
    "from": 0,
    "size": 100
}

# for lyrics search
# https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-mlt-query.html
more_like_this = {
    "query": {
        "bool": {
            "must": [
                {
                    "more_like_this": {
                        "fields": ["dzr_lyrics.content"],
                        "like": "sample_query_here",
                        "min_term_freq": 1,
                        "max_query_terms": 12
                    }
                }
            ],
            "must_not": [
                {
                    "query_string": {
                        "default_field": "_id",
                        "query": "msd_track_id_here"
                    }
                }
            ]
        }
    },
    "from": 0,
    "size": 100
}

# config preset for logs
log_config = {
    'method':
        {
            'query_method': 'title_query_with_artist_rerank',
            'mode': 'msd_field',
            'size': 100,
            'dataset': 'shs_train'
        },
    'metrics':
        {
            'MAP': 0.,
            'MRFC': 0.,
            'MPER': 40.
        },
    'run_time': 60
}

# Experiment profiles
# SHS against MSD experiment
shs_msd = {
    'dzr_map': False,
    'filter_duplicates': False,
    'shs_mode': False
}

# SHS against MSD experiment by excluding all the official duplicates
shs_msd_no_dup = {
    'dzr_map': False,
    'filter_duplicates': True,
    'shs_mode': False
}

# SHS-DZR against MSD-DZR experiment by excluding all the official duplicates
shs_dzr_msd = {
    'dzr_map': True,
    'filter_duplicates': True,
    'shs_mode': False
}

# SHS train set against SHS train set experiment
shs_shs = {
    'dzr_map': False,
    'filter_duplicates': False,
    'shs_mode': True
}

# SHS train set against SHS train set experiment without official duplicates.
shs_shs_no_dup = {
    'dzr_map': False,
    'filter_duplicates': True,
    'shs_mode': True
}

output_evaluations = {
    'size': 100,
    'map': 0,
    'method': 'title',
    'experiment': 'shs_msd',
}
