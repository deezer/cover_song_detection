# -*- coding: utf-8 -*-
"""
Set of functions and methods for various search requests to dzr_elastic search augmented msd db

Albin Andrew Correya
R&D Intern
@Deezer,2017
"""
from requests import get
from copy import deepcopy
import templates as presets


class SearchModule(object):
    """
    Class containing custom methods to search the elasticsearch index containing the augmented MSD dataset
    """
    from elasticsearch import Elasticsearch
    import pandas as pd
    import json

    init_json = deepcopy(presets.simple_query_string)  # save the preset as attribute

    def __init__(self, uri_config, query_json=None, timeout=30):
        """
        Init params:
                    uri_config : uri_config dictionary specifying the host and port of es db.
                                (check 'uri_config' in the templates.py file)
                    query_json : {default : None}

        """
        self.config = uri_config
        self.handler = self.Elasticsearch(hosts=[{'host': self.config['host'],
                                                  'port': self.config['port'],
                                                  'scheme': self.config['scheme']}], timeout=30)

        if query_json:
            self.post_json = query_json
        else:
            self.post_json = presets.simple_query_string

        return

    def _load_json(self, jsonfile):
        """Load a json file as python dict"""
        with open(jsonfile) as f:
            json_data = self.json.load(f)
        return json_data  

    def _make_request(self, target_url, query, verbose=False):
        """ 
        [DEPRECIATED] make the request and fetch results
        """
        if verbose:
            print "GET %s -d '%s'" % (target_url, self.json.dumps(query))
        r = get(target_url, data=self.json.dumps(query))
        return self.json.loads(r.text)

    def _format_url(self, msd_id):
        return "%s://%s:%s/%s/%s/%s" % (
            self.config['scheme'],
            self.config['host'],
            self.config['port'],
            self.config['index'],
            self.config['type'],
            msd_id
        )

    def _format_query(self, query_str, msd_id, mode='simple_query', field='msd_title', size=100):
        """
        Format POST json dict object with query_str and msd_id
        """
        if mode == 'simple_query':
            self.post_json['query']['bool']['must'][0]['simple_query_string']['query'] = query_str
            self.post_json['query']['bool']['must'][0]['simple_query_string']['fields'][0] = field
            # we exclude the query id from the result
            self.post_json['query']['bool']['must_not'][0]['query_string']['query'] = msd_id
            self.post_json['size'] = size
        if mode == 'query_string':
            self.post_json['query']['bool']['must'][0]['query_string']['query'] = query_str
            # we exclude the query id from the result
            self.post_json['query']['bool']['must_not'][0]['query_string']['query'] = msd_id
            self.post_json['size'] = size
        return self.post_json

    @staticmethod
    def _format_init_json(init_json, query_str, msd_id, field='msd_title', size=100):
        """
        """
        init_json['query']['bool']['must'][0]['simple_query_string']['query'] = query_str
        init_json['query']['bool']['must'][0]['simple_query_string']['fields'][0] = field
        # we exclude the query id from the result
        init_json['query']['bool']['must_not'][0]['query_string']['query'] = msd_id
        init_json['size'] = size
        return init_json

    @staticmethod
    def _parse_response_for_eval(response):
        """
        Parse list of msd_track_ids and their respective scores from a search response json

        Input :
                response : json response from the elasticsearch
        """
        msd_ids = [d['_id'] for d in response]
        scores = [d['_score'] for d in response]
        return msd_ids, scores

    def _view_response(self, response):
        """
        Aggregrate response as pandas dataframe to view response as tables in the ipython console

        Input :
                response : json response from the elasticsearch

        Output : A pandas dataframe with aggregrated results

        """
        row_list = [(track['_id'], track['_score'], track['_source']['msd_title']) for track in response]

        results = self.pd.DataFrame({
            'msd_id': [r[0] for r in row_list],
            'score': [r[1] for r in row_list],
            'msd_title': [r[2] for r in row_list]
        })
        return results

    def format_lyrics_post_json(self, body, lyrics, track_id, size, field='dzr_lyrics.content'):
        """
        format post_json template for lyrics search with lyrics and msd_track-id
        """
        self.post_json = body
        self.post_json['query']['bool']['must'][0]['more_like_this']['like'] = lyrics
        self.post_json['query']['bool']['must'][0]['more_like_this']['fields'][0] = field
        # we exclude the query id from the results
        self.post_json['query']['bool']['must_not'][0]['query_string']['query'] = track_id
        self.post_json['size'] = size

    def limit_post_json_to_shs(self):
        """
        Limits search only on the songs present in the second hand songs train set as provided by labrosa
        ie. limit search to 1 x 12960 from 1 x 1M

        """
        if len(self.post_json['query']['bool']['must']) <= 1:  # mar: why this condition?
            self.post_json['query']['bool']['must'].append({'exists': {'field': 'shs_id'}})

    def limit_to_dzr_mapped_msd(self):
        """
        Limits search only on the songs have a respective mapping to the deezer_song_ids
        ie. limit search to 1 x ~83k
        """
        self.post_json['query']['bool']['must'].append({'exists': {'field': 'dzr_song_title'}})

    def add_remove_duplicates_filter(self):
        """
        Filter songs with field 'msd_is_duplicate_of' from the search
        and response using must_not exist method in the post-request
        """
        if len(self.post_json['query']['bool']['must_not']) <= 1:  # mar: why this condition?
            self.post_json['query']['bool']['must_not'].append({'exists': {'field': 'msd_is_duplicate_of'}})

    @staticmethod
    def add_must_field_to_query_dsl(post_json, role_type='Composer', field='dzr_artists.role_name',
                                    query_type='simple_query_string'):
        post_json['query']['bool']['must'].append({query_type: {'fields': [field], 'query': role_type}})
        return post_json  # mar: why here we return something and not the previous add_ function

    @staticmethod
    def add_role_artists_to_query_dsl(post_json, artist_names, field='dzr_artists.artist_name',
                                      query_type='simple_query_string'):
        if len(artist_names) > 1:
            query_str = ' OR '.join(artist_names)
        else:
            query_str = artist_names[0]

        post_json['query']['bool']['must'].append({query_type: {'fields': [field], 'query': query_str}})
        return post_json  # mar: why here we return something and not the previous add_ function

    @staticmethod
    def parse_field_from_response(response, field='msd_artist_id'):
        """
        Parse a particular field value from the es response

        :param response: es response json
        :param field: field_name
        """
        if field not in response['_source'].keys():
            return None
        elif not response['_source'][field]:
            return None
        elif field == 'dzr_lyrics':
            return response['_source'][field]['content']
        else:
            return response['_source'][field]

    def get_field_info_from_id(self, msd_id, field):
        """
        Retrieve info for a particular field associated to a msd_id in the es db
            eg. get_field_info_from_id(msd_id='TRWFERO128F425FE0D', field='dzr_lyrics.content') 
        """
        response = get(self._format_url(msd_id))
        field_info = self.parse_field_from_response(response.json(), field=field)
        return field_info

    def get_lyrics_by_id(self, track_id):
        """
        Returns the lyrical content associated with a specific msd_track_id from the es db
        """
        return self.get_field_info_from_id(msd_id=track_id, field='dzr_lyrics')

    def get_mxm_lyrics_by_id(self, track_id):
        """
        Get Musixmatch lyrics associated with a msd track id from es index if there is any.
        :param track_id: msd track id
        :return:
        """
        return self.get_field_info_from_id(msd_id=track_id, field='mxm_lyrics')

    def get_cleaned_title_from_id(self, msd_id, field="dzr_msd_title_clean"):
        """
        Get preprocessed MSD title by MSD track id
        """
        # mar: the field "dzr_msd_title_clean" should not be a parameter (like in get_mxm_lyrics)
        response = get(self._format_url(msd_id))
        return self.parse_field_from_response(response.json(), field=field)

    def get_dzr_roles_from_id(self, track_id, role_type='Composer'):
        """
        Get dzr_role data corresponds to a msd track from the ES index of there is any
        """
        # mar: I suggest to override self.handler.get to avoid passing the index as a parameter
        roles = self.handler.get(index=self.config['index'], id=track_id, _source_include=['dzr_artists'])
        if 'dzr_artists' in roles['_source'].keys():
            roles = roles['_source']['dzr_artists']
            return [role['artist_name'] for role in roles if role['role_name'] == role_type]

        return None  # mar: should return an empty list here

    def search_es(self, body):
        """
        Make a search request to elasticsearch provided by json POST dictionary
        [This is a general method you can use for querying the es db with respective query_dsl as inputs]

        Input :
                body : JSON post dict for elastic search 
                (you can use the template jsons in the templates.py script)
                eg : body = templates.simple_query_string
        """
        res = self.handler.search(index=self.config["index"], body=body)
        return res['hits']['hits']

    def search_by_exact_title(self, track_title, track_id, mode='simple_query', out_mode='view', size=100):
        """
        Search by track_title using simple_query_string method in the elasticsearch
        """
        res = self.search_es(self._format_query(query_str=track_title, msd_id=track_id, mode=mode, size=size))

        # mar: because the following code is copy/pasted several times, it should be a function
        # like return_results(res, out_mode)
        if out_mode == 'eval':
            msd_ids, scores = self._parse_response_for_eval(res)
            return msd_ids, scores

        if out_mode == 'view':
            return self._view_response(res)

        return None

    def search_with_cleaned_title(self, track_id, out_mode='view', field="dzr_msd_title_clean", size=100):
        """
        Search by cleaned msd_track-title
        """
        # mar: field="dzr_msd_title_clean" should not ne a parameter but included in get_cleaned_title_from_id
        track_title = self.get_cleaned_title_from_id(msd_id=track_id)
        res = self.search_es(self._format_query(query_str=track_title, msd_id=track_id, mode='simple_query',
                                                field=field, size=size))
        if out_mode == 'eval':
            msd_ids, scores = self._parse_response_for_eval(res)
            return msd_ids, scores

        if out_mode == 'view':
            return self._view_response(res)

        return None

    def search_by_dzr_lyrics(self, post_json, track_id, out_mode='eval', size=100):
        """
        Search es_msd_augmented_db for similar lyrics to an input lyrics
        based on the "more_like_this" document similarity method in elasticsearch

        [NOTE]: It returns a tuple of list of response msd_track_ids and
        response_scores from the elastic search response if the track has corresponding "dzr_lyrics"
                otherwise return a tuple of (None, None)
        Inputs :
                post_json : (dict) post-json template for "more_like_this" es search
                            use presets.more_like_this template

                track_id : (string) msd track id

            Params :
                    out_mode : ['eval', 'view']
                    size : size of the required response from es_db

        """
        lyrics = self.get_lyrics_by_id(track_id)

        if not lyrics:
            return None, None

        self.format_lyrics_post_json(body=post_json, track_id=track_id, lyrics=lyrics, size=size)
        res = self.search_es(body=self.post_json)

        if out_mode == 'eval':
            msd_ids, scores = self._parse_response_for_eval(res)
            return msd_ids, scores

        if out_mode == 'view':
            return self._view_response(res)

        return None

    def search_by_mxm_lyrics(self, post_json, msd_track_id, out_mode='eval', size=100):
        """
        Search the es_db by musixmatch_lyrics which are mapped to certain msd_track_ids
        These mappings are obtained from the musixmatch dataset (https://labrosa.ee.columbia.edu/millionsong/musixmatch)

        [NOTE]: It returns a tuple of list of response msd_track_ids and
          response_scores from the elastic search response if the track has corresponding "mxm_lyrics"
          otherwise return a tuple of (None, None)

        Inputs:
                post_json : (dict) Query_DSL json template for the es_query (eg. presets.more_like_lyrics)
                msd_track_id : (string) MSD track identifier of the query file

            Params :
                    out_mode : (string) Available modes (['eval', 'view'])
                    size : (int) size of the required response from es_db
        """
        lyrics = self.get_mxm_lyrics_by_id(msd_track_id)

        if not lyrics:
            return None, None

        self.format_lyrics_post_json(body=post_json, track_id=msd_track_id, lyrics=lyrics,
                                     size=size, field='mxm_lyrics')
        res = self.search_es(body=self.post_json)

        if out_mode == 'eval':
            msd_ids, scores = self._parse_response_for_eval(res)
            return msd_ids, scores

        if out_mode == 'view':
            return self._view_response(res)

        return None

    def search_with_roles(self, track_title, track_id, out_mode='view', shs_mode=False,
                          filter_duplicates=False, size=100):
        """
        Search ES index with MSD song title and Composer credited artists
        :param track_title:
        :param track_id:
        :param out_mode:
        :param shs_mode:
        :param filter_duplicates:
        :param size:
        :return:
        """
        roles = self.get_dzr_roles_from_id(track_id)

        post_json = deepcopy(self.init_json)

        post_json['query']['bool']['must'][0]['simple_query_string']['query'] = track_title
        post_json['query']['bool']['must'][0]['simple_query_string']['fields'][0] = 'msd_title'
        post_json['query']['bool']['must_not'][0]['query_string']['query'] = track_id 
        post_json['size'] = size

        if shs_mode:
            post_json['query']['bool']['must'].append({'exists': {'field': 'shs_id'}})

        if filter_duplicates:
            post_json['query']['bool']['must_not'].append({'exists': {'field': 'msd_is_duplicate_of'}})

        if roles:
            post_dict = self.add_must_field_to_query_dsl(post_json, role_type='Composer', field='dzr_artists.role_name')
            post_dict = self.add_role_artists_to_query_dsl(post_dict, roles, field='dzr_artists.artist_name')

            res = self.search_es(post_dict)

            if res:
                if out_mode == 'eval':
                    msd_ids, scores = self._parse_response_for_eval(res)
                    return msd_ids, scores
                if out_mode == 'view':
                    return self._view_response(res)
            else:
                return None, None
        else:
            return None, None

        return None
