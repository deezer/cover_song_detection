# -*- coding: utf-8 -*-
"""
Methods for running various experiments on es msd db for the task of cover
song detection using metadata and lyrics ingested in the ES index.

----------
Albin Andrew Correya
R&D Intern
@Deezer, 2017
"""

from utils import log, timeit
import templates as presets
# bad hack for avoiding encoding erros for the moment
# to be removed soon
import sys

reload(sys)
sys.setdefaultencoding("utf8")

LOGGER = log('./logs/experiments.log')


class Experiments(object):
    """
    Class containing methods for running various experiments on
    SecondHandSong and MillionSongDataset ingested in the elasticsearch
    msd_augmented index for the task of cover song detection.
    This is a wrapper on the es_search.py -> SearchModule class
    for doing fast prototyping.

    Pandas dataframe and json dict is mainly used as the data
    structure for dealing with aggregrated response results.
    
    Usage:
        exp = Experiments(es_search_class, shs_dataset_csv, presets.shs_msd)
        results = exp.run_song_title_match_task(size=100)
        m_avgp = exp.mean_average_precision(res)
    """

    import pandas as pd
    import numpy as np
    import time

    def __init__(self, search_class, shs_csv, profile=None):
        """
        Init parameters

        :param search_class: An instance of SearchModule class (es_search.py)
        :param shs_csv: path to csv file of SecondHandSong dataset (check the ./data/ folder)
            This will be the query-set and groundtruth for the experiments
        :param profile: {default: None}
            A python dictionary corresponds to the profile of the experiment object

            eg: {
                'filter_duplicates':True,
                'dzr_map':False,
                'shs_mode':False
            }

            NOTE : a set of profile templates can be found inside the templates.py file.
        """
        self.es = search_class
        self.dataset = self._load_csv_as_df(shs_csv)
        self.query_ids = self.dataset.msd_id.values.tolist()
        self.query_titles = self.dataset.title.values.tolist()

        if profile:
            self.filter_duplicates = profile['filter_duplicates']
            self.dzr_map = profile['dzr_map']
            self.shs_mode = profile['shs_mode']
        else:
            self.filter_duplicates = presets.shs_msd['filter_duplicates']
            self.dzr_map = presets.shs_msd['dzr_map']
            self.shs_mode = presets.shs_msd['shs_mode']

        return

    def _load_csv_as_df(self, csvfile):
        """Load csv file as pandas dataframe"""
        return self.pd.read_csv(csvfile)

    def _get_subframe_df(self, dataframe, field):
        """get a particular subframe from the pandas dataframe"""
        return dataframe[field].copy().values.tolist()

    def _tolist(self, x):
        """For use it as pandas dataframe.apply() callback"""
        return list(x)

    def _merge_df(self, results_df, field='msd_id'):
        """Merge the dataset and the results df"""
        results_df[field] = self.pd.Series(results_df.index.values, index=results_df.index)
        return self.pd.merge(self.dataset, results_df, on=field, how='left')

    def _groupby_work(self, merged_df):
        return merged_df.groupby('work_id')['msd_id'].agg({'clique_songs': self._tolist})

    def load_result_json_as_df(self, jsonfile):
        """Load results json from the experiments to pandas df"""
        return self.pd.read_json(jsonfile, orient='index')

    def dict_to_pickle(self, mydict, filename):
        """save a dict to pickle file"""
        import pickle
        doc = open(filename, 'wb')
        pickle.dump(mydict, doc)
        return

    def get_clique_id(self, track_id):
        """DEPRECIATED"""
        # have to recheck if this is same for all the sample
        return self.dataset[self.dataset.msd_id==track_id].clique_id.values.tolist()

    def get_ground_truth(self, query_id, reference_id):
        """DEPRECIATED [To_remove]"""
        if str(self.get_clique_id(query_id)) == str(self.get_clique_id(reference_id)):
            return 1
        else:
            return 0

    def reset_preset(self):
        self.es.post_json = self.es.init_json
        return

    def get_artist_id(self, track_id):
        """
        Returns artist_id for a specific msd_track_id from the dataset
        """
        return self.dataset.artist_id[self.dataset.msd_id == track_id].values[0]

    def rerank_by_field(self, field_id, response, proximitiy=1, field='msd_artist_id'):
        """
        Re-rank the search results by taking a field with thresholding
        """
        top_list = list()
        bottom_list = list()
        if response:
            top_score = response[0]['_score']
        else:
            return []
        for row in response:
            if row['_source'][field] == field_id and (top_score - row['_score']) <= proximitiy:
                top_list.append(row)
            else:
                bottom_list.append(row)
        if not top_list:
            return response
        else:
            return top_list + bottom_list

    def get_score_thres(self, res_ids, res_scores, proximity=1.):
        """

        :param res_ids: A list of ranked msd_track_ids. (typically from the lyrics_search response)
        :param res_scores: A list of ranked scores corresponds to the res_ids
        :param proximity: (int, default: 1) A threshold value for determining the boundary of differnce among the top_score and the other scores
        :return: (top_ids, top_list, thres_idx)
            top_ids : top msd_track_ids
            top_list : top es search scores
            thres_idx : threshold index
        """
        top_score = res_scores[0]
        top_list = [score for score in res_scores if (top_score-score) <= proximity]
        thres_idx = len(top_list)
        top_ids = res_ids[:thres_idx]
        return top_ids, top_list, thres_idx

    def rerank_title_results_by_lyrics(self, title_res, lyrics_res, mode='view', proximity=0.5):
        """
        :param title_res: pandas dataframe with aggregrated response of song_title match results
        :param lyrics_res: pandas dataframe with aggregrated response of lyrics_similarity search results
        :param mode: (available modes ['view', 'eval']) {default : 'view'}
            'view' - return reranked_response as pandas dataframe
            'eval' - return reranked_response as tuple of list of msd_ids and relative scores
        :param proximity:
        :return:
        """
        top_ids, top_scores, thres_idx = self.get_score_thres(
            lyrics_res.msd_id.values, lyrics_res.score.values, proximity=proximity)  # threshold is 0.5
        title_res_ids = title_res.msd_id.values.tolist()
        common_ids = self.np.intersect1d(title_res.msd_id.values, top_ids)

        if len(common_ids) > 0:
            top_list = common_ids
            bottom_list = [x for x in title_res_ids if x not in common_ids]

            # preserve the ranking in the lyrics search response if it doesn't ()
            top_list = top_ids[sorted([list(top_ids).index(x) for x in top_list])]

            new_ranked_list = list(top_list) + bottom_list
            idx = [title_res_ids.index(x) for x in new_ranked_list]
            merged_df = title_res.iloc[idx]  # select the new ranked dataframe from the indexes
            merged_df = merged_df.set_index(self.np.arange(len(merged_df)))  # update the dataframe with new ranks
            if mode == 'view':
                return merged_df
            elif mode == 'eval':
                return merged_df.msd_id.values.tolist(), merged_df.score.values.tolist()
        else:
            if mode == 'view':
                return title_res
            elif mode == 'eval':
                return title_res.msd_id.values.tolist(), title_res.score.values.tolist()
        return

    """
    ------------------------------------------
    ------ AUTOMATED EXPERIMENTS -------------
    These are methods for running automated search experiments on the es msd_augmented_db
    """

    @timeit
    def run_song_title_match_task(self, size=100, verbose=True):
        """
        Simple experiment with simple text match
        """
        start_time = self.time.time()

        if self.shs_mode:
            self.es.limit_post_json_to_shs()

        if self.filter_duplicates:
            self.es.add_remove_duplicates_filter()

        if self.dzr_map:
            self.es.limit_to_dzr_mapped_msd()

        results = dict()

        LOGGER.info("\n=======Running song title-match task for %s query songs against top %s results of MSD... "
                    "with shs_mode %s, duplicate %s, dzr_map %s ========\n"
                    % (len(self.query_ids), size, str(self.shs_mode), str(self.filter_duplicates), str(self.dzr_map)))

        for title in enumerate(self.query_titles):
            if verbose:
                print "------%s-------%s" % (title[0], title[1])

            res_ids, res_scores = self.es.search_by_exact_title(
                unicode(title[1]), track_id=self.query_ids[title[0]], out_mode='eval', size=size)
            # aggregrate response_ids and scores into a dict by query_msd_id as key
            results[self.query_ids[title[0]]] = {'id': res_ids, 'score': res_scores}

        LOGGER.info("\n Task runtime : %s" % (self.time.time() - start_time))
        return self.pd.DataFrame.from_dict(results, orient='index')

    @timeit
    def run_cleaned_song_title_task(self, size=100, verbose=True):
        """Run MSD pre-processed title task"""
        start_time = self.time.time()

        if self.shs_mode:
            self.es.limit_post_json_to_shs()

        if self.filter_duplicates:
            self.es.add_remove_duplicates_filter()

        if self.dzr_map:
            self.es.limit_to_dzr_mapped_msd()

        results = dict()

        LOGGER.info("\n=======Running cleaned title-match task for %s query songs against top %s results of MSD... "
                    "with shs_mode %s, duplicate %s, dzr_map %s ========\n"
                    % (len(self.query_ids), size, str(self.shs_mode), str(self.filter_duplicates), str(self.dzr_map)))

        for ids in enumerate(self.query_ids):
            if verbose:
                print "----%s----%s" % (ids[0], ids[1])
            res_ids, res_scores = self.es.search_with_cleaned_title(track_id=ids[1], out_mode='eval', size=size)
            results[ids[1]] = {'id': res_ids, 'score': res_scores}

        LOGGER.info("\n Task runtime : %s" % (self.time.time() - start_time))
        return self.pd.DataFrame.from_dict(results, orient='index')

    @timeit
    def run_field_rerank_task(self, field='msd_artist_id', size=100, proximitiy=1, verbose=True):
        """
        In this task, a msd song with same artist id with the query song will be ranked top of the list
        """
        results = dict()
        LOGGER.info("\n=======Running song title-matching task with reranking by '%s' for %s query "
                    "songs against top %s results of MSD... with shs_mode %s, duplicate %s, dzr_map %s ========\n"
                    % (field, len(self.query_ids), size, str(self.shs_mode),
                       str(self.filter_duplicates), str(self.dzr_map)))

        if self.shs_mode:
            self.es.limit_post_json_to_shs()

        if self.filter_duplicates:
            self.es.add_remove_duplicates_filter()

        if self.dzr_map:
            self.es.limit_to_dzr_mapped_msd()

        for index,title in enumerate(self.query_titles):
            if verbose:
                print "------%s-------%s" % (index, title)
            response = self.es.search_es(self.es._format_query(title, self.query_ids[index], size=size))
            query_artist_id = self.get_artist_id(self.query_ids[index])
            re_ranked = self.rerank_by_field(query_artist_id, response, field=field, proximitiy=proximitiy)
            res_ids, res_scores = self.es._parse_response_for_eval(re_ranked)
            results[self.query_ids[index]] = {'id': res_ids, 'score': res_scores}  # save it to dictionary
        return self.pd.DataFrame.from_dict(results, orient='index')


    @timeit
    def run_dzr_lyrics_search_task(self, post_json=presets.more_like_this, size=100, verbose=True):
        """
        more-like document search of lyrics using es
        Check documentation for details (https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-mlt-query.html)

        :param post_json: query dsl for lyrics elastic search
        :param size: size of the top-k reponse
        :param verbose: Boolean
        :return: A aggregrated response from ES
        """

        if self.shs_mode:
            self.es.limit_post_json_to_shs()

        if self.filter_duplicates:
            self.es.add_remove_duplicates_filter()

        if self.dzr_map:
            self.es.limit_to_dzr_mapped_msd()

        results = dict()

        LOGGER.info("\n=======Running dzr lyrics search task for %s query songs against top %s "
                    "results of MSD... with shs_mode %s, duplicate %s, dzr_map %s ========\n"
                    % (len(self.query_ids), size, str(self.shs_mode), str(self.filter_duplicates), str(self.dzr_map)))

        for ids in enumerate(self.query_ids):
            if verbose:
                print "------%s-------%s" % (ids[0], ids[1])
            res_ids, res_scores = self.es.search_by_dzr_lyrics(
                post_json=post_json, track_id=ids[1], out_mode='eval', size=size)
            results[ids[1]] = {'id': res_ids, 'score': res_scores}
        return self.pd.DataFrame.from_dict(results, orient='index')

    @timeit
    def run_mxm_lyrics_search_task(self, post_json=presets.more_like_this, size=100, verbose=True):
        """
        Lyrics search method using MXM lyrics
        (https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-mlt-query.html)
        """
        results = dict()

        if self.shs_mode:
            self.es.limit_post_json_to_shs()

        if self.filter_duplicates:
            self.es.add_remove_duplicates_filter()

        if self.dzr_map:
            self.es.limit_to_dzr_mapped_msd()

        LOGGER.info("\n=======Running musixmatch-msd lyrics search task for %s query songs against "
                    "top %s results of MSD... with shs_mode %s, duplicate %s, dzr_map %s ========\n"
                    % (len(self.query_ids), size, str(self.shs_mode), str(self.filter_duplicates), str(self.dzr_map)))

        for index, ids in enumerate(self.query_ids):
            if verbose:
                print "----%s----%s" % (index, ids)
            res_ids, res_scores = self.es.search_by_mxm_lyrics(post_json, msd_track_id=ids, out_mode='eval', size=size)
            results[ids] = {'id': res_ids, 'score': res_scores}

        return self.pd.DataFrame.from_dict(results, orient='index')

    @timeit
    def run_rerank_title_with_dzr_lyrics_task(self, size=100, with_cleaned=False, verbose=True):
        """
        Here you make two requests with song_title metadata and dzr_lyrics and merge the results with the top resutls
        of lyrics to rerank song-title search response
        """
        results = dict()

        if self.shs_mode:
            self.es.limit_post_json_to_shs()

        if self.filter_duplicates:
            self.es.add_remove_duplicates_filter()

        if self.dzr_map:
            self.es.limit_to_dzr_mapped_msd()

        post_json = self.es.post_json

        LOGGER.info("\n=======Running rerank experiment of title search response with dzr_lyrics response for %s "
                    "query songs against top %s results of MSD... with shs_mode %s, duplicate %s, dzr_map %s ========\n"
                    % (len(self.query_ids), size, str(self.shs_mode), str(self.filter_duplicates), str(self.dzr_map)))

        for index, title in enumerate(self.query_titles):
            if verbose:
                print "---%s---%s" % (index, self.query_ids[index])

            self.es.post_json = post_json  # post-json template for title search

            if with_cleaned:
                text_df = self.es.search_with_cleaned_title(self.query_ids[index], out_mode='view', size=size)
            else:
                text_df = self.es.search_by_exact_title(title, self.query_ids[index], out_mode='view', size=size)

            lyrics_df = self.es.search_by_dzr_lyrics(
                presets.more_like_this, self.query_ids[index], out_mode='view', size=size)

            if type(lyrics_df) != tuple:
                if lyrics_df.empty:
                    res_ids, res_scores = text_df.msd_id.values.tolist(), text_df.score.values.tolist()
                else:
                    res_ids, res_scores = self.rerank_title_results_by_lyrics(text_df, lyrics_df, mode='eval')
            else:
                res_ids, res_scores = text_df.msd_id.values.tolist(), text_df.score.values.tolist()
            results[self.query_ids[index]] = {'id': res_ids, 'score': res_scores}
        return self.pd.DataFrame.from_dict(results, orient='index')

    @timeit
    def run_rerank_title_with_mxm_lyrics_task(self, size=100, with_cleaned=False, verbose=True, threshold=0.5):
        """
        Experiment we rerank the es response of song_title search with top results of mxm_lyrics similarity results

        :param size: {default : 100}
        :param with_cleaned: {default : False} If set true, switch simple
            text_search method to cleaned_processed title method
        :param verbose: {default : False}
        :param threshold:
        :return: Aggregated results as pandas dataframe
        """
        results = dict()

        if self.shs_mode:
            self.es.limit_post_json_to_shs()

        if self.filter_duplicates:
            self.es.add_remove_duplicates_filter()

        if self.dzr_map:
            self.es.limit_to_dzr_mapped_msd()

        post_json = self.es.post_json

        LOGGER.info("\n=======Running rerank experiment of title search response with mxm_lyrics response for %s query "
                    "songs against top %s results of MSD... with shs_mode %s, duplicate %s, dzr_map %s ========\n"
                    % (len(self.query_ids), size, str(self.shs_mode), str(self.filter_duplicates), str(self.dzr_map)))

        for index, title in enumerate(self.query_titles):
            if verbose:
                print "---%s---%s" % (index, self.query_ids[index])

            self.es.post_json = post_json  # post-json template for title search

            if with_cleaned:
                text_df = self.es.search_with_cleaned_title(self.query_ids[index], out_mode='view', size=size)
            else:
                text_df = self.es.search_by_exact_title(title, self.query_ids[index], out_mode='view', size=size)

            lyrics_df = self.es.search_by_mxm_lyrics(
                presets.more_like_this, msd_track_id=self.query_ids[index], out_mode='view', size=size)

            if type(lyrics_df) != tuple:
                if lyrics_df.empty:
                    res_ids, res_scores = text_df.msd_id.values.tolist(), text_df.score.values.tolist()
                else:
                    res_ids, res_scores = self.rerank_title_results_by_lyrics(
                        text_df, lyrics_df, mode='eval', proximity=threshold)
            else:
                res_ids, res_scores = text_df.msd_id.values.tolist(), text_df.score.values.tolist()
            results[self.query_ids[index]] = {'id': res_ids, 'score': res_scores}
        return self.pd.DataFrame.from_dict(results, orient='index')

    @timeit
    def run_text_credits_search_task(self, size=100, verbose=True):
        """
        """
        results = dict()

        LOGGER.info("\n== Running credit search online experiment with song_title for %s query songs against "
                    "top %s results of MSD... with shs_mode %s, duplicate %s, dzr_map %s\n"
                    % (len(self.query_ids), size, str(self.shs_mode), str(self.filter_duplicates), str(self.dzr_map)))

        for index, title in enumerate(self.query_titles):

            if verbose:
                print "---%s---%s" % (index, self.query_ids[index])

            res_ids, res_scores = self.es.search_with_roles(
                track_title=title, track_id=self.query_ids[index], shs_mode=self.shs_mode,
                filter_duplicates=self.filter_duplicates, out_mode='eval', size=size)

            results[self.query_ids[index]] = {'id': res_ids, 'score': res_scores}

        return self.pd.DataFrame.from_dict(results, orient='index')

    @timeit
    def run_rerank_credits_with_title_task(self, size=100, threshold=0.1, verbose=True):
        results = dict()

        LOGGER.info("\nRerank credist online task")

        for index, title in enumerate(self.query_titles):

            if verbose:
                print "---%s---%s" % (index, self.query_ids[index])

            self.es.post_json = presets.simple_query_string
            title_df = self.es.search_by_exact_title(
                track_title=title, track_id=self.query_ids[index], out_mode='view', size=size)
            credits_df = self.es.search_with_roles(
                title, self.query_ids[index], shs_mode=self.shs_mode, out_mode='view', size=size)

            if type(credits_df) == tuple:
                res_ids, res_scores = title_df.msd_id.values.tolist(), title_df.score.values.tolist()
            else:
                res_ids, res_scores = self.rerank_title_results_by_lyrics(title_df, credits_df,
                                                                          mode='eval', proximity=threshold)

            results[self.query_ids[index]] = {'id': res_ids, 'score': res_scores}

        return self.pd.DataFrame.from_dict(results, orient='index')

    @timeit
    def run_credits_rerank_offline_task(self, text_results_json, role_type='Composer', threshold=0.5, verbose=True):
        text_df = self.pd.read_json(text_results_json)
        results = dict()
        cnt = 0
        # error_idxs = list()

        LOGGER.info("Running dzr_song credits reranking task on the %s results "
                    "file with role_type : %s and threshold : %s" % (text_results_json, role_type, threshold))

        for idx in range(len(text_df)):

            query_id = text_df.index[idx]
            if verbose:
                print "---%s---%s" % (idx, query_id)
            top_rerank_idxs = list()
            top_res_roles = list()

            response_ids = text_df.iloc[idx].id
            response_scores = text_df.iloc[idx].score

            if not response_scores or not response_ids or len(response_ids) == 0:
                results[query_id] = {'id': text_df.msd_id.values.tolist(), 'score': text_df.score.values.tolist()}
                cnt += 1
                # error_idxs.append(idx)
            else:
                t_ids, t_scores, thres_idx = self.get_score_thres(response_ids, response_scores, proximity=threshold)

                role_artists = self.es.get_dzr_roles_from_id(track_id=query_id, role_type=role_type)

                if role_artists:
                    for index, ids in enumerate(response_ids[:thres_idx]):
                        res_roles = self.es.get_dzr_roles_from_id(track_id=ids, role_type=role_type)
                        # print "RES_ROLES : ",res_roles
                        if res_roles:
                            res_roles = [artist for artist in res_roles if artist in role_artists]
                            top_res_roles.extend(res_roles)
                            top_rerank_idxs.append(index)
                        else:
                            pass
                    if top_rerank_idxs:
                        top_ids = self.np.array(response_ids)[top_rerank_idxs]
                        top_scores = list(self.np.array(response_scores)[top_rerank_idxs])
                        bottom_ids = [ids for ids in response_ids if ids not in top_ids]
                        bottom_idx = [response_ids.index(x) for x in bottom_ids]
                        bottom_scores = self.np.array(response_scores)[bottom_idx]
                        new_ranked_ids = list(top_ids) + bottom_ids
                        new_ranked_scores = top_scores + list(bottom_scores)
                        results[query_id] = {'id': new_ranked_ids, 'score': new_ranked_scores}
                    else:
                        results[query_id] = {'id': text_df.msd_id.values.tolist(),
                                             'score': text_df.score.values.tolist()}

                else:
                    results[query_id] = {'id': text_df.msd_id.values.tolist(), 'score': text_df.score.values.tolist()}

        LOGGER.debug("%s queries dont have proper dzr roles" % cnt)

        return self.pd.DataFrame.from_dict(results, orient='index')

    @timeit
    def run_audio_rerank_task(self, text_results_json, audio_results_json, threshold=0.1):
        """
        [OFFLINE EXPERIMENT]

        Function to re-rank text-based results with audio-based results
        text_results_json : json file
        audio_results_json : json file
        threshold : {default: 0.1}

        """
        text_df = self.pd.read_json(text_results_json)
        audio_df = self.pd.read_json(audio_results_json)
        results = dict()
        cnt = 0
        error_idxs = []

        def get_low_score(scores, thres=threshold):

            def list_duplicates_of(seq, item):
                start_at = -1
                locs = []
                while True:
                    try:
                        loc = seq.index(item, start_at+1)
                    except ValueError:
                        break
                    else:
                        locs.append(loc)
                        start_at = loc
                return locs

            # top_score = scores[0]
            # top_list = [score for score in scores if self.np.abs(top_score-score)<=thres]
            top_list = []
            for score in scores:
                # if self.np.abs(top_score-score)<=thres:
                if score <= thres:
                    top_list.append(score)
            # idxs = [scores.index(x) for x in top_list]

            dup_idxs = []
            for s in top_list:
                dup_idxs.extend(list_duplicates_of(top_list, s))

            idxs = list(set(dup_idxs))
            print "Score index :", idxs
            return idxs

        LOGGER.info("Running audio reranking task on the metadata search experiments results "
                    "file with a threshold of %s" % threshold)

        for idx in range(len(audio_df)):
            print "Index :", idx
            text_res_ids = text_df.iloc[idx].id
            text_res_scores = text_df.iloc[idx].id
            audio_res_ids = audio_df.iloc[idx].id
            audio_res_scores = audio_df.iloc[idx].score

            if not audio_res_scores or not audio_res_ids or len(audio_res_ids) == 0:
                results[audio_df.index[idx]] = {'id': text_res_ids, 'score': text_res_scores}
                cnt += 1
                error_idxs.append(idx)
            else:
                a_df = self.pd.DataFrame({'id': audio_res_ids, 'score': audio_res_scores})
                # t_df = self.pd.DataFrame({'id': text_res_ids, 'score' : text_res_scores})

                thres_idxs = get_low_score(a_df.score.values.tolist(), threshold)

                if len(thres_idxs) != 0:
                    a_df = a_df.iloc[thres_idxs]
                    top_ids = a_df.id.values.tolist()
                    top_scores = a_df.score.tolist()
                    # common_ids = self.np.intersect1d(top_ids, t_df.id.values)
                    bottom_ids = [x for x in text_df.iloc[idx].id if x not in top_ids]
                    bottom_idx = [text_res_ids.index(x) for x in bottom_ids]
                    text_res_scores = self.np.array(text_res_scores)
                    bottom_scores = text_res_scores[bottom_idx]
                    new_ranked_ids = top_ids + bottom_ids
                    new_ranked_scores = top_scores + list(bottom_scores)
                    results[audio_df.index[idx]] = {'id': new_ranked_ids, 'score': new_ranked_scores}
                else:
                    results[audio_df.index[idx]] = {'id': text_res_ids, 'score': text_res_scores}

        LOGGER.debug("%s queries dont have proper audio reranked resposne" % cnt)
        # print error_idxs
        return self.pd.DataFrame.from_dict(results, orient='index')

    @timeit
    def maximum_achievable_metrics(self, results_df):
        """
        In this experiment we rerank the response ids with the ground_truth to compute
        the maximum achievable MAP by re-ranking the metadata-search results with
        other content such as lyrics, audio etc.
        """
        LOGGER.info("Computing maximum achievable mean average precison from the results dataframe")
        results_df = self._merge_df(results_df)
        results = dict()
        for index, response in results_df.iterrows():
            if type(response['id']) == list:
                response_ids = response['id']
                # result_songs = response['id']
                clique_songs = results_df.msd_id[results_df.work_id == response['work_id']].values
                top_list = self.np.intersect1d(clique_songs, response_ids)
                if len(top_list) > 0:
                    bottom_list = [x for x in response_ids if x not in top_list]
                    if bottom_list:
                        results[response['msd_id']] = {'id': list(top_list) + bottom_list}
                    else:
                        results[response['msd_id']] = {'id': list(top_list)}
                else:
                    results[response['msd_id']] = {'id': response_ids}
        return self.pd.DataFrame.from_dict(results, orient='index')

    # ----------------------------------------EVALUATION METRICS----------------------------------------------------
    def average_precision_at_k(self, results_df, query_msd_id):
        """
        Compute average precision for a particular query and response from the aggregrated results_dataframe
        Here "k" is the msd_query_id in results df

        Inputs:
                results_df :
                query_msd_id :

        """
        results_df = self._merge_df(results_df)
        response_ids = results_df[results_df.msd_id == query_msd_id].id.values.tolist()[0]
        work_id = results_df.work_id[results_df.msd_id == query_msd_id].values[0]
        clique_songs = results_df.msd_id[results_df.work_id == work_id].values
        # print clique_songs, len(clique_songs)
        true_idx = [response_ids.index(x) for x in response_ids if x in clique_songs]
        ground_truth = self.np.zeros(len(response_ids))
        if len(true_idx) > 0:
            ground_truth[true_idx] = 1
        precision_at_k = self.np.cumsum(ground_truth) / self.np.arange(1., len(response_ids)+1)
        precision_list = ground_truth * precision_at_k
        avg_precision = sum(precision_list) / float(len(clique_songs) - 1)
        return avg_precision

    def average_precision(self, results_df, size=None):
        """
        Average precisions

        Inputs :
                results_df :
                size :

        Returns a list of average precision
        """
        results_df = self._merge_df(results_df)
        avg_precisions = list()
        cnt = 0
        for index, response in results_df.iterrows():
            if type(response['id']) == list:
                if size:
                    response_ids = response['id'][:size]
                else:
                    response_ids = response['id']
                clique_songs = results_df.msd_id[results_df.work_id == response['work_id']].values
                true_idx = [response_ids.index(x) for x in response_ids if x in clique_songs]
                ground_truth = self.np.zeros(len(response_ids))
                if len(true_idx) > 0:
                    ground_truth[true_idx] = 1
                precision_at_k = self.np.cumsum(ground_truth) / self.np.arange(1., len(response_ids)+1)
                precision_list = ground_truth * precision_at_k
                avg_precision = sum(precision_list) / float(len(clique_songs) - 1)
                avg_precisions.append(avg_precision)
            else:
                cnt += 1
                avg_precisions.append(0)
        LOGGER.debug("%s queries have no lyrics nor response out of %s queries" % (cnt, len(results_df)))
        return avg_precisions

    @timeit
    def mean_average_precision(self, results_df, size=None):
        """
        Mean of average precisions for the task
        """
        # return self.np.mean(self.old_average_precision(results_df))
        return self.np.mean(self.average_precision(results_df, size=size))

    def average_rank(self, results_df):
        """
        Computes average position of relevant documents and measures where the relevant docs falls in a ranked list
        """
        average_ranks = list()
        for query_id in results_df.keys():
            # response_ids = self.ast.literal_eval(results_df[query_id][0])
            response_ids = results_df[query_id][0]
            if type(response_ids) == list:
                clique_id = self.dataset.work_id[self.dataset.msd_id == query_id].values[0]
                clique_songs = self.dataset.msd_id[self.dataset.work_id == clique_id].values
                # true_list = len(self.np.intersect1d(clique_songs, response_ids))
                true_idx = [response_ids.index(x) for x in response_ids if x in clique_songs]
                if len(true_idx) == 0:
                    average_ranks.append(1000000)
                    # pass
                else:
                    average_ranks.append(self.np.average(true_idx))
        return self.np.average(average_ranks)

    def mean_rank_first_cover(self, results_df):
        """
        Mean rank of the first correctly identified cover
        """
        mean_ranks = list()
        for query_id in results_df.keys():
            # response_ids = self.ast.literal_eval(results_df[query_id][0])
            response_ids = results_df[query_id][0]
            if type(response_ids) == list:
                clique_id = self.dataset.work_id[self.dataset.msd_id == query_id].values[0]
                clique_songs = self.dataset.msd_id[self.dataset.work_id == clique_id].values
                # true_list = len(self.np.intersect1d(clique_songs, response_ids))
                true_idx = [response_ids.index(x) for x in response_ids if x in clique_songs]
                if len(true_idx) == 0:
                    # mean_ranks.append(0)
                    pass
                else:
                    mean_ranks.append(true_idx[0]+1)

        return self.np.mean(mean_ranks)

    def covers_identified(self, results_df, size=None):
        """
        Total number of covers identified compared to the dataset
        """
        total_covers = list()
        percentage = list()
        # here you merge the results_df with the shs_dataset df we load in the init class
        results_df = self._merge_df(results_df)
        for index, response in results_df.iterrows():
            if type(response['id']) == list:
                if size:
                    response_ids = response['id'][:size]
                else:
                    response_ids = response['id']
                clique_songs = results_df.msd_id[results_df.work_id == response['work_id']].values
                # check intersection of two list for detected covers
                detected_covers = self.np.intersect1d(clique_songs, response_ids)
                total_covers.append(len(detected_covers))
                percentage.append((len(detected_covers) / float(len(clique_songs)))*100)
        return total_covers, percentage

    def total_covers_identified(self, results_df):
        """
        Total number of covers identified
        """
        total_covers, percentage = self.covers_identified(results_df)
        return sum(total_covers)

    def mean_percentage_of_covers(self, results_df, size=None):
        """
        Mean percentage of covers
        """
        total_covers, percentage = self.covers_identified(results_df, size=size)
        return self.np.mean(percentage)
