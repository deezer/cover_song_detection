# -*- coding: utf-8 -*-
"""
Some general utility functions

Albin Andrew Correya
R&D Intern
@2017
"""

import logging
import time
import json


def log(log_file):
    """Returns a logger object with predefined settings"""
    # LOG_FILE = './logs/MAIN_LOG.log'
    root_logger = logging.getLogger(__name__)
    root_logger.setLevel(logging.DEBUG)
    file_handler = logging.FileHandler(log_file)
    log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(log_formatter)
    root_logger.addHandler(file_handler)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)
    return root_logger


def timeit(method):
    """Custom timeit profiling function."""
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print '%r - runtime : %2.2f ms' % \
                  (method.__name__, (te - ts) * 1000)
        return result
    return timed


def checkTimeOutException(query_func):
    done = False
    while not done:
      try:
        query_func()
        done = True
      except TimeoutError:
        pass
    return
    

def slice_results(results_json, size):
    """Slice the query response results to a specified size"""
    with open(results_json) as f:
        data = json.load(f)
    sliced_dict = dict()
    for msd_id in data.keys():
        if type(data[msd_id]['id'])==list:
            sliced_dict[msd_id] = {
                                'id': data[msd_id]['id'][:size], 
                                'score': data[msd_id]['id'][:size]
                                }
        else:
            sliced_dict[msd_id] = {'id': None, 'score': None}
    return sliced_dict


# some utits for accessing msd_metadata sql db
def init_connection(db_file):
    """Loads a sqldb file and returns the connection object"""
    try:
        import sqlite3
        con = sqlite3.connect(db_file)  # specifiy your path to sql db file provided by labrosa team
    except:
        raise ImportError("Cannot import db_file")
    return con


def get_fields_from_msd_db(db_file, field_name='track_id'):
    """
    Input : "track_metadata.db" sql db file provided by labrosa
    Output : A list of specified fields for 1M songs in the msd dataset
    """
    con = init_connection(db_file)
    query = con.execute("""SELECT %s from songs""" % field_name)
    results = query.fetchall()

    return [field[0] for field in results]


def get_msd_data_from_track_id(con, track_id, field_name='track_name'):
    query = con.execute("""SELECT %s from songs WHERE track_id='%s'""" % (field_name, track_id))
    results = query.fetchall()
    return [field[0] for field in results]


def get_msd_field_metadata_from_ids(db_file, track_ids, field_name='track_name'):
    con = init_connection(db_file)
    msd_ids = ','.join(['%d' % msd_id for msd_id in track_ids])
    query = con.execute("""SELECT %s from songs WHERE track_id IN (%s)""" %(field_name, msd_ids))
    results = query.fetchall()
    return [field[0] for field in results]


def plot_kde_precision_scores(title_prec, lyrics_prec, rerank_prec):
    import seaborn as sns
    sns.set(color_codes=True)
    sns.kdeplot(title_prec, label="title", shade=True)
    sns.kdeplot(lyrics_prec, label="lyrics", shade=True)
    sns.kdeplot(rerank_prec, label="title+lyrics_rerank", shade=True)
    sns.utils.plt.show()
    return


def get_nums_with_less(results_df, size=100):
    """Get results row in the datacframe with less than the size values"""
    nlist = list()
    ids = list()
    for items, res in results_df.iterrows():
        if len(res['id']) < size:
            nlist.append(len(res['id']))
            ids.append(items)
    return nlist, ids
