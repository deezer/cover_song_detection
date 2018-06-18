"""
utility functions for processing resutls file for audio reranking experiments
"""
from utils import timeit, log
import pandas as pd
import numpy as np
import os


logger = log('./logs/audio_logs.log')


def savelist_to_file(path_list, filename):
    """Write a list of string to a text file"""
    doc = open(filename, 'w')
    for item in path_list:
        doc.write("%s\n" % item)
    doc.close()
    return


def parse_mirex_output_txt(textfile):
    """
    Parse distance matrix from the text output of joan serra's cover song detection algorithm

    Input : path/to/the/textfile

    Output : pandas dataframe with query/candidates distance scores
    """
    text = open(textfile)
    data = text.readlines()
    array = list()
    m = None
    for lines in data:
        if lines.startswith("Dist"):
            m = True
        if m is True:
            if not lines.startswith("  Could not open"):
                array.append(lines)
    text.close()
    doc = open("../distanceMatrix.txt", "w")
    for lines in array:
        doc.write("%s\n" % lines)
    doc.close()
    df = pd.read_csv("../distanceMatrix.txt", index_col=0, skiprows=1, sep='\t')
    os.system('rm ../distanceMatrix.txt')
    return df.transpose()


@timeit
def results_json_to_enriched_results_df(results_json, dzr_msd_map_df):
    """
    [NOTE] - only need to be run once
    """
    collections = list()
    queries = list()
    results = pd.read_json(results_json)
    for i in range(len(results)):
        collections.extend(results.iloc[i].id)
        queries.append(results.iloc[i].msd_id)
    collections.extend(queries)
    df = pd.DataFrame({'msd_track_id': collections})
    df = df.drop_duplicates('msd_track_id')
    df_dzr = pd.merge(df, dzr_msd_map_df, on='msd_track_id', how='left')
    df_dzr['dzr_path'] = df_dzr.song_id.apply(get_dzr_sng_path_from_sng_id)
    return df_dzr



def results_json_to_query_collection_pairs(results_json, enriched_csv, col_path, query_path):
    """
    Create a set of query-collection text file pairs from a aggregrated es search
    results for running mirex (serra 2009)binary scripts
    Inputs :
            results_json :
            enriched_csv :
            col_path :
            query_path :
    """
    res = pd.read_json(results_json)
    map_data = pd.read_csv(enriched_csv)
    logger.info("Constructing query-collection text files from the results-%s to %s and %s"
                % (results_json, col_path, query_path))
    for i in range(len(res)):
        mid = res.iloc[i].msd_id
        rids = res.iloc[i].id
        if not rids:
            logger.debug("No response found for index %s" % i)
        qpaths = map_data.dzr_path[map_data.msd_track_id == mid].values[0]
        rpaths = [map_data.dzr_path[map_data.msd_track_id == rid].values[0] for rid in rids]
        qpaths = qpaths.replace("data", "mnt")
        rpaths = [string.replace("data", "mnt") for string in rpaths]
        savelist_to_file([qpaths, '\n'], query_path+'query_'+str(i)+'_.txt')
        savelist_to_file(rpaths, col_path+'collections_'+str(i)+'_.txt')
    return


def get_id_score_pairs_from_distance_df(distance_df, results_df, index):
    """
    Returns the new ranked response of msd_track_ids and audio similarity
    scores from a distance matrix of mirex 2009 binary output
    Inputs:
            distance_df :
            results_df :
            index :

    Outputs:
            res_ids :
            res_scores :
    """
    res_msd_ids = results_df.iloc[index].id
    sorted_df = distance_df.sort_values(1)
    new_ranked_idx = sorted_df.index.values

    if len(sorted_df) != len(res_msd_ids):
        logger.debug("Mismatch of response msd id length in index %s" % index)

    # new reranked response ids and scores from the audio similarity measures
    # note the index from the output_txt file starts with 1
    res_ids = [res_msd_ids[int(i)-1] for i in new_ranked_idx]
    res_scores = sorted_df[1].values.tolist()
    return res_ids, res_scores


def serra_output_txt_to_results_df(output_directory, results_json):
    """
    Read a collection of output_*.txt files from mirex 2009 binary
    output and aggregrate it to a pandas dataframe
    as required for metric computation scripts

    Inputs :
            output_directory : path to the folder with the output_*.txt files from the mirex binary scripts
            results_json : results_json
    """
    results_df = pd.read_json(results_json)
    output_files = os.listdir(output_directory)
    for t in output_files:
        if t.startswith('.') or not t.endswith('.txt'):
            output_files.remove(t)

    output_files = sorted(output_files, key=lambda x: int(x.split('_')[2].split('.')[0]))  # sort the filename list
    results = dict()
    cnt = 0
    error_files = list()
    for idx, txt_file in enumerate(output_files):
        print "--%s--%s" % (idx, txt_file)
        distance_df = parse_mirex_output_txt(output_directory+txt_file)
        if distance_df.shape[1] == 1:
            query_msd = results_df.index[idx]
            res_ids, res_scores = get_id_score_pairs_from_distance_df(distance_df, results_df, index=idx)
            results[query_msd] = {'id': res_ids, 'score': res_scores}
        else:
            cnt += 1
            query_msd = results_df.index[idx]
            results[query_msd] = {'id': results_df.iloc[idx].id, 'score': None}
            error_files.append(txt_file)
    logger.debug("\n%s files had errors with the output distance matrix.." % cnt)
    #print error_files
    return pd.DataFrame.from_dict(results, orient='index')
