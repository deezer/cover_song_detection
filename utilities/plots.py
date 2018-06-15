# -*- coding: utf-8 -*-
"""
Functions for various plots on the results json file
"""
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import json


def parse_results(jsonfile, method):
    """
    jsonfile : path to jsonfile with experiment results
    method : Any of the methods inside the list
        ['title_match', rerank_artist_id', 'lyrics_more_like']

    """
    with open(jsonfile) as f:
        data = json.load(f)
    sizes = list()
    mp = list()
    mper = list()
    for key, values in data.iteritems():
        if key == method:
            for d in values:
                sizes.append(d['size'])
                mp.append(d['map'])
                mper.append(d['mper'])

    return {"map": mp, "mper": mper, "size": sizes}


def plot_optimal_topN_pruning(results_json):
    with open(results_json) as f:
        data = json.load(f)
    results = [(int(key), value['map']) for key, value in data.iteritems()]
    results = sorted(results, key=lambda r: int(r[0]))
    sizes = [x[0] for x in results]
    metrics = [x[1] for x in results]

    # plt.title("Mean average precision of msd song-title experiment
    # on the SHS train against the MSD for various prune sizes")
    plt.plot(sizes, metrics)
    plt.xlabel("Prune size (k)")
    plt.ylabel("Mean Average Precision")
    plt.show()
    return


# functions for plotting some stats
def plot_lang_stats(msd_dzr_lang_csv, crop=True, barwidth=0.99, norm=False):
    """plots the histogram of language distribution"""
    lan_csv = pd.read_csv(msd_dzr_lang_csv)
    langs = lan_csv.lan.unique()
    freqs = list()
    for lan in langs:
        freqs.append(len(lan_csv[lan_csv.lan == lan]))
    sorted_tup = [(item[0], item[1]) for item in zip(langs, freqs)]
    sorted_tup.sort(key=lambda x: x[1], reverse=True)
    print "\n---Language stats for MSD---\n"
    for item in sorted_tup:
        print "%s : %s" % (item[0], (item[1]/1000000. * 100))

    langs = [item[0] for item in sorted_tup]
    freqs = [item[1] for item in sorted_tup]
    if norm:
        freqs = [(item/1000000.)*100 for item in freqs]
    if crop:
        langs = langs[:10]
        freqs = freqs[:10]

    # bar_locs = np.arange(1+barwidth, len(langs))
    # Plotting histogram
    plt.title("Histogram of top 10 languages in the MillionSongDataset")
    ax = plt.subplot(111)
    bins = map(lambda x: x, range(1, len(freqs)+1))
    ax.bar(bins, freqs, width=barwidth)
    ax.set_xticks(map(lambda x: x, range(1, len(langs)+1)))
    ax.set_xticklabels(langs, rotation=0)
    ax.set_xlabel("Language")
    ax.set_ylabel("Percentage (%)")
    plt.show()
    return


def plot_results_boxplot(jsonfile, metric='map'):
    """
    map = mean average precision
    """
    title_info = parse_results(jsonfile, method='title_match')
    rerank_info = parse_results(jsonfile, method='rerank_artist_id')
    lyrics_info = parse_results(jsonfile, method='lyrics_more_like')

    data = pd.DataFrame({"song-title": title_info[metric],
                         "artist_id_rerank": rerank_info[metric],
                         "lyrics": lyrics_info[metric]})

    ax = sns.boxplot(data=data,
                     palette="Set2",
                     orient="v",
                     order=["song-title", "artist_id_rerank", "lyrics"])

    ax.set_xlabel("search-method")
    if metric == 'map':
        ax.set_ylabel("mean average precision (MAP)")
    if metric == 'mper':
        ax.set_ylabel("mean percentage of covers (MPER)")
    sns.utils.plt.show()
    return
