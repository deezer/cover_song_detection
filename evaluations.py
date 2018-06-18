# -*- coding: utf-8 -*-
"""
Scripts for running various evaluation tasks for the task of large-scale cover song detection

[NOTE] : All the logs are stored to the LOG_FILE

Albin Andrew Correya
R&D Intern
@Deezer
"""

from joblib import Parallel, delayed
from es_search import SearchModule
from experiments import Experiments
from utils import log
import templates as presets
import argparse

# Logging handlers
LOG_FILE = './logs/evaluations.log'
LOGGER = log(LOG_FILE)


def shs_train_set_evals(size, method="msd_title", with_duplicates=True, mode="msd"):
    """
    :param size: Required prune size of the results
    :param method: (string type) {default:"msd_title"}
        choose the method of experiment available modes are
        ["msd_title", "pre-msd_title", "mxm_lyrics", "title_mxm_lyrics", "pre-title_mxm_lyrics"]
    :param with_duplicates: (boolean) {default:True} include
        or exclude MSD official duplicate tracks from the experiments
    :param mode: 'msd' or 'shs'
    """

    es = SearchModule(presets.uri_config)

    if mode == "msd":
        if with_duplicates:
            exp = Experiments(es, './data/train_shs.csv', presets.shs_msd)
        else:
            exp = Experiments(es, './data/train_shs.csv', presets.shs_msd_no_dup)
    elif mode == "shs":
        exp = Experiments(es, './data/train_shs.csv', presets.shs_shs)
    else:
        raise Exception("\nInvalid 'mode' parameter ... ")

    if method == "msd_title":
        LOGGER.info("\n%s with size %s, duplicates=%s and msd_mode=%s" %
                    (method, size, with_duplicates, mode))
        results = exp.run_song_title_match_task(size=size)

    elif method == "pre-msd_title":
        LOGGER.info("\n%s with size %s, duplicates=%s and msd_mode=%s" %
                    (method, size, with_duplicates, mode))
        results = exp.run_cleaned_song_title_task(size=size)

    elif method == "mxm_lyrics":
        LOGGER.info("\n%s with size %s, duplicates=%s and msd_mode=%s" %
                    (method, size, with_duplicates, mode))
        results = exp.run_mxm_lyrics_search_task(presets.more_like_this, size=size)

    elif method == "title_mxm_lyrics":
        LOGGER.info("\n%s with size %s, duplicates=%s and msd_mode=%s" %
                    (method, size, with_duplicates, mode))
        results = exp.run_rerank_title_with_mxm_lyrics_task(size=size, with_cleaned=False)

    elif method == "pre-title_mxm_lyrics":
        LOGGER.info("\n%s with size %s, duplicates=%s and msd_mode=%s" %
                    (method, size, with_duplicates, mode))
        results = exp.run_rerank_title_with_mxm_lyrics_task(size=size, with_cleaned=True)

    else:
        raise Exception("\nInvalid 'method' parameter....")

    mean_avg_precision = exp.mean_average_precision(results)
    LOGGER.info("\n Mean Average Precision (MAP) = %s" % mean_avg_precision)

    return


def shs_test_set_evals(size, method="msd_title", with_duplicates=True):
    """
    :param size: Required prune size of the results
    :param method: (string type) {default:"msd_title"}
        choose the method of experiment available modes are
        ["msd_title", "pre-msd_title", "mxm_lyrics", "title_mxm_lyrics", "pre-title_mxm_lyrics"]
    :param with_duplicates: (boolean) {default:True} include
        or exclude MSD official duplicate tracks from the experiments
    :return:
    """

    es = SearchModule(presets.uri_config)

    if with_duplicates:
        exp = Experiments(es, './data/test_shs.csv', presets.shs_msd)
    else:
        exp = Experiments(es, './data/test_shs.csv', presets.shs_msd_no_dup)

    if method == "msd_title":
        LOGGER.info("\n%s with size %s and duplicates=%s " % (method, size, with_duplicates))
        results = exp.run_song_title_match_task(size=size)

    elif method == "pre-msd_title":
        LOGGER.info("\n%s with size %s and duplicates=%s" % (method, size, with_duplicates))
        results = exp.run_cleaned_song_title_task(size=size)

    elif method == "mxm_lyrics":
        LOGGER.info("\n%s with size %s and duplicates=%s" % (method, size, with_duplicates))
        results = exp.run_mxm_lyrics_search_task(presets.more_like_this, size=size)

    elif method == "title_mxm_lyrics":
        LOGGER.info("\n%s with size %s and duplicates=%s" % (method, size, with_duplicates))
        results = exp.run_rerank_title_with_mxm_lyrics_task(size=size, with_cleaned=False)

    elif method == "pre-title_mxm_lyrics":
        LOGGER.info("\n%s with size %s and duplicates=%s" % (method, size, with_duplicates))
        results = exp.run_rerank_title_with_mxm_lyrics_task(size=size, with_cleaned=True)

    else:
        raise Exception("\nInvalid 'method' parameter for the experiment ! ")

    mean_avg_precision = exp.mean_average_precision(results)
    LOGGER.info("\n Mean Average Precision (MAP) = %s" %mean_avg_precision)

    return


def automate_online_evals(mode, n_threads=-1, exp_mode="msd", is_duplicates=False, size=100,
                          methods=["msd_title", "pre-msd_title", "mxm_lyrics",
                                   "title_mxm_lyrics", "pre-title_mxm_lyrics"]):
    """

    Run the paralleled automated evaluation tasks as per the chosen requirements from the parameters

    :param mode: (type : string) chose whether train or test mode from the list ["test", "train"]
    :param n_threads: number of threads to parallelize with (-1
    :param exp_mode: (type : string) Choose experiment mode from the list ["msd", "shs"]
    :param is_duplicates: (type : boolean)  Choose whether you should include duplicates in the experiments
    :param size: (type : int) Required size of the pruned response
    :param methods: Choose a list of methods to compute in the automated process
        available methods are ["msd_title", "pre-msd_title",
        "mxm_lyrics", "title_mxm_lyrics", "pre-title_mxm_lyrics"]

    """
    LOGGER.info("\n ======== Automated online experiments on shs_ %s "
                "with exp_mode %s and duplicates %s size %s ======= "
                % (mode, exp_mode, is_duplicates, size))

    sizes = [size for i in range(len(methods))]
    duplicates = [is_duplicates for i in range(len(methods))]

    if mode == "test":
        args = zip(sizes, methods, duplicates)
        Parallel(n_jobs=n_threads, verbose=1)(map(delayed(shs_test_set_evals), args))

    if mode == "train":
        exp_modes = [exp_mode for i in range(len(methods))]
        args = zip(sizes, methods, duplicates, exp_modes)
        Parallel(n_jobs=n_threads, verbose=1)(map(delayed(shs_train_set_evals), args))

    LOGGER.info("\n ===== Process finished successfully... ===== ")

    return


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description="Run automated evaluation for cover song detection task mentioned in the paper",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("-m", action="store", default='test',
                        help="choose whether 'train' or 'test' mode")
    parser.add_argument("-t", action="store", default=-1,
                        help="number of threads required")
    parser.add_argument("-e", action="store", default='msd',
                        help="choose between 'msd' or 'shs' ")
    parser.add_argument("-d", action="store", default=0,
                        help="choose whether you want to exclude msd official duplicates song from the experiments")
    parser.add_argument("-s", action="store", default=100,
                        help="required prune size for the results")

    args = parser.parse_args()

    d = bool(args.d)
    methods = ["msd_title", "pre-msd_title", "mxm_lyrics", "title_mxm_lyrics", "pre-title_mxm_lyrics"]

    automate_online_evals(mode=args.m, n_threads=args.t, exp_mode=args.e, is_duplicates=d, size=args.s, methods=methods)

    print "\n ...Done..."
