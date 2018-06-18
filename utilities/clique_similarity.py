# -*- coding: utf-8 -*-
"""
Set of functions to compute the similarity of song titles with in and outside it's clique.
~
Albin Andrew Correya
R&D Intern
@Deezer, 2018
"""
from itertools import combinations
from Levenshtein import ratio
import numpy as np
import pandas as pd
import random


def get_clique_similarity_same_set(dataset_csv):
    """Compute Levenshtein similarity of song titles in same cliques in SHS"""
    dataset = pd.read_csv(dataset_csv)
    clique_ids = dataset.work_id.unique().tolist()
    clique_sims = list()
    for work_id in clique_ids:
        song_titles = dataset.title[dataset.work_id == work_id].values.tolist()
        distances = list()
        for (title1, title2) in combinations(song_titles, 2):
            measure = ratio(title1, title2)
            distances.append(measure)
        clique_sims.append(np.mean(distances))
    return clique_sims


def get_clique_similarity_dif_set(dataset_csv):
    """Compute Levenshtein similarity of song titles in different cliques in SHS"""
    distances = list()
    dataset = pd.read_csv(dataset_csv)
    clique_ids = dataset.work_id.unique().tolist()
    all_titles = dataset.title.values.tolist()

    for i in range(len(clique_ids)):
        ref_title = random.choice(all_titles)
        clique_id = dataset.work_id[dataset.title == ref_title].values[0]
        ref_titles = dataset.title[dataset.work_id != clique_id].values.tolist()
        com_title = random.choice(ref_titles)
        distance = ratio(ref_title, com_title)
        distances.append(distance)

    return distances


def plot_clique_similarity_dist(dataset_csv):
    """Plot the distribution plot of string similarities within and outside its clique"""
    import matplotlib.pyplot as plt
    import seaborn as sns
    palette = ["#000000", "#737170"]
    sns.set_palette(palette)
    sim_same_clique = get_clique_similarity_same_set(dataset_csv)
    sim_dif_clique = get_clique_similarity_dif_set(dataset_csv)
    sns.distplot(sim_same_clique, hist=True,
                 kde_kws={"lw": 1, "label": "within same clique"})
    sns.distplot(sim_dif_clique, hist=True,
                 kde_kws={"lw": 1, "label": "within different clique"})
    plt.xlabel("Similarity measure")
    plt.ylabel("Density")
    plt.show()
    return
