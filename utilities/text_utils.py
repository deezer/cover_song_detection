# -*- coding: utf-8 -*-
"""
Set of functions for text processing

* Format msd_track titles for song-title based query of cover song detection
---------------------
Albin Andrew Correya
R&D Intern
@Deezer, 2017
"""

from nltk.stem import SnowballStemmer
from fuzzywuzzy import fuzz, process
from utils import init_connection, timeit
import re
import csv
# [To be removed in future. Just a small hack to get the things done for at the moment]
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

stemmer = SnowballStemmer("english")
codebook = ['version', 'demo', 'live', 'remix', 'mix', 'remaster', 'albumn', 'instrumental', 'cover', 'digital', 
            'acoustic', 'lp version', 'remastered', 'digital remaster', 'remastered lp version', 'mono', 'stereo', 
            'extended', 'vocal mix', 'album version', 'album', 'vocal', 'extended version', 'reprise', 'single', 
            'radio edit', 'short version', 'explicit', 'bonus track', 'edit', 'session', 'e.p', 'ep version',
            'original']


def stemit(string):
    """apply stemming to a string based on nltk.stem.SnowballStemmer()"""
    return stemmer.stem(unicode(string))


def title_formatter(string, mode='regex', striplist=codebook, threshold=70):
    """
    Remove elements similar to predefined items inside the elements of string with parenthesis
    Note : callback function to be used inside pandas.dataframe.apply() and have been used recursively

    Inputs :
            mode : choose either of one mode from ['regex', 'fuzzy']
                   'regex' : uses regex matching
                   'fuzzy' : uses fuzzy levenstien distance

            striplist : A list of strings which the match have to be computed
                        eg : ['version', 'live', 'remix', 'mix', 'remaster', 'albumn',
                              'instrumental', 'cover', 'digital', 'acoustic', 'lp version', 'remastered',
                              'digital remaster', 'remastered lp version', 'mono', 'stereo']

    eg : >>> string = Let it be (Live Version)
         >>> title_formatter(string)
         out: "Let it be"
    """
    # to avoid 'NaN' values appear in the pandas dataframe when applying it as a callback function
    if type(string) != float:
        to_remove = "(" + string[string.find("(")+1:string.find(")")] + ")"
        stemmed_str = stemit(to_remove)
        for word in striplist:
            if mode == 'fuzzy':
                if fuzz.ratio(stemmed_str, word) >= threshold:
                    return string.replace(to_remove, "")
            if mode == 'regex':
                # strip the string with parse string if there is any match with the words in the striplist
                if re.findall(r"\b" + word + r"\b", stemit(string)): 
                    return string.replace(to_remove, "")
    return string


@timeit
def add_formatted_title_to_dataset(dataset_csv, mode='regex'):
    """
    dataset_csv : shs csv file
    mode : choose either of one mode from ['regex', 'fuzzy']
    """
    dataset = pd.read_csv(dataset_csv)
    new_data = pd.DataFrame()
    new_data['new_title'] = dataset.title.apply(title_formatter, mode=mode)
    new_data.new_title = new_data.new_title.apply(title_formatter, mode=mode)
    new_data = new_data.merge(dataset, left_index=True, right_index=True)
    return new_data


@timeit
def get_formatted_msd_track_title_csv(db_file, filename='./msd_formatted_titles.csv'):
    """
    Remove and reformat all the msd song titles and store it as csvfile.
    The output csv file is structured as follows :
        msd_track_id, msd_song_title, msd_new_song_title

    Inputs :
            db_file - track_metadata.db file provided by the labrosa
            filename - filename for the output csvfile ('./msd_formatted_titles.csv' by default)


    [NOTE] : tested runtime ~ 33.76 minutes
    """

    def double_format(string):
        s = title_formatter(string)
        return title_formatter(s)
    
    con = init_connection(db_file)
    query = con.execute("""SELECT track_id, title FROM songs""")
    results = query.fetchall()
    con.close()
    with open(filename, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['msd_id', 'msd_title', 'title'])
        writer.writeheader()
        cnt = 0
        for track_id, track_name in results:
            print "--%s--" % cnt
            if track_name:
                title = double_format(track_name).encode('utf8')
            writer.writerow({'msd_id': track_id, 
                             'msd_title': track_name,
                             'title': title})
            cnt += 1
    print "~Done..."
    return 


def extract_removefactor(string):
    if type(string) != float:
        return "(" + string[string.find("(")+1:string.find(")")] + ")"
    return string
