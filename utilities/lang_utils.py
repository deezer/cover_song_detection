"""
Set of functions to process text, documents and pandas datframe for language analysis

Language detection
Language translation

"""
from requests import get
import pandas as pd
import csv
import time
import sys
reload(sys)

sys.setdefaultencoding("ISO-8859-1")


# language utils
def get_lang_text(text):
    """
    Detects language of a input string by making a get request to dzr-language-analyser.
    (Code taken from dzr_utils)
    """
    res = get('http://dzr-analysis-08.sadm.ig-1.net:9000/detect?q=' + text)
    # print "\nResponse :", res.json()
    return res.json()['language']


def translate_text(text, lan='en'):
    """
    Translate a input string to a specified language using API
    By default translate to english

    [TODO]
    """
    return


def get_lang_fields_from_lyrics(dataset_csv):
    """
    Add language annotation to MSD from dzr_lyrics
    """
    ids = list()
    langs = list()
    lyrics = list()
    data = pd.read_csv(dataset_csv)
    data = data[data.dzr_content_text.notnull()].copy()
    cnt = 0
    for row in data.iterrows():
        print "----%s-----" % cnt
        try:
            ids.append(row[1].dzr_song_id)
            l_content = row[1].dzr_content_text[:1000].replace("%", "")
            langs.append(get_lang_text(l_content.encode('utf-8').strip()))
            lyrics.append(row[1].dzr_lyrics_id)
            cnt += 1
        except:
            ids.append(row[1].dzr_song_id)
            langs.append("UNKNOWN")
            lyrics.append(row[1].dzr_lyrics_id)
    return pd.DataFrame({'dzr_song_id': ids, 'dzr_lyrics_id': lyrics, 'content_lan': langs})


def add_lan_annotation_title(dataset_csv, field='dzr_song_title'):
    """
    Detect lan of the song from the song_title or from the album title
    """
    start_time = time.time()
    data = pd.read_csv(dataset_csv)
    data = data[data[field].notnull()].copy()
    cnt = 0
    ids = list()
    langs = list()
    with open('./title_lang.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['dzr_song_id', 'title_lan'])
        writer.writeheader()
        for row in data.iterrows():
            try:
                print "---%s---" % cnt
                ids.append(row[1].dzr_song_id)
                title = row[1].dzr_album_title.replace("%", "")
                lan = get_lang_text(title.encode('utf-8').strip())
                if lan == "UNKNOWN":  # check the lang of album title if song title detected as unknown
                    lan = get_lang_text(title.encode('utf-8').strip())
                langs.append(lan)
                writer.writerow({'dzr_song_id': row[1].dzr_song_id, 'title_lan': lan})
            except:
                ids.append(row[1].dzr_song_id)
                langs.append('UNKNOWN')
                writer.writerow({'dzr_song_id': row[1].dzr_song_id, 'title_lan': 'UNKNOWN'})
            cnt += 1
    print "\n..Process finished in -%s- seconds..." % (start_time - time.time())
    return pd.DataFrame({'dzr_song_id': ids, 'title_lan': langs})


def annotate_lang_from_msd(msd_db_file, msd_track_ids, out_filename='./msd_lang.csv'):
    from gen_utils import init_connection, get_msd_data_from_track_id

    # get all the msd track ids from the sql db
    start_time = time.time()
    with open(out_filename, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['msd_id', 'lan'])
        writer.writeheader()
        cnt = 0
        con = init_connection(msd_db_file)
        for track in msd_track_ids:
            print "---%s---" % cnt
            track_title = get_msd_data_from_track_id(con, track, field_name='title')
            track_title = track_title[0].replace("%", "")
            print track_title
            lan = get_lang_text(track_title.encode('utf-8').strip())
            writer.writerow({'msd_id': track, 'lan': lan})
            cnt += 1
    print "\n..Process finished in -%s- seconds..." % (start_time - time.time())
    return


def main():
    pass


if __name__ == '__main__':
    main()
