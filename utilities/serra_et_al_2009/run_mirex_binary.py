from joblib import Parallel, delayed
import time, os
import subprocess
import argparse
import glob


def compute_features(args):
    ''''''
    print args
    start_time = time.time()
    collections_txt = args[0]
    feature_path = args[1]
    logfile_path = args[2]
    subprocess.call("./compute_hpcpFeatures.sh %s %s %s" %(collections_txt, feature_path, logfile_path), shell=True)
    print "Feature extraction finished in --%s-- s" %(time.time() - start_time)
    return


def compute_qmax_distance(args):
    ''' '''
    print args
    collections_txt = args[0]
    query_txt = args[1]
    directory = args[2]
    log_filename = args[3]
    output_filename = args[4]
    return subprocess.call("./compute_qmaxDistance.sh %s %s %s %s %s" %(collections_txt, query_txt, directory, log_filename, output_filename), shell=True)



def serra_cover_algo(collections_txt, query_txt, directory, output_filename):

    feature_process = compute_features(collections_txt, query_txt, directory)

    if feature_process!=0:
        raise Exception("Feature extraction process failed ...")

    qmax_process = compute_qmax_distance(collections_txt, query_txt, directory, output_filename)

    if qmax_process!=0:
        raise Exception("Process failed...")
    return



def run_feature_extraction(collection_directory, feature_directory):
    '''Run the feature extraction with parallelisation'''
    collection_files = os.listdir(collection_directory)
    for s in collection_files:
    	if s.startswith("."):
    		collection_files.remove(s)
    collection_files = [collection_directory+s for s in collection_files]
    collection_files = sorted(collection_files, key = lambda x: int(x.split('_')[2].split('/')[1]))
    print "%s collections txt files found..." %len(collection_files)
    feature_path = [feature_directory for i in range(len(collection_files))]
    log_file_paths = ['hpcp_logs_split_'+str(i)+'.txt' for i in range(len(collection_files))]

    args = zip(collection_files, feature_path, log_file_paths)
    Parallel(n_jobs=-1, verbose=1)(map(delayed(compute_features), args))
    return


def run_qmax_computation(col_path, query_path, feature_path, out_path):
    '''Run the qmax distance computation with parallelization'''
    collection_files = os.listdir(col_path)
    query_files = os.listdir(query_path)
    for s in collection_files:
        if s.startswith("."):
            collection_files.remove(s)
    for x in query_files:
        if x.startswith("."):
            query_files.remove(x)
    collection_files = sorted(collection_files, key = lambda m: int(m.split('_')[1]))
    query_files = sorted(query_files, key = lambda m: int(m.split('_')[1]))

    collection_files = [col_path+c for c in collection_files]
    query_files = [query_path+q for q in query_files]

    feature_directory = [feature_path for i in range(len(query_files))]
    log_filenames = [feature_path+'qmax_log_'+str(i)+'.txt' for i in range(len(query_files))]
    out_filenames = [out_path+'output_qmax_'+str(i)+'.txt' for i in range(len(query_files))]

    args = zip(collection_files, query_files, feature_directory, log_filenames, out_filenames)
    Parallel(n_jobs=-1, verbose=1)(map(delayed(compute_qmax_distance), args))
    return


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description= "Run mirex cover similarity algorithm (serra et. al 2009) binary files with parallelisation",
                        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("-a", action="store", default='./audio_collections/',
                        help="path to collection_files for audio feature extraction")
    parser.add_argument("-c", action="store", default='./collection_txts/',
                        help="path to collection_files for qmax")
    parser.add_argument("-q", action="store", default='./query_txts/',
                        help="path to query_files for qmax")
    parser.add_argument("-p", action="store", default="./output_features/",
                        help="path to directory where the audio features should be stored")
    parser.add_argument("-o", action="store", default='./qmax_output/',
                        help="output_filename")
    parser.add_argument("-m", action="store", default=0,
                        help="mode of the process")

    cmd_args = parser.parse_args()

    #print cmd_args

    run_feature_extraction(cmd_args.a, cmd_args.p)

    print 'Feature extraction finished'

    run_qmax_computation(cmd_args.c, cmd_args.q, cmd_args.p, cmd_args.o)

    print "\n.....DONE...."

