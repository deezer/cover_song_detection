#!/bin/bash

if [ "$#" -ne 4 ]; then
	echo "USAGE: ./run_submission.sh <collection_list_file> <query_list_file> <working_directory> <output_file> "
	exit
fi

#echo "Creating temporary directory"
#mkdir $3

echo "Extracting descriptors..."
./myessentiaextractor -sl $1 -op $3 -dn hpcp -ah 20 -al 20 -at divmax > $3/log_feature_extraction.txt

echo "Computing distances..."
./coverid -d qmax -q $2 -c $1 -p $3 -oti 2 -m 9 -tau 1 -k 0.095 -go 0.5 -ge 0.5 > $3/log_temporaryresults.txt

echo "Refining distances..."
./setdetect -rf $3/log_temporaryresults.txt -nn 1 -dt 1000.0 > $4

echo "Removing logs and temporary files..."
rm -r $3

echo "Done."
