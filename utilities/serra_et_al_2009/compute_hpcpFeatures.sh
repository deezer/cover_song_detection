#!/bin/bash

if [ "$#" -ne 3 ]; then
        echo "USAGE: ./get_hpcpFeatures.sh <collection_list_file> <working_directory> <output_log_filename> "
        exit
fi

echo "Extracting descriptors..."
./myessentiaextractor -sl $1 -op $2 -dn hpcp -ah 20 -al 20 -at divmax > $2$3

echo "Done."
