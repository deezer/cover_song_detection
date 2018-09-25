if [ "$#" -ne 5 ]; then
	echo "USAGE: ./compute_qmaxDistance.sh <collection_list> <query_list> <working_directory> <log_filename> <output_filename>"
	exit
fi

echo "Computing Qmax"
./coverid -d qmax -q $2 -c $1 -p $3 -oti 2 -m 9 -tau 1 -k 0.095 -go 0.5 -ge 0.5 > $4

echo "Refining distances..."
./setdetect -rf $4 -nn 1 -dt 1000.0 > $5

echo "Done...."
