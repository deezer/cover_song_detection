# Reproducing MIREX 2009 Cover Song Detection Algorithm results

# Requirements

* Download the binary codes of Serra et.al 2009 mirex submission from 
[here](http://www.iiia.csic.es/~jserra/downloads/2009_SerraZA_MIREX-Covers.tar.gz) and copy in the same directory.


# Document structure

(Note : Wildcard (\*) denotes the index of all the queries in the aggregrated results dataframe.
	   ie. [0 to 4252] in the case of shs_test_dzr dataset) 

## Query lists

```
./path_to_query_folder/query_*_.txt
```

## Collection lists
```
./path_to_collections_folder/collections_*_.txt
```

# Usage

```bash
$ python run_mirex_binary.py -a ./audio_collections/ -c ./collection_txts/ -q ./query_txts/ -p ./output_features/ -o ./qmax_output/
```

