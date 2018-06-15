# es_msd_modules

Methods to interact with  ElasticSearch MSD augmented index

# Requirements

Install python dependencies from requirements.txt file

```
pip install -r requirements.txt
```

# Setup

* Fill host, port and index information of the ES db in the [templates.py](templates.py) file


# Usage


## Modular mode

In this section, you can have a glimpse on how to use these classes and various methods for doing experiments

```python
#import modules
from es_search import SearchModule
from experiments import Experiments
import templates as presets

# Initiaite es search class
es = SearchModule(presets.uri_config)

# search method by msd_track title in view mode
results = es.search_by_exact_title('Listen To My Babe', 'TRPIIKF128F1459A09', mode='view')

#You can also use the experiment class to automate particular experiments for a method
#Initiate experiment class with the instance of SearchModule and path to the dataset as arguments
exp = Experiments(es, './data/test_shs.csv')

#run the song title match experiment with top 100 results
results = exp.run_song_title_match_task(size=100)

#compute evaluation metrics for the task
mean_avg_precison = exp.mean_average_precision(results)
```


## Evaluation tasks

Some examples for using functions in evaluations.py script to reproduce the results mentioned in the paper
```python
from evaluations import *

#Evaluation task on SHS train set against the whole MSD (1 x 999,999 songs)
shs_train_set_evals(size=100, method="msd_title", mode="msd", with_duplicates=True)

#You can specify various prune sizes and methods as parameters
shs_train_set_evals(size=1000, method="mxm_lyrics", mode="msd", with_duplicates=False)

#You can run the same experiment only on the SHS train set against itself by specifying "mode" param as "shs" (1 x 12,960)
shs_train_set_evals(size=100, method="msd_title", mode="shs", with_duplicates=True)

#In same way you can do the evaluation experiments on SHS test sets
shs_test_set_evals(size=100, method="title_mxm_lyrics", with_duplicates=True)

```


If you don't want to care about how the module works and you only need results various experiments, then this is for you. 
It's a wrapper around the modules to run automated experiments and save the results to a .log file or a json_template. 
The experiments are multi-threaded and able to run from terminal using command-line arguments.

```bash
$ python evaluations.py -m test -t -1 -e msd -d 0 -s 100
```

# Documentation

### [SearchModule](es_search.py)

Methods for doing various search queries on the ES MSD augmented    index for the cover detection task


### [Experiments](experiments.py)

Methods for doings pre-defined experiments for cover detection. A wrapper around es_search.py

#### Tasks
MSD song title

Pre-processed MSD song title

MXM lyrics

MSD song title + MXM lyrics

MSD song title + MXM lyrics + Offline Audio re-ranking


#### Metrics

Mean Average Precision

### [Evaluation](evaluations.py)

Scripts for doing automated experiments for pre-defined evaluation methods

### [JSON Templates](templates.py)

ES [query-dsl](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html) templates for doing various queries on the ES MSD augmented index.


