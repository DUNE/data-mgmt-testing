# README for xroot_es_client.py and rucio_es_client.py

### Purpose


xroot_es_client.py and rucio_es_client.py is used to pull information regarding SAM and Rucio events respectively related to DUNE. xroot_es_client.py specifically is capable of being adapted to work with other experiments by changing a single command line argument. xroot_es_client.py use an Elasticsearch client to pull information about individual SAM events and the SAM system to pull information regarding specific projects as well as file metadata. rucio_es_client.py uses just the Elasticsearch client to pull information on individual successful and failed data transfer events. The outputs are designed to be compatible with the Network Visualizer code that is also currently part of the data-mgmt-testing repository.


### Usage


"python3 xroot_es_client.py --help" returns:

```
usage: xroot_es_client.py [-h] [-S START_DATE] [-E END_DATE] [-Z SEARCH_SIZE] [-U USER] [-X EXPERIMENT] [-C ES_CLUSTER]
                          [-D DIRNAME] [--debug-level DEBUG_LEVEL] [--show-timing] [--simultaneous-pids SIMULTANEOUS_PIDS]
                          [--output-for-display] [--sitename-file SITENAME_FILE] [--clear-raws] [--display-aggregate]
                          [--make-csvs]

optional arguments:
  -h, --help            show this help message and exit
  -S START_DATE, --start START_DATE
                        The earlest date to search for matching transfers. Defaults to today's date. Must be in form yyyy-mm-dd
  -E END_DATE, --end END_DATE
                        The latest date to search for matching transfers. Defaults to the same value as the start date, giving
                        a 1 day search. Must be in form yyyy-mm-dd
  -Z SEARCH_SIZE, --size SEARCH_SIZE
                        Number of results returned from Elasticsearch at once. Must be between 1 and 1,000 to accomodate limits
                        of SAM metadata access
  -U USER, --user USER  Searches for a specific user
  -X EXPERIMENT, --experiment EXPERIMENT
                        Searches for a specific experiment
  -C ES_CLUSTER, --cluster ES_CLUSTER
                        Specifies the Elasticsearch cluter to target
  -D DIRNAME, --directory DIRNAME
                        Sets the cached searches directory
  --debug-level DEBUG_LEVEL
                        Determines which level of debug information to show. 1: Errors only, 2: Warnings and Errors, 3: Basic
                        process info, 4: Advanced process info
  --show-timing         Shows timing information if set
  --simultaneous-pids SIMULTANEOUS_PIDS
                        Defines how many project IDs the client will attempt to handle simultaneously
  --output-for-display  If set, also outputs a file for use with the Network Visualizer frontend
  --sitename-file SITENAME_FILE
                        File to pull node-site associations from. Only needed if compiling for display
  --clear-raws          If set, deletes all raw files from this run after summarizing them
  --display-aggregate   If set, writes all events for display compilation instead of auto-summarizing
  --make-csvs           If set, also writes summary information to CSV files
```

"python3 rucio_es_client.py --help" returns:

```
usage: rucio_es_client.py [-h] [-S START_DATE] [-E END_DATE] [-C ES_CLUSTER]
                          [-D DIRNAME] [-Z SEARCH_SIZE]
                          [--debug-level DEBUG_LEVEL] [--force-overwrite]
                          [--simultaneous-days SIMULTANEOUS_DAYS]
                          [--show-timing]

optional arguments:
  -h, --help            show this help message and exit
  -S START_DATE, --start START_DATE
                        The earlest date to search for matching transfers.
                        Defaults to today's date. Must be in form yyyy-mm-dd
  -E END_DATE, --end END_DATE
                        The latest date to search for matching transfers.
                        Defaults to the same value as the start date, giving a
                        1 day search. Must be in form yyyy-mm-dd
  -C ES_CLUSTER, --cluster ES_CLUSTER
                        Specifies the Elasticsearch cluster to target
  -D DIRNAME, --directory DIRNAME
                        Sets the cached searches directory
  -Z SEARCH_SIZE, --search-size SEARCH_SIZE
                        Number of results returned from Elasticsearch at once.
  --debug-level DEBUG_LEVEL
                        Determines which level of debug information to show.
                        1: Errors only, 2: Warnings and Errors, 3: Basic
                        process info, 4: Advanced process info
  --force-overwrite     Sets whether existing files will be overwritten. Only
                        advised for regularly running backend systems and
                        maintenance, not live users.
  --simultaneous-days SIMULTANEOUS_DAYS
                        Defines how many days the client will attempt to
                        handle simultaneously. Advise keeping low to avoid
                        timeout errors. Defaults to 4.
  --show-timing         Shows timing information if set
```

At minimum, input the start and end dates ('Year-Month-Day') for the search when running the script. The end date will default to the present day if not specified.

```
python3 xroot_es_client.py -S 'XXXX-XX-XX' -E 'XXXX-XX-XX'

python3 rucio_es_client.py -S 'XXXX-XX-XX' -E 'XXXX-XX-XX'

```

### Additional information

**External Documentation**
Top-level Python Elasticsearch client: https://elasticsearch-py.readthedocs.io/en/v7.10.1/

Python Elasticsearch Scoll API: https://elasticsearch-py.readthedocs.io/en/7.10.0/api.html?highlight=scroll#elasticsearch.Elasticsearch.scroll

Elasticsearch search API, including basic query template: https://www.elastic.co/guide/en/elasticsearch/reference/7.10/search-search.html

Elasticsearch wildcard query: https://www.elastic.co/guide/en/elasticsearch/reference/7.10/query-dsl-wildcard-query.html

Elasticsearch filter query: https://www.elastic.co/guide/en/elasticsearch/reference/7.10/query-filter-context.html

Elasticsearch query sorting: https://www.elastic.co/guide/en/elasticsearch/reference/7.10/sort-search-results.html

Elasticsearch boolean query (includes and/or type parameters): https://www.elastic.co/guide/en/elasticsearch/reference/7.10/query-dsl-bool-query.html

Additional Elasticsearch query information: https://www.elastic.co/guide/en/elasticsearch/reference/7.10/query-dsl.html

Tutorial for use of the Elasticsearch scroll API: https://simplernerd.com/elasticsearch-scroll-python/

SAM web client github repository: https://github.com/DUNE/sam-web-client/tree/master