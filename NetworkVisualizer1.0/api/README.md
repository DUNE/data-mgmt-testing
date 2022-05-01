#README for es_client.py usage

##Purpose

es_client.py is used to pull information regarding Rucio transfers for the purpose
of monitoring the status of the overall network infrastructure related to Fermilab,
CERN, and affiliated organizations. It uses the ElasticSearch monitoring system
currently used to track information about Rucio transfers as its data source,
and is designed to produce files for use with the Network Visualizer code contained
in this same repository.

##Usage
python3 es_client.py --help returns:
```
usage: es_client.py [-h] [-S START_DATE] [-E END_DATE] [-C ES_CLUSTER] [-D DIRNAME] [-Z SEARCH_SIZE] [--debug-level DEBUG_LEVEL] [--force-overwrite] [--simultaneous-days SIMULTANEOUS_DAYS] [--show-timing]

optional arguments:
  -h, --help            show this help message and exit
  -S START_DATE, --start START_DATE
                        The earlest date to search for matching transfers. Defaults to today's date. Must be in form yyyy-mm-dd
  -E END_DATE, --end END_DATE
                        The latest date to search for matching transfers. Defaults to the same value as the start date, giving a 1 day
                        search. Must be in form yyyy-mm-dd
  -C ES_CLUSTER, --cluster ES_CLUSTER
                        Specifies the Elasticsearch cluster to target
  -D DIRNAME, --directory DIRNAME
                        Sets the cached searches directory
  -Z SEARCH_SIZE, --search-size SEARCH_SIZE
                        Number of results returned from Elasticsearch at once.
  --debug-level DEBUG_LEVEL
                        Determines which level of debug information to show. 1: Errors only, 2: Warnings and Errors, 3: Basic process info,
                        4: Advanced process info
  --force-overwrite     Sets whether existing files will be overwritten. Only advised for regularly running backend systems and maintenance, not live users.
  --simultaneous-days SIMULTANEOUS_DAYS
                        Defines how many days the client will attempt to handle simultaneously. Advise keeping low to avoid timeout errors. Defaults to 4.
  --show-timing         Shows timing information if set
```

##Additional information
Format for adding a new category of searching/processing to the overall program
```
<process type name> : {
	"file_format" : '<filetype name>_{}_{}_{}_to_{}_{}_{}.json',
	"conditions"  : {
		<Conditions for processing a transfer. See code for examples.>
	},
	"restrictions" : {
		<Restrictions on which transfers may be processed. See code for details.>
	},
	"min_condition_count" : <The minimum number of conditions that must be met to process a transfer>
	"max_restriction_count" : <The maximum number of restrictions which may be met to still process a transfer>
	"process_func" : <Function pointer to the function or generator associated with this processing type>
	"process_type" : <Whether this process type is a "function" or a "generator", formats detailed below>
}
```

Format for process type "function":
```
def funcname(individual_transfer):
	<Your code here>
	return json.dumps(<dictionary of transfer information>)
```

Format for process type "generator":
```
def gen_name(day_being_run):
	<Your code here>
	while True:
		input = yield
		if input["operation"] == "STORE":
			<Your code here>		
		elif input["operation"] == "GET":
			break
	for <all data>:
		<Your code here>
		yield new_entry
	<Your code here>
	yield "FINISHED"
```