## Migration Guide/Suggestions

This is a guide to (hopefully) help with migration of this code to work with whatever replaces Elasticsearch.

The code for es_client.py was written with full knowledge that a. Elasticsearch might be replaced at some point and b. That there were already discussions of changing how certain information was tracked and stored. As such, there was an effort to make it as easy as possible to swap out Elasticsearch with some other system with minimal effort on the part of the developer.


### What needs to be changed

Line 33 through Line 41, the scroll generator:

This will need major changes, if not complete removal and replacement. This generator contains the vast majority of the interactions between this code and the Elasticsearch cluster. It takes in the Elasticsearch client object initialized in check_args, the specific Elasticsearch index to search in, the body of the Elasticsearch query, the amount of time to keep a search alive for (the "scroll" argument), and the number of results to return at once. It utilizes the Elasticsearch scroll API (which is being deprecated soon as of writing this) to search through a larger set of results than allowed by the search_size parameter. It yields a list of size search_size or smaller so long as it still has results to scroll through, requesting a new set each time it's called again.

**If at all possible, I recommend emulating this generator with whatever replaces Elasticsearch**

One of the main advantages to this method is that it's completely encapsulated. The scroll ID (necessary for letting the cluster know where you left off) is completely internally handled, meaning you don't have to repeatedly return and re-pass a scroll_id parameter, and the way search_size limits how much data you end up processing at once (which is very convenient given the limit RAM on the virtual machines available at the time of writing). It all allows for very easy control of information flow, and has simplified everything for me quite a bit.

---
Line 451 in check_args:
The es_cluster argument is currently hardcoded to point at the Fermilab Elasticsearch cluster. It should probably be changed. Call it a hunch.

---
Line 480, in the function check_args:

	`self.client = Elasticsearch([self.args["es_cluster"]])`
initializes the Elasticsearch client and will need to be removed or replaced, depending on how the replacement needs to be initialized and handled.

---
Line 497 in the function day_overseer:

`index = f"rucio-transfers-v0-{y}.{m}"`

initializes the Elasticsearch index to be used on a certain day. There will almost certainly be an analog with whatever replaces Elasticsearch, and I recommend replacing it here for as silent a drop-in as possible.

---
Line 511 through Line 542 in day_overseer:
These lines define the Elasticsearch query used for a given day. This should be changed to whatever query and data format is used by its replacement.

---
Line 583 through Line 603 in the function es_worker:

It's unclear how much of this function will need to change (though a name change for clarity's sake might be advisable). However, there are two main things to be aware of. First, this function loops through a generator (the "scroll" function, which is not actually a member of the class), pulling a large list of data with each iteration. It then iterates through the list and pulls the actual data from each entry (accessed with `entry[_source]`) and puts it into a data queue. Each entry is in the form of a dictionary, as is the _source element within the entry. Specifics of what data needs to be passed on will be included later in this document.

---
Line 792 through Line 794 in the function show_timing_info:

While not strictly necessary for operation, these lines provide potentially useful statistics about overall runtime and time taken by various pieces of the program. Making sure it works with the new system might be useful.
