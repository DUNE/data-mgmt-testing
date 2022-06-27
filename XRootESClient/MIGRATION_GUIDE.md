## Migration Guide/Suggestions

This is a guide to (hopefully) help with migration of this code to work with whatever replaces SAM and/or Elasticsearch.

The code for xroot_es_client.py was written with full knowledge that the underlying systems (especially SAM) were likely to be replaced within the next few years. As such, it was designed to make migration as easy as possible. Hopefully it worked, and this is fairly seamless and painless. Hopefully this guide helps.



## Things to change
### **For SAM replacement**

Line 32:

Import statement for SAM web client

---
Line 74 to Line 75:

Initialization of timing variables for SAM-related actions

---
Line 92:

Initializes the variable controlling the number of SAM worker threads

---
Line 106:

Initializes the variable tracking the number of finished SAM worker threads

---
Line 119:

Initializes the variable for the SAM overseer thread

---
Line 222 in the function set_pid_count:

Sets the maximum simultaneous number of SAM worker threads

---
Line 244 to Line 245 in reset_vals:

Resets the timing data for SAM-related actions

---
Line 262 in reset_vals:

Resets the maximum number of SAM worker threads

---
Line 276 in reset_vals:

Resets the number of finished SAM worker threads

---
Line 289 in reset_vals:

Resets the variable for the SAM overseer thread to None

---
Line 299 and Line 301 in show_timing_info:

Prints timing information about SAM related actions

---
Line 319 in new_run:

Resets the number of finished SAM worker threads

---
Line 364 in new_run:

Initializes the samweb client. Hopefully the replacement will have some sort of analog

---
Line 374 in new_run:

Creates SAM overseer thread

---
Line 379 in new_run:

Starts the SAM overseer thread

---
Line 399 in new_run:

Resets the number of finished SAM worker threads (for a specific day)

---
Line 777 in compiler_worker_func:

The while loop relies on the sam_worker thread for a given PID to be alive

---
Line 864 to Line 895, the function sam_overseer_func:

This function handles the creation, running, and termination of SAM worker threads. While the general structure can probably remain the same (as it's just supposed to manage the threads rather than interact directly with SAM), it should still be given a once-over, for renaming if nothing else.

---
Line 897 to Line 931, the function sam_worker_func:

This function directly interacts with SAM by pulling metadata for a list of file IDs (acquired by ElasticSearch). The main line that will need to be changed is Line 908, which is where the actual request and response happen, but the entire function should be given a once-over. I highly recommend seeing if there's an analog for the getMultipleMetadata function as requesting the metadata in bulk is extremely helpful for overall speedup. Response times for single-file requests are typically in the tens of milliseconds, which adds up fast, and was one of the major limiting factors in speed for this program's prototype code.

---
Line 1059 in es_worker_func:

This line creates the ElasticSearch index for a given search. This is currently "sam-events-v1-yyyy.mm", which will almost certainly change with a new system.

---
Line 1094 to Line 1116, the function get_proj_list:

This function gets a list of projects that occurred in a certain date range from SAM. It then gets the project summary from SAM and creates all of the necessary objects to process that project under the corresponding project ID in self.pids.

---
Line 1131:

The --size argument is currently supposed to be between 1 and 1000 and defaults to 1000 as SAM's multiple file metadata pull can only accommodate 1000 records at once. In this case, more is better since it means a lower overall run time for the SAM worker threads, so if the new system allows for more results to be pulled at once, I recommend using that higher number.

## Information Required for SAM replacement
The following are the specific data fields needed for different parts of SAM-related processing as well as where they fit into this code. There are enough that it will likely be more helpful to point at their position in the code than to list them all individually.


**Lines 468 through 478, and 565 through 575**
All instances of self.fids[curr_fid][field] are retrieving metadata about specific file metadata from SAM. All of this data is retrieved by an instance of sam_worker_func. The fields "data_tier" and "DUNE.campaign" are not strictly necessary and their absence is currently handled. "file_size", however, is a required field.

Lines 565 through 575 are more or less the same as 468 as 478 but specifically account for the last file ID in a file.


**Line 813 through Line 841:**
All fields whose right element takes the form next_file[field] are from ElasticSearch and shouldn't need changing in the event of a SAM replacment.

All fields whose right element takes the form self.pids[pid]["metadata"][field] are retrieving metadata about a specific project rather than a file. All of this data is stored by get_proj_list in a class-level dictionary.



### **Line Changes For ElasticSearch replacement**

Line 20:

Import statement for ElasticSearch
---

Line 131 in the function check_args:

Sets the default argument for the ElasticSearch cluster being used
---

Line 965 to Line 1093, the function es_worker_func:

This function will likely need to be completely rewritten to work with whatever replacement is used. It takes a project ID and references the current and target dates, constructs a query for the ElasticSearch cluster, then uses the scroll function/generator (defined and initialized within the es_worker_func) to request all matching records from the ElasticSearch cluster. It then appends each result to a thread-safe data queue (self.pids[pid]["write_queue"]).



## Information Required for ElasticSearch replacement
The following are the specific data fields needed for different parts of SAM-related processing as well as where they fit into this code. There are enough that it will likely be more helpful to point at their position in the code than to list them all individually.

**Line 175 to Line 216:**
All instances of item[datafield] are taken from ElasticSearch-stored data. The data is about specific SAM events and files involved in a specific project.






I believe that should cover everything needed for a fairly smooth migration. Apologies if I missed anything. Best of luck to you! 