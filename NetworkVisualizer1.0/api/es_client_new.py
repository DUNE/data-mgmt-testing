import json
import os
import sys
import time
import threading
import queue
import re


from pathlib import Path

#ElasticSearch API is needed for interaction with Fermilab's ElasticSearch system
from elasticsearch import Elasticsearch

from datetime import datetime
from dateutil import parser as date_parser
from dateutil.relativedelta import relativedelta


#Based on Simplernerd's tutorial here: https://simplernerd.com/elasticsearch-scroll-python/
#Scrolls through all of the results in a given date range
#scroll is a generator that will step through all of the page results for a
#given index. scroll maintains its internal state and returns a new page of results
#each time it runs.
def scroll(es, idx, body, scroll):
    global scroll_time
    page = es.search(index=idx, body=body, scroll=scroll, size=search_size)
    scroll_id = page['_scroll_id']
    hits = page['hits']['hits']
    while len(hits):
        yield hits
        start = time.perf_counter()
        page = es.scroll(scroll_id=scroll_id, scroll=scroll)
        end = time.perf_counter()
        scroll_time += end-start
        scroll_id = page['_scroll_id']
        hits = page['hits']['hits']


class ESMonitoringClient():
    def __init__(self):
        self.debug = 3

        self.day_semaphore = 7
        self.day_lock = threading.Lock()

        #If true, will display timing information for different aspects
        #of the program
        self.display_timing = False

        #Stores timing data
        self.times = {
            "run_time" : 0,
            "io_time" : 0,
            "elasticsearch_time" : 0
        }

        #Variable for individual run arguments
        self.args = {}

        #Stores the output directory name
        self.dirname = ""

        self.client = None

        '''
        dune_transfers_display_YYYY....
        dune_transfers_aggregates_display_YYYY...
        dune_network_checkup_display_YYYY...
        dune_failed_transfers_display_YYYY...
        dune_failed_transfers_aggregates_display_YYYY...
        dune_failed_network_checkup_display_YYYY...
        '''
        self.file_info = {
            "successful_transfers" : {
                "file_format" : 'dune_transfers_display_{}_{}_{}_to_{}_{}_{}.json',
                "conditions"  : {
                    "event_type" : r"transfer-done"
                },
                "restrictions" : {
                    "name" : r"1gbtestfile-"
                },
                "min_condition_count" : 1,
                "max_restriction_count" : 0,
                "process_func" : self.transfer_success,
                "process_type" : "function"
            },
            "aggregate_successes" : {
                "file_format" : 'dune_transfers_aggregates_display_{}_{}_{}_to_{}_{}_{}.json',
                "conditions"  : {
                    "event_type" : r"transfer-done"
                },
                "restrictions" : {
                    "name" : r"1gbtestfile-"
                },
                "min_condition_count" : 1,
                "max_restriction_count" : 0,
                "process_func" : self.aggregate_success,
                "process_type" : "generator"
            },
            "checkup_successes" : {
                "file_format" : 'dune_network_checkup_display_{}_{}_{}_to_{}_{}_{}.json',
                "conditions"  : {
                    "event_type" : r"transfer-done",
                    "name" : r"1gbtestfile-"
                },
                "restrictions" : {
                },
                "min_condition_count" : 2,
                "max_restriction_count" : 0,
                "process_func" : self.checkup_success,
                "process_type" : "function"
            },
            "failed_transfers" : {
                "file_format" : 'dune_failed_transfers_display_{}_{}_{}_to_{}_{}_{}.json',
                "conditions"  : {
                    "event_type" : r"transfer-failed",
                    "event_type" : r"transfer-submission_failed"
                },
                "restrictions" : {
                    "name" : r"1gbtestfile-"
                },
                "min_condition_count" : 1,
                "max_restriction_count" : 0,
                "process_func" : self.transfer_failed,
                "process_type" : "function"
            },
            "aggregate_failures" : {
                "file_format" : 'dune_failed_transfers_aggregates_display_{}_{}_{}_to_{}_{}_{}.json',
                "conditions"  : {
                    "event_type" : r"transfer-failed",
                    "event_type" : r"transfer-submission_failed"
                },
                "restrictions" : {
                    "name" : r"1gbtestfile-"
                },
                "min_condition_count" : 1,
                "max_restriction_count" : 0,
                "process_func" : self.aggregate_failed,
                "process_type" : "generator"
            },
            "checkup_failures" : {
                "file_format" : 'dune_failed_network_checkup_display_{}_{}_{}_to_{}_{}_{}.json',
                "conditions"  : {
                    "event_type" : r"transfer-failed",
                    "event_type" : r"transfer-submission_failed",
                    "name" : r"1gbtestfile-"
                },
                "restrictions" : {
                },
                "min_condition_count" : 2,
                "max_restriction_count" : 0,
                "process_func" : self.checkup_failed,
                "process_type" : "function"
            }
        }


    def check_args(self):
        #Defaults only applies to optional arguments
        default_optionals = {
            "debug_level" : 3,
            "show_timing" : False,
            "dirname" : f"{Path.cwd()}/cached_searches",
            "es_cluster" : "https://fifemon-es.fnal.gov",
            "search_size" : 5000,
            #end_date assignment is handled elsewhere
            "end_date"  : "0",
            "force_overwrite" : False
            }


        keys = self.args.keys()

        required_args = ["start_date"]

        missing = []

        for arg in required_args:
            if not arg in keys:
                missing.append(arg)

        if not len(missing) == 0:
            print(f"Error: Missing the following required arguments:\n{' '.join(arg for arg in missing)}")

        missing = []

        for arg in default_optionals:
            if not arg in keys:
                self.args[arg] = default_optionals[arg]

        self.client = Elasticsearch([es_cluster])


    def day_overseer(self, day, filetypes):
		'''divide each day into six hour chunks
        (gives us 4 threads per day for faster data pull for all time increments.
        Also convenient number since processing a week of data at once would lead to 28 total threads,
        which is manageable by most systems (with limited RAM being the most prominent concern at that point))'''

        y = day.strftime("%Y")
        m = day.strftime("%m")
        d = day.strftime("%d")
        index = f"rucio-transfers-v0-{y}.{m}"

        data_queue = threading.Queue()

        hour = 0
        #Make sure hour_step is always a factor of 24 or may break things
        hour_step = 6
        thread_list = []
        '''for each chunk
			add es_worker thread to list with args passed
            generate query for given thread
			make es_worker thread with index, queue, and query'''
        while hour < 24:
            es_template = {
                "query" : {
                    "bool" : {
                        "filter" : {
                            "range" : {
                                "@timestamp" : {
                                    "gte" : f"{y}-{m}-{d}'T'{str(hour):02}:00:00",
                                    "lte" : f"{y}-{m}-{d}'T'{str(hour + hour_step - 1):02}:59:59"
                                }
                            }
                        },
                        "should" : [
                            {
                                "match": {
                                    "event_type" : "transfer-failed"
                                },
                            },
                            {
                                "match": {
                                    "event_type" : "transfer-submission_failed"
                                }
                            },
                            {
                                "match": {
                                    "event_type" : "transfer-done"
                                }
                            }
                        ],
                        "minimum_should_match" : 1
                    }
                }
            }

            new_thread = threading.Thread(target=self.es_worker, args=(index, data_queue, es_template,) daemon=True)
            thread_list.append(new_thread)

            hour += hour_step

        '''start all threads and data_processor with index, day, filetypes, queue, and thread list'''

        for t in thread_list:
            t.start()

        data_processor_thread = threading.Thread(target=self.data_processor, args=(day, filetypes, data_queue, thread_list,) daemon=True)

        #data_processor_thread only terminates after all es_worker threads terminate
        data_processor_thread.join()

        self.day_lock.acquire()
        self.day_semaphore += 1
        self.day_lock.release()

    def es_worker(self, index, data_queue, query):
		'''Difference from original: Instead of writing directly, sends data to data processing queue'''
        for data in scroll(self.client, index, query):
            for entry in data:
                data_queue.put(entry["_source"])


    def data_processor(self, day, filetypes, data_queue, thread_list):
		'''set up summary matrices
		set up file objects
			-Check which file objects needed here. Have checks later for writing
			(basically, is the file name blank) since processing overhead time will
			be miniscule compared to overhead time for ES response'''

        y1 = day.strftime("%Y")
        m1 = day.strftime("%m")
        d1 = day.strftime("%d")

        next_day = day + relativedelta(days=+1)

        y2 = day.strftime("%Y")
        m2 = day.strftime("%m")
        d2 = day.strftime("%d")


        generators = {}
        files = {}

        for key in filetypes:
            #Initializes all generators
            if self.file_info[key]["process_type"] == "generator":
                generators[key] = self.file_info[key]["process_func"]()
                next(generators[key])
            dir_path = f"{self.args['dirname']}/{day.year}/{str(day.month):02}"
            fname = self.file_info[key]["file_format"].format(y, m, d, y_next, m_next, d_next)
            files[key] = open(f"{dir_path}/{fname}", "w+")
            files[key].write('[\n')

        '''
        List of filenames:
        dune_transfers_display_YYYY....
        dune_transfers_aggregates_display_YYYY...
        dune_network_checkup_display_YYYY...
        dune_failed_transfers_display_YYYY...
        dune_failed_transfers_aggregates_display_YYYY...
        dune_failed_network_checkup_display_YYYY...
        '''

        #Spins until all elasticsearch threads have been shut down or
		while any([t.is_alive() for t in thread_list]) or not data_queue.is_empty():
			if data_queue.is_empty():
				time.sleep(0.1)
				continue

            next_data = data_queue.get()
            data_queue.task_done()

            for key in filetypes:
                con_count = 0
                res_count = 0
                for con in self.file_info[key]["conditions"].keys():
                    if next_data[con].find(self.file_info[key]["conditions"][con]) != -1:
                        con_count += 1
                for re in self.file_info[key]["restrictions"].keys():
                    if next_data[res].find(self.file_info[key]["restrictions"][res]) != -1:
                        res_count += 1
                if res_count <= self.file_info[key]["max_restriction_count"] and con_count >= self.file_info[key]["min_condition_count"]:
                    if self.file_info[key]["process_type"] == "generator":
                        msg = {
                            "data" : next_data,
                            "operation" : "STORE"
                        }
                        generators[key].send(msg)
                    elif self.file_info[key]["process_type"] == "function":
                        res = self.file_info[key]["process_func"](next_data)
                        files[key].write(f"{res},\n")


			'''process data
				-Write directly where appropriate
				-Modify matrices where appropriate
				-Handle differentiation between successes and failures here by event type
				-Handle differentiation between normal and network test here
					-By handling both differentiation types here, we can use a single "should' query
					and process events individually on our end rather than making multiple queries or clients'''

        for key in filetypes:
            if self.file_info["process_type"] == "generator":
                con_count = 0
                res_count = 0
                for con in self.file_info[key]["conditions"].keys():
                    if next_data[con].find(self.file_info[key]["conditions"][con]) != -1:
                        con_count += 1
                for re in self.file_info[key]["restrictions"].keys():
                    if next_data[res].find(self.file_info[key]["restrictions"][res]) != -1:
                        res_count += 1
                if res_count <= self.file_info[key]["max_restriction_count"] and con_count >= self.file_info[key]["min_condition_count"]:
                    data = generators[key].send("GET")
                    while data != "FINISHED":
                        files[key].write(f"{data},\n")

        for key in filetypes:
            #Handles trailing commas on last line
            files[key].seek(files[key].tell()-2)
            files[key].write("\n]")
            files[key].close()


    def main(self, args):
        #Start time for timing info
        start_time = datetime.now().timestamp()

        #Set up arguments and check if defaults needed
        self.args = dict(args)
        self.check_args()

        self.debug_level = int(self.args["debug_level"])

        if self.args["end_date"] == "0":
            self.args["end_date"] = date_parser.parse(self.args["start_date"])
        if self.debug > 3:
            print(f"Args recieved: {self.args}")

        #Code to go day by day for output files as expected by other programs associated
        #with this project.
        curr_date = date_parser.parse(self.args["start_date"])
        target_date = date_parser.parse(self.args["end_date"])

        overseer_threads = []

        while (target_date - curr_date).days > 0:
            y = curr_date.strftime("%Y")
            m = curr_date.strftime("%m")
            d = curr_date.strftime("%d")

            index = f"rucio-transfers-v0-{y}.{m}"

            if not self.client.indices.exists(index=index):
                curr_date += relativedelta(days=+1)
                if self.args["debug"] >= 2:
                    print(f"Error: index {index} does not exist in cluster but is needed for date {curr_date}. Skipping date.")
                continue

            self.day_lock.acquire()
            curr_semaphore = self.day_semaphore
            self.day_lock.release()
            if curr_semaphore <= 0:
                time.sleep(0.1)
                continue
            self.day_lock.acquire()
            self.day_semaphore -= 1
            self.day_lock.release()


            self.args["end_date"] = (curr_date + relativedelta(days=+1)).strftime("%Y-%m-%d")

            #Makes sure that every step of the path from the provided directory onwards exists
            #and then sets self.dirname
            if not Path(self.args['dirname']).is_dir():
                Path(self.args['dirname']).mkdir(exist_ok=True)
            if not Path(f"{self.args['dirname']}/{curr_date.year}").is_dir():
                Path(f"{self.args['dirname']}/{curr_date.year}").mkdir(exist_ok=True)
            if not Path(f"{self.args['dirname']}/{curr_date.year}/{str(curr_date.month):02}").is_dir():
                Path(f"{self.args['dirname']}/{curr_date.year}/{str(curr_date.month):02}").mkdir(exist_ok=True)

            required_files = []

            y_next, m_next, d_next = self.args["end_date"].split("-")

            #Checks which files need to be written
            for key in self.file_info.keys():
                dir_path = f"{self.args['dirname']}/{curr_date.year}/{str(curr_date.month):02}"
                fname = self.file_info[key]["file_format"].format(y, m, d, y_next, m_next, d_next)
                if not Path(f"{dir_path}/{fname}") or self.args["force_overwrite"]:
                    required_files.append(key)

            overseer = threading.Thread(target=self.day_overseer, args=(curr_date, required_files,), daemon=True)
            overseer_threads.append(overseer)
            overseer_threads[-1].start()

        #Joins all overseer threads
        for t in overseer_threads:
            t.join()
