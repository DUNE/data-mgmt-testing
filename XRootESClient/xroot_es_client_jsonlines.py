#!/usr/bin/env python3
#Developed by Zachary Lee
#with work by Dr. Heidi Schellman as reference

#JSON needed to process incoming Elasticsearch results
#and for output formatting for easier use by webserver.
DEBUGSITE=0
import json
import jsonlines
import csv
import os
import sys
import time
import threading
import queue

from pathlib import Path

#ElasticSearch API is needed for interaction with Fermilab's ElasticSearch system
from elasticsearch import Elasticsearch

#Used to check that we're not looking for dates in the future
from datetime import datetime
from dateutil import parser as date_parser
from dateutil.relativedelta import relativedelta

import urllib
from urllib.request import urlopen, HTTPError, URLError

import argparse as ap

import samweb_client


#Structure of class:
# Main new_run (main thread) handles the initial project summary function,
# overseer functions/threads, writer thread, and summarizer function
# There are 4 immediate subthreads in new_run: Three "overseer" threads and one
# thread for writing results to separate raw jsonl files.
# Each overseer thread can manage up to some set number of "worker" threads simultaneously
# (as set by the respective max_threads variables). Each worker thread corresponds to
# an individual project ID. Each category of thread (Elasticsearch (ES), SAM, Compiler,
# and Writer) feeds into some other part of the process.
#The ES threads compile a list of files and their metadata for each project.
# The SAM threads take each file ID and retrieve additional
# metadata for them. The Compiler threads take ES data and project metadata and
# compile it into JSON objects for the Writer thread. The Writer thread then
# writes the compiled data into project ID separated files until all previous
# threads (ES, SAM, and Compiler) have terminated. The new_run thread waits for
# the Writer thread to terminate, then runs the Summarizer function. The
# summarizer function is designed to step through each raw jsonl file in
# PID-order (ascending), calculate the processing rate for each file id,
# then sending the summarized information to a new jsonl file. All raw file results
# are processed to the same summary file, ordered by project ID.
class XRootESClient():
    def __init__(self):
        #Debug levels:
        #   0: No debug message
        #   1: Show errors
        #   2: Show warnings
        #   3: Show thread completion messages
        #   4: Show additional process information
        self.debug = 3

        #If true, will display timing information for different aspects
        #of the program
        self.display_timing = False

        #Stores timing data
        self.times = {
            "run_time" : 0,
            "io_time" : 0,
            "elasticsearch_time" : 0,
            "sam_files_time" : 0,
            "sam_proj_time" : 0,
            "summarize_time" : 0,
            "compile_time" : 0,
            "metadata_fetches" : 0
        }

        #Variable for individual run arguments
        self.args = {}

        #Stores the output directory name
        self.dirname = ""

        #Number of attempts to acquire FID metadata before giving up
        self.max_retries = 30

        #Controls the number of PIDS processed simultaneously at each level
        self.max_es_threads = 8
        self.max_sam_threads = 8
        self.max_compiler_threads = 8

        self.total_pids = 0
        self.total_fids = 0

        #Class-scope data structures for File ID metadata, Project ID data,
        #and processed data to be written
        self.fids = {}
        self.pids = {}
        self.pid_list = None
        self.finished_data = queue.Queue()

        self.finished_es_threads = 0
        self.finished_sam_threads = 0
        self.finished_compiler_threads = 0

        #Thread locks for File ID data access and incrementing/decrementing
        #certain values
        self.fid_lock = threading.Lock()
        self.time_lock = threading.Lock()
        self.inc_lock = threading.Lock()



        #Class-scope variables for the different overseer threads
        self.compiler_overseer = None
        self.sam_overseer = None
        self.es_overseer = None
        self.writer_thread = None

    def check_args(self):
        #Defaults only applies to optional arguments
        default_optionals = {
            "debug_level" : 3,
            "show_timing" : False,
            "simultaneous_pids" : 8,
            "dirname" : f"{Path.cwd()}/cached_searches",
            "experiment" : "dune",
            "es_cluster" : "https://fifemon-es.fnal.gov",
            "search_size" : 1000,
            "user" : "",
            "output_for_display" : False,
            "display_aggregate" : False,
            "sitename_file" : f"{Path.cwd()}/SiteNames.json",
            "clear_raws" : False,
            "make_csvs" : False,
            "overwrite_files" : True,
            #end_date assignment is handled elsewhere
            "end_date"  : "0"
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


    #Puts the relevant country extension at the front of a location
    def countrify(self, loc):
        country = loc.split(".")[-1]
        if country == "edu" or country == "gov":
            country = "us"
        return country+"_"+loc

    #Takes a node's name and finds the associated site
    def translate_site(self, name, nodename_sitename):
        if name in nodename_sitename:
            return nodename_sitename[name]
        else:
            return "UNKNOW_"+name

    def summarize_record(self, item, nodename_sitename):
        sumrec={}
        if "site" not in item or "file_location" not in item:
            #print("missing: ",item["node"])
            return None
        site = self.countrify(item["site"])
        if "rate" not in item:
            return None
        finalstate = item["last_file_state"]
        application = "unknown"
        if "application" in item:
            application = item["application"]
        disk = item["file_location"]
        user = item["username"]
        date = item["@timestamp"][0:10]
        duration = round(item["duration"], 4)
        sumrec["source"] = self.translate_site(disk,nodename_sitename)
        sumrec["user"] = user
        sumrec["date"] = date
        process_id = 0
        if "process_id" in item:
            sumrec["process_id"] = item["process_id"]
        sumrec["start_time"] = item["@timestamp"]
        sumrec["file_transfer_time"] = duration
        sumrec["file_size"] = item["file_size"]
        sumrec["username"] = user
        sumrec["application"] = application
        sumrec["version"] = item["version"]
        sumrec["final_state"] = finalstate
        sumrec["destination"] = self.translate_site(site,nodename_sitename)
        sumrec['transfer_speed(MB/s)'] = round(item["rate"], 4)
        sumrec['transfer_speed(B/s)'] = round(item["rate"]*1024*1024, 4)
        sumrec["project_name"] = item["project_name"]
        #sumrec["file_name"] = os.path.basename(item["file_url"])
        sumrec["name"] = os.path.basename(item["file_url"])
        sumrec["data_tier"] = item["data_tier"]
        sumrec["node"] = item["node"]
        return sumrec

    #Sets the number of simultaneous PIDs to process
    #Default is 8 to avoid excessive numbers of threads
    def set_pid_count(self, count):
        self.max_es_threads = count
        self.max_sam_threads = count
        self.max_compiler_threads = count

    #Resets values to initial state
    def reset_vals(self):
        #Debug levels:
        #   0: No debug message
        #   1: Show errors
        #   2: Show warnings
        #   3: Show thread completion messages
        #   4: Show additional process information
        self.debug = 3

        #If true, will display timing information for different aspects
        #of the program
        self.display_timing = False

        #Stores timing data
        self.times = {
            "run_time" : 0,
            "io_time" : 0,
            "elasticsearch_time" : 0,
            "sam_files_time" : 0,
            "sam_proj_time" : 0,
            "summarize_time" : 0,
            "compile_time" : 0,
            "metadata_fetches" : 0
        }

        #Variable for individual run arguments
        self.args = {}

        #Stores the output directory name
        self.dirname = ""

        #Number of attempts to acquire FID metadata before giving up
        self.max_retries = 30

        #Controls the number of PIDS processed simultaneously at each level
        self.max_es_threads = 8
        self.max_sam_threads = 8
        self.max_compiler_threads = 8

        self.total_pids = 0
        self.total_fids = 0

        #Class-scope data structures for File ID metadata, Project ID data,
        #and processed data to be written
        self.fids = {}
        self.pids = {}
        self.pid_list = None
        self.finished_data = queue.Queue()

        self.finished_es_threads = 0
        self.finished_sam_threads = 0
        self.finished_compiler_threads = 0

        #Thread locks for File ID data access and incrementing/decrementing
        #certain values
        self.fid_lock = threading.Lock()
        self.time_lock = threading.Lock()
        self.inc_lock = threading.Lock()



        #Class-scope variables for the different overseer threads
        self.compiler_overseer = None
        self.sam_overseer = None
        self.es_overseer = None
        self.writer_thread = None

    #Displays timing data collected by other functions
    def show_timing_info(self):
        print("======================")
        print("Single run timing info")
        print("======================")
        print(f"Overall run time: {round(self.times['run_time'], 3)} seconds")
        print(f"Project summaries fetch time: {round(self.times['sam_proj_time'], 3)} seconds")
        print(f"Average Elasticsearch thread time: {round(self.times['elasticsearch_time']/self.total_pids, 3)} seconds")
        print(f"Average active SAM FID metadata fetch time: {round(1000*self.times['sam_files_time']/self.total_fids, 4)} seconds per 1000 files")
        print(f"Total summarizer function time: {round(self.times['summarize_time'], 3)} seconds")

    #Sets the debug level
    def set_debug_level(self, level):
        self.debug = level

    #Sets whether the program will display timing data
    def set_show_timing_info(self, display_timing):
        self.display_timing = display_timing

    #Main thread for the class.
    def new_run(self, args):
        #Gets the overall run start time
        start_time = datetime.now().timestamp()

        #Resets finished thread counts
        self.finished_es_threads = 0
        self.finished_sam_threads = 0
        self.finished_compiler_threads = 0


        self.args = dict(args)

        self.check_args()

        #Sets debug level and whether to show timing info
        self.set_debug_level(int(self.args["debug_level"]))
        if self.args["show_timing"]:
            self.set_show_timing_info(True)
        self.set_pid_count(int(self.args["simultaneous_pids"]))
        #Checks to see if the default end date is set
        if self.args["end_date"] == "0":
            self.args["end_date"] = date_parser.parse(self.args["start_date"]) + relativedelta(days=+1)
        if self.debug > 3:
            print(f"Args recieved: {self.args}")

        #Code to go day by day for output files as expected by other programs associated
        #with this project.
        curr_date = date_parser.parse(self.args["start_date"])
        target_date = date_parser.parse(self.args["end_date"])

        while (target_date - curr_date).days > 0:
            self.args["end_date"] = (curr_date + relativedelta(days=+1)).strftime("%Y-%m-%d")


            #Makes sure that every step of the path from the provided directory onwards exists
            #and then sets self.dirname
            if not Path(self.args['dirname']).is_dir():
                Path(self.args['dirname']).mkdir(exist_ok=True)
            if not Path(f"{self.args['dirname']}/{curr_date.year}").is_dir():
                Path(f"{self.args['dirname']}/{curr_date.year}").mkdir(exist_ok=True)
            if not Path(f"{self.args['dirname']}/{curr_date.year}/{curr_date.month}").is_dir():
                Path(f"{self.args['dirname']}/{curr_date.year}/{curr_date.month}").mkdir(exist_ok=True)

            self.dirname = f"{self.args['dirname']}/{curr_date.year}/{curr_date.month}"

            if self.debug > 2:
                print("\n==========================================================================")
                print(f"Started processing date range {self.args['start_date']} to {self.args['end_date']}")
                print("==========================================================================")

            #Initializes the SAM Web client
            self.samweb = samweb_client.SAMWebClient(experiment=self.args["experiment"])
            #Gets list of DUNE-related projects started in the specified date range
            self.get_proj_list()
            if self.debug > 3:
                print(f"Projects gotten: {' '.join(pid for pid in self.pids)}")
            if self.debug > 2:
                print(f"{len(self.pids.keys())} projects found")
            #Creates and starts all overseer threads as daemons to prevent continued
            #operation if the main program ends.
            self.es_overseer = threading.Thread(target=self.es_overseer_func, daemon=True)
            self.sam_overseer = threading.Thread(target=self.sam_overseer_func, daemon=True)
            self.compiler_overseer = threading.Thread(target=self.compiler_overseer_func, daemon=True)
            self.writer_thread = threading.Thread(target=self.writer, daemon=True)
            self.es_overseer.start()
            time.sleep(0.5)
            self.sam_overseer.start()
            time.sleep(0.5)
            self.compiler_overseer.start()
            time.sleep(0.5)
            self.writer_thread.start()
            time.sleep(0.5)
            #Writer thread doesn't exit until all other non-main threads end,
            #so we only need to wait for it to join.
            self.writer_thread.join()
            if self.debug > 2:
                print("Writing to raw files complete. Now summarizing.")
            #Runs the summaraization file generating function
            self.summarizer()

            #Clears the pid list
            self.fids = {}
            self.pids = {}

            #Resets finished thread counts
            self.finished_es_threads = 0
            self.finished_sam_threads = 0
            self.finished_compiler_threads = 0

            self.args["start_date"] = self.args["end_date"]
            curr_date += relativedelta(days=+1)

        #Gets the overall run end time and duration
        end_time = datetime.now().timestamp()
        self.times["run_time"] = end_time - start_time
        if self.display_timing:
            self.show_timing_info()

    #Creates a summary file with data about each FID found sorted by PID then FID
    def summarizer(self):
        #Gets summarizer start time
        start_time = datetime.now().timestamp()
        #We're making a single summary file for all projects IDs
        sum_writer = jsonlines.open(f"{self.dirname}/{self.args['experiment']}_summary_{self.args['start_date']}_{self.args['end_date']}.jsonl", mode="w")
        if self.args["output_for_display"]:
            display_writer = open(f"{self.dirname}/out_{self.args['experiment']}_{self.args['start_date']}_to_{self.args['end_date']}.json", "w")
            display_writer.write('{ "data" : [\n')
            site_file = open(self.args["sitename_file"],'r')
            nodename_sitename =json.load(site_file)
            site_file.close()
            is_start = True
            if self.args["display_aggregate"]:
                aggregates = {}

        if self.args["make_csvs"]:
            csv_filename = f"{self.dirname}/{self.args['experiment']}_summary_{self.args['start_date']}_{self.args['end_date']}.csv"
            csv_file = open(csv_filename, 'w')
            csv_writer = None
        #Steps through all found project IDs
        for pid in self.pid_list:
            #Checks to make sure that the raw file we're accessing isn't empty
            #(indicating no relevant SAM events for that project)
            raw_fname = f"{self.dirname}/{self.args['experiment']}_raw_{self.args['start_date']}_{self.args['end_date']}_{pid}.jsonl"
            if os.path.exists(raw_fname) and  Path(raw_fname).stat().st_size == 0:
                if self.args["clear_raws"]:
                    os.remove(self.pids[pid]["raw_filename"])
                continue

            #Each PID has an associated raw file, with a saved name for convenience
            with jsonlines.open(raw_fname) as reader:
                #Sets some default values for checks or inter-iteration variables
                curr_fid = None
                last_start = None
                prev_event = None
                start_num = None
                last_count = 0
                #Every line is a full event, so we process each line individually
                for count, event in enumerate(reader):
                    try:
                        #Checks to see if we have the default FID (indicates that
                        #this is the first iteration of the loop)
                        if curr_fid == None:
                            curr_fid = event["file_id"]
                            last_start = event
                            start_num = count
                        #Checks if we've moved on to a new file id. This indicates
                        #that we need to output a summary of the last FID as we've
                        #processed its last event (since the files are grouped by
                        #FID and then sorted by timestamp within those groups)
                        elif event["file_id"] != curr_fid:
                            #last_start is a copy of the first event with the current
                            #FID, which we need for our summary.
                            summary = last_start
                            #Pulls the file size for the current FID from previously
                            #fetched SAM metadata and adds it to our summary
                            summary["file_size"] = self.fids[curr_fid]["file_size"]
                            #Checks if there's a data tier associated with this FID,
                            #and adds it to the summary
                            if "data_tier" in self.fids[curr_fid]:
                                summary["data_tier"] = self.fids[curr_fid]["data_tier"]
                            else:
                                summary["data_tier"] = ""
                            #Checks if there's a campaign associated with this FID,
                            #and adds it to the summary
                            if "DUNE.campaign" in self.fids[curr_fid]:
                                summary["campaign"] = self.fids[curr_fid]["DUNE.campaign"]
                            else:
                                summary["campaign"] = ""
                            #Compares the current line/event count and subtracts the line count
                            #of the first event associated with this FID to get a total
                            #action/event count for this file
                            summary["actions"] = count - start_num
                            #Adds the file state from the last known event
                            summary["last_file_state"] = prev_event["file_state"]
                            #Adds the timestamp from the last known event
                            summary["last_timestamp"] = prev_event["timestamp"]
                            #Calculates the difference between the first and last events for this FID
                            summary["duration"] = prev_event["timestamp"] - last_start["timestamp"]
                            #Checks that we actually have a file size and duration, then adds the total
                            #processing rate to the summary
                            if "file_size" in summary and "duration" in summary and summary["file_size"] != None and summary["duration"] != 0:
                                summary["rate"]=summary["file_size"]/summary["duration"] / 1024 / 1024
                            else:
                                summary["rate"] = 0
                            #0 actions makes no sense as we should have at least 1 event if we've
                            #encounted a FID.
                            if summary["actions"] == 0:
                                summary["actions"] = 1
                            #Writes the summary for this FID
                            sum_writer.write(summary)
                            #Updates the variables that track our current FID,
                            #first known event for this FID, and the event number
                            #of that first event.
                            curr_fid = event["file_id"]
                            last_start = event
                            start_num = count

                            if self.args["output_for_display"]:
                                to_write = self.summarize_record(summary, nodename_sitename)
                                if not self.args["display_aggregate"]:
                                    if not to_write == None:
                                        if not is_start:
                                            display_writer.write(",\n")
                                        else:
                                            is_start = False
                                        display_writer.write(json.dumps(to_write, indent=2))
                                else:
                                    if not to_write == None:
                                        if not pid in aggregates.keys():
                                            aggregates[pid] = {}
                                        if not to_write["source"] in aggregates[pid].keys():
                                            aggregates[pid][to_write["source"]] = {}
                                        if not to_write["destination"] in aggregates[pid][to_write["source"]].keys():
                                            aggregates[pid][to_write["source"]][to_write["destination"]] = dict(to_write)
                                            aggregates[pid][to_write["source"]][to_write["destination"]]["name"] = f"project_{pid}_{to_write['source']}_to_{to_write['destination']}"
                                            aggregates[pid][to_write["source"]][to_write["destination"]]["data_tier"] = []
                                            aggregates[pid][to_write["source"]][to_write["destination"]]["file_count"] = 0
                                        else:
                                            aggregates[pid][to_write["source"]][to_write["destination"]]["file_size"] += to_write["file_size"]
                                            aggregates[pid][to_write["source"]][to_write["destination"]]["file_transfer_time"] += to_write["file_transfer_time"]
                                        if not to_write["data_tier"] in aggregates[pid][to_write["source"]][to_write["destination"]]["data_tier"]:
                                            aggregates[pid][to_write["source"]][to_write["destination"]]["data_tier"].append(to_write["data_tier"])
                                        aggregates[pid][to_write["source"]][to_write["destination"]]["file_count"] += 1

                            if self.args["make_csvs"]:
                                if csv_writer is None:
                                    fieldnames = summary.keys()
                                    csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                                    csv_writer.writeheader()
                                csv_writer.writerow(summary)

                        #Updates the value of the last checked event and event
                        #number to the current event and event number
                        prev_event = event
                        last_count = count




                    except:
                        print(f"Error with FID {curr_fid}: {sys.exc_info()}")




                #Makes sure the last FID in a file is accounted for
                try:
                    #last_start is a copy of the first event with the current
                    #FID, which we need for our summary.
                    summary = last_start
                    #Pulls the file size for the current FID from previously
                    #fetched SAM metadata and adds it to our summary
                    summary["file_size"] = self.fids[curr_fid]["file_size"]
                    #Checks if there's a data tier associated with this FID,
                    #and adds it to the summary
                    if "data_tier" in self.fids[curr_fid]:
                        summary["data_tier"] = self.fids[curr_fid]["data_tier"]
                    else:
                        summary["data_tier"] = ""
                    #Checks if there's a campaign associated with this FID,
                    #and adds it to the summary
                    if "DUNE.campaign" in self.fids[curr_fid]:
                        summary["campaign"] = self.fids[curr_fid]["DUNE.campaign"]
                    else:
                        summary["campaign"] = ""
                    #Compares the current line/event count and subtracts the line count
                    #of the first event associated with this FID to get a total
                    #action/event count for this file
                    summary["actions"] = last_count - start_num + 1
                    #Adds the file state from the last known event
                    summary["last_file_state"] = prev_event["file_state"]
                    #Adds the timestamp from the last known event
                    summary["last_timestamp"] = prev_event["timestamp"]
                    #Calculates the difference between the first and last events for this FID
                    summary["duration"] = prev_event["timestamp"] - last_start["timestamp"]
                    #Checks that we actually have a file size and duration, then adds the total
                    #processing rate to the summary
                    if "file_size" in summary and "duration" in summary and summary["file_size"] != None and summary["duration"] != 0:
                        summary["rate"]=summary["file_size"]/summary["duration"]*0.000001
                    #0 actions makes no sense as we should have at least 1 event if we've
                    #encounted a FID.
                    if summary["actions"] == 0:
                        summary["actions"] = 1
                    #Writes the last FID summary for this PID
                    sum_writer.write(summary)

                    if self.args["output_for_display"]:
                        to_write = self.summarize_record(summary, nodename_sitename)
                        if not self.args["display_aggregate"]:
                            if not to_write == None:
                                if not is_start:
                                    display_writer.write(",\n")
                                else:
                                    is_start = False
                                display_writer.write(json.dumps(to_write, indent=2))
                        else:
                            if not to_write == None:
                                if not pid in aggregates.keys():
                                    aggregates[pid] = {}
                                if not to_write["source"] in aggregates[pid].keys():
                                    aggregates[pid][to_write["source"]] = {}
                                if not to_write["destination"] in aggregates[pid][to_write["source"]].keys():
                                    aggregates[pid][to_write["source"]][to_write["destination"]] = dict(to_write)
                                    aggregates[pid][to_write["source"]][to_write["destination"]]["name"] = f"project_{pid}_{to_write['source']}_to_{to_write['destination']}"
                                    aggregates[pid][to_write["source"]][to_write["destination"]]["data_tier"] = []
                                    aggregates[pid][to_write["source"]][to_write["destination"]]["file_count"] = 0
                                else:
                                    aggregates[pid][to_write["source"]][to_write["destination"]]["file_size"] += to_write["file_size"]
                                    aggregates[pid][to_write["source"]][to_write["destination"]]["file_transfer_time"] += to_write["file_transfer_time"]
                                if not to_write["data_tier"] in aggregates[pid][to_write["source"]][to_write["destination"]]["data_tier"]:
                                    aggregates[pid][to_write["source"]][to_write["destination"]]["data_tier"].append(to_write["data_tier"])
                                aggregates[pid][to_write["source"]][to_write["destination"]]["file_count"] += 1
                except:
                    print(f"Error with FID {curr_fid}: {sys.exc_info()}")

            if self.args["clear_raws"]:
                os.remove(self.pids[pid]["raw_filename"])

        #Closes the summary file
        if self.args["output_for_display"]:
            if self.args["display_aggregate"]:
                for pid in aggregates.keys():
                    for source in aggregates[pid].keys():
                        for dest in aggregates[pid][source].keys():
                            info_out = aggregates[pid][source][dest]
                            info_out["transfer_speed(B/s)"] = round(info_out["file_size"] / info_out["file_transfer_time"], 4)
                            info_out["transfer_speed(MB/s)"] = round(info_out["transfer_speed(B/s)"] / 1024 / 1024, 4)
                            if not is_start:
                                display_writer.write(",\n")
                            else:
                                is_start = False
                            display_writer.write(json.dumps(info_out, indent=2))
            display_writer.write("\n]}")
            display_writer.close()

        if self.args["make_csvs"]:
            csv_file.close()

        sum_writer.close()

        #Gets summarizer end time and duration
        end_time = datetime.now().timestamp()
        self.times["summarize_time"] = end_time - start_time

    #Takes each item sent to the finished data queue, and writes it to a
    #raw data json file based on the project ID for the file.
    def writer(self):
        proj_files = {}

        #Checks that the cached_searches directory exists, and creates it if not.
        if not Path(self.dirname).is_dir():
            Path(self.dirname).mkdir(exist_ok=True)

        #Makes all of the raw file objects
        for pid in self.pid_list:
            fname = f"{self.dirname}/{self.args['experiment']}_raw_{self.args['start_date']}_{self.args['end_date']}_{pid}.jsonl"
            if self.args["overwrite_files"] or not os.path.exists(fname):
                self.pids[pid]["raw_filename"] = fname
                proj_files[pid] = open(fname, "w+")


        #Checks that either the compiler is still running, or there's still data in
        #the finished data queue to prevent early shutdowns
        while self.compiler_overseer.is_alive() or not self.finished_data.empty():
            if self.finished_data.empty():
                time.sleep(0.5)
                continue
            #Gets the next json object to write
            to_write = self.finished_data.get()
            if self.debug > 3:
                fid = to_write["file_id"]
                pid = to_write["project_id"]
                print(f"Writer recieved file {fid} for project {pid}")
            #Writes the json object to the appropriate file alongside a newline
            proj_files[to_write["project_id"]].write(json.dumps(to_write))
            proj_files[to_write["project_id"]].write("\n")
            #Notifies the queue that the object has been processed
            self.finished_data.task_done()

        #Closes all file objects
        for pid in self.pid_list:
            if pid in proj_files:
                proj_files[pid].close()

    #site_finder code here taken from XRootParser code by Dr. Heidi Schellman
    def site_finder(self, source):
        knownsites = [".gov",".fr",".es",".br",".edu",".in",".cz",".uk",".fr",".ch",".nl",".ru",".pr"]
        specials = {"dice":"dice.bristol.uk","CRUSH":"crush.syracuse.edu","gridpp.rl.ac.uk":"gridpp.rl.ac.uk","comp20-":"lancaster.uk","discovery":"nmsu.edu","nid":"nersc.lbnl.gov","wn-2":"unibe-lhcp.bern.ch","qmul":"esc.qmul.uk","uct2":"uct2-mwt2.uchicago.edu","nubes.stfc.ac.uk":"nubes.stfc.ac.uk","unl.edu":"unl.edu","wn0":"sheffield.uk","wn1":"sheffield.uk","wn2":"sheffield.uk","local-":"uprm:pr","arbutus":"arbutus.ca","axion":"axion.ca","local":"uprm.pr"}
        if "node" in source:
            node = source["node"]


            country = "."+node.split(".")[-1]
            #print ("country guess",country)
            if country in knownsites:
                # strip off the node name to get site name
                source["site"]= node[node.find(".")+1:]
                # fix because of structure of these node names
                if("gridpp.rl.ac.uk" in node):
                  source["site"]= "gridpp.rl.ac.uk"
                if DEBUGSITE:
                  print("site check",node,source["site"])
                return source["site"]
            for s in specials:
                if s in node:
                    source["site"] = specials[s]
                    if DEBUGSITE:
                      print("site check",node,source["site"])
                    return source["site"]
            if not "." in node:
                source["site"] = ""
                if DEBUGSITE:
                  print("site check",node,source["site"])
                return source["site"]
            source["site"] = "mystery-site"
            if DEBUGSITE:
               print("site check",node,source["site"])

        else:
            source["site"] = "unknown"
            if DEBUGSITE:
              print("site check",node,source["site"])
        return source["site"]

    #Overseer function that manages data compilation worker threads
    #Uses a makeshift semaphore to manage the number of simultaneous threads
    def compiler_overseer_func(self):
        i = 0
        #Steps through all PIDs
        while i < len(self.pids.keys()):
            #Checks to see if we have more threads available
            if self.max_compiler_threads <= 0:
                if self.debug > 3:
                    print("No available compiler threads. Waiting.")
                time.sleep(0.2)
            else:
                if self.debug > 3:
                    print(f"Making compiler thread for pid {self.pid_list[i]}")
                pid = self.pid_list[i]
                #Decrements the semaphore upon spawning a new thread
                raw_fname = f"{self.dirname}/{self.args['experiment']}_raw_{self.args['start_date']}_{self.args['end_date']}_{pid}.jsonl"
                if self.args["overwrite_files"] or not os.path.exists(raw_fname):
                    self.inc_lock.acquire()
                    self.max_compiler_threads -= 1
                    self.inc_lock.release()
                    self.pids[pid]["compiler_worker"] = threading.Thread(target=self.compiler_worker_func,args=(pid,),daemon=True)
                    self.pids[pid]["compiler_worker"].start()
                else:
                    self.inc_lock.acquire()
                    self.finished_compiler_threads += 1
                    self.inc_lock.release()
                i += 1
        #Waits for all threads to terminate
        for pid in self.pid_list:
            if not self.pids[pid]["compiler_worker"] is None:
                self.pids[pid]["compiler_worker"].join()

    #Compiles all data into finished form for a given project
    def compiler_worker_func(self, pid):
        #Ensures that the thread continues so long as there's data to be processed
        #or the threads that provide it with data are still alive (or have yet to be
        #initialized due to thread count limitations)
        while self.pids[pid]["sam_worker"] == None or self.pids[pid]["sam_worker"].is_alive() or not self.pids[pid]["write_queue"].empty():
            if self.pids[pid]["write_queue"].empty():
                time.sleep(1)
                continue
            #Pulls the next file to prepare for writing and notifies the queue
            #that the file has been taken
            next_file = self.pids[pid]["write_queue"].get()
            self.pids[pid]["write_queue"].task_done()

            #Tries to acquire file information for the specific file ID, and
            #retires a configurable number of times
            self.fid_lock.acquire()
            retry_count = 0
            fid = next_file["file_id"]
            while not fid in self.fids and retry_count < self.max_retries:
                retry_count += 1
                self.fid_lock.release()
                time.sleep(1)
                self.fid_lock.acquire()
            #Checks to see if we've hit the retry limit
            if retry_count >= self.max_retries:
                if self.debug > 1:
                    print(f"Warning: Timed out waiting for FID {fid} metadata. Skipping.")

            #Assigngs file metadata based on stored file information
            else:
                fid_md = self.fids[fid]
            self.fid_lock.release()
            try:
                #Checks for special file urls before processing
                if "eos" in next_file["file_url"]:
                    file_location = "eospublic.cern.ch"
                else:
                    file_location = next_file["file_url"].replace("https://","").replace("root://","").split("/")[0].split(":")[0]

                #Compiles data
                new_entry = {
                    "process_state" : next_file["process_state"],
                    "@timestamp" : next_file["@timestamp"],
                    "job_id" : next_file["job_id"],
                    "node" : next_file["node"],
                    "project_name" : next_file["project_name"],
                    "project_id" : pid,
                    "snapshot_id" : next_file["snapshot_id"],
                    "file_id" : next_file["file_id"],
                    "process_id" : next_file["process_id"],
                    "file_url" : next_file["file_url"],
                    "file_state" : next_file["file_state"],
                    "username" : next_file["username"],
                    "event" : next_file["event"],
                    "timestamp" : date_parser.isoparse(next_file["@timestamp"]).timestamp(),
                    "file_location" : file_location,
                    "site" : self.site_finder(next_file),
                    "project_status" : self.pids[pid]["metadata"]["project_status"],
                    "project_start_time" : self.pids[pid]["metadata"]["project_start_time"],
                    "project_end_time" : self.pids[pid]["metadata"]["project_end_time"],
                    "station_name" : self.pids[pid]["metadata"]["station_name"],
                    "dataset_def_id" : self.pids[pid]["metadata"]["dataset_def_id"],
                    "dataset_def_name" : self.pids[pid]["metadata"]["dataset_def_name"],
                    "process_counts" : self.pids[pid]["metadata"]["process_counts"],
                    "file_counts" : self.pids[pid]["metadata"]["file_counts"],
                    "files_in_snapshot" : self.pids[pid]["metadata"]["files_in_snapshot"],
                    "application" : self.pids[pid]["metadata"]["processes"][0]["application"]["name"],
                    "version" : self.pids[pid]["metadata"]["processes"][0]["application"]["version"]
                }

                #Puts the finished data into a queue for the writer thread
                self.finished_data.put(new_entry)

            except Exception as e:
                if self.debug > 1:
                    md = self.pids[pid]["metadata"]
                    print(f"Warning: Could not process entry FID {fid} for entry {next_file} and project metadata {md}")
                    print(f"Exception {e} was thrown")

        #Increments the compiler thread sempahore to allow new threads to spawn
        self.inc_lock.acquire()
        self.max_compiler_threads += 1
        self.finished_compiler_threads += 1
        if self.debug > 3:
            print(f"Finished compiler thread with pid {pid}")
        if self.debug > 2:
            print(f"{self.finished_compiler_threads}/{len(self.pids.keys())} compiler threads finished")
        self.inc_lock.release()


    #Overseer function for SAM-releated threads
    def sam_overseer_func(self):
        i = 0
        #Steps through all PIDs
        while i < len(self.pids.keys()):
            #Checks to see if we have more threads available
            if self.max_sam_threads <= 0:
                if self.debug > 3:
                    print("No available SAM threads. Waiting.")
                time.sleep(0.2)
            else:
                if self.debug > 3:
                    print(f"Making SAM thread for pid {self.pid_list[i]}")
                pid = self.pid_list[i]
                raw_fname = f"{self.dirname}/{self.args['experiment']}_raw_{self.args['start_date']}_{self.args['end_date']}_{pid}.jsonl"
                if self.args["overwrite_files"] or not os.path.exists(raw_fname):
                    #Decrements the semaphore when spawning a new thread
                    self.inc_lock.acquire()
                    self.max_sam_threads -= 1
                    self.inc_lock.release()
                    self.pids[pid]["sam_worker"] = threading.Thread(target=self.sam_worker_func,args=(pid,),daemon=True)
                    self.pids[pid]["sam_worker"].start()
                    self.total_pids += 1
                else:
                    self.inc_lock.acquire()
                    self.finished_sam_threads += 1
                    self.inc_lock.release()
                i += 1

        #Waits for all threads to terminate
        for pid in self.pid_list:
            if not self.pids[pid]["sam_worker"] is None:
                self.pids[pid]["sam_worker"].join()

    def sam_worker_func(self, pid):
        #Runs as long as there's data, or the thread that produces data is either alive or hasn't been started
        while self.pids[pid]["es_worker"] == None or self.pids[pid]["es_worker"].is_alive() or not self.pids[pid]["fid_queue"].empty():
            #Waits if the FID queue is empty
            if self.pids[pid]["fid_queue"].empty():
                time.sleep(1)
                continue
            start_time = datetime.now().timestamp()
            #Gets the next set of file IDs
            to_search = self.pids[pid]["fid_queue"].get()
            #Gets the metadata for the set of FIDs
            data = self.samweb.getMultipleMetadata(to_search)
            self.fid_lock.acquire()
            self.total_fids += len(to_search)
            #Breaks out each individual FID's metadata and writes it to a class-level
            #dictionary hashed with file ID
            for item in data:
                try:
                    self.fids[item['file_id']] = item
                except:
                    print(f"Could not process item {item}")
            self.fid_lock.release()
            end_time = datetime.now().timestamp()
            self.time_lock.acquire()
            self.times["sam_files_time"] = self.times["sam_files_time"] + end_time - start_time
            self.time_lock.release()
        #Increments the sam_threads semaphore
        self.inc_lock.acquire()
        self.max_sam_threads += 1
        self.finished_sam_threads += 1
        if self.debug > 3:
            print(f"Finished SAM file metadata thread with pid {pid}")
        if self.debug > 2:
            print(f"{self.finished_sam_threads}/{len(self.pids.keys())} SAM file metadata threads finished")
        self.inc_lock.release()

    def es_overseer_func(self):
        i = 0
        #Steps through all PIDs
        while i < len(self.pids.keys()):
            #Checks to see if we have more threads available
            if self.max_es_threads <= 0:
                if self.debug > 3:
                    print("No available ElasticSearch threads. Waiting")
                time.sleep(0.2)
            else:
                pid = self.pid_list[i]
                if self.debug > 3:
                    print(f"Making ElasticSearch thread for pid {pid}")
                raw_fname = f"{self.dirname}/{self.args['experiment']}_raw_{self.args['start_date']}_{self.args['end_date']}_{pid}.jsonl"
                if self.args["overwrite_files"] or not os.path.exists(raw_fname):
                    #Decrements the sempahore when spawning a new thread
                    self.inc_lock.acquire()
                    self.max_es_threads -= 1
                    self.inc_lock.release()
                    self.pids[pid]["es_worker"] = threading.Thread(target=self.es_worker_func,args=(pid,),daemon=True)
                    self.pids[pid]["es_worker"].start()
                else:
                    self.inc_lock.acquire()
                    self.finished_es_threads += 1
                    self.inc_lock.release()
                i += 1
        #Waits for all threads to terminate
        for pid in self.pid_list:
            if not self.pids[pid]["es_worker"] is None:
                self.pids[pid]["es_worker"].join()

    #Function to pull all logged SAM Events for a given PID in the established date range
    def es_worker_func(self, pid):
        #Gets start time for this worker function
        start_time = datetime.now().timestamp()
        #Sets up variables for the function
        curr_date = date_parser.parse(self.args["start_date"])
        #Increases the searched date range by a month to account for end-of-month
        #spillover
        target_date = date_parser.parse(self.args["end_date"]) + relativedelta(months=+1)
        y0,m0,d0 = curr_date.strftime("%Y-%m-%d").split('-')
        #Creates an ElasticSearch search template
        es_template = {
            "query" : {
                "bool" : {
                    "filter" : {
                        #Only gets results after a certain date
                        "range" : {
                            "@timestamp" : {
                                "gte" : f"{y0}-{m0}-{d0}"
                            }
                        }
                    },
                    #Matches the PID
                    "must": {
                        "match": {
                             "project_id" : pid
                        }
                    },
                    #Matches one of three file states and one of two events
                    "should" : [
                        {
                            "match": {
                                "file_state" : "consumed"
                            }
                        },
                        {
                            "match": {
                                "file_state" : "skipped"
                            }
                        },
                        {
                            "match": {
                                "file_state" : "transferred"
                            }
                        },
                        {
                            "match": {
                                "event" : "update_file_state"
                            }
                        },
                        {
                            "match": {
                                "event" : "consumed_file"
                            }
                        }
                    ],
                    "minimum_should_match" : 2
                }
            }
        }

        #If we're searching for a specific user, add a requirement to search for that user
        if self.args["user"] != "":
            es_template["query"]["bool"]["should"].append({"match":{"username" : f"{self.args['user']}"}})
            es_template["query"]["bool"]["minimum_should_match"] += 1

        #Adds a sort to the template, sorting first by file ID and then by event
        #time within that sort. Ultimately gives us groups of the same file ID
        #that are sorted by time
        es_template["sort"] = [{"file_id": "asc"},{"event_time": "asc"}]

        #Based on Simplernerd's tutorial here: https://simplernerd.com/elasticsearch-scroll-python/
        #Scrolls through all of the results in a given date range
        #scroll is a generator that will step through all of the page results for a
        #given index. scroll maintains its internal state and returns a new page of results
        #each time it runs.
        def scroll(es, idx, body, scroll, search_size):
            page = es.search(index=idx, body=body, scroll=scroll, size=search_size)
            scroll_id = page['_scroll_id']
            hits = page['hits']['hits']
            while len(hits):
                yield hits
                page = es.scroll(scroll_id=scroll_id, scroll=scroll)
                scroll_id = page['_scroll_id']
                hits = page['hits']['hits']


        #Scrolls through the ES client and adds all information to the info array
        #compile_info now runs inside of get_speeds, so data in "info" will be in its
        #final state before export.
        data_exists = False
        while curr_date <= target_date:
            #Generates an index for the new month
            y = curr_date.strftime("%Y")
            m =  curr_date.strftime("%m")
            index = f"sam-events-v1-{y}.{m}"

            #Non-existent indices will crash the program if not properly handled
            if self.pids[pid]["es_client"].indices.exists(index=index):
                #Calling "scroll" gets the next set of results. The number
                #of results depends on the "size" parameter passed to the
                #initial search
                for data in scroll(self.pids[pid]["es_client"], index, es_template, "5m", self.args["search_size"]):
                    if len(data) > 0:
                        data_exists = True
                        #With individual results, we write each processed transfer
                        #individually to a file. We write them after each scroll
                        #to reduce memory usage
                        fids_to_dump = []
                        for res in data:
                            self.pids[pid]["write_queue"].put(res["_source"])
                            if not res["_source"]['file_id'] in self.fids and not res["_source"]['file_id'] in fids_to_dump:
                                fids_to_dump.append(res["_source"]['file_id'])
                        self.pids[pid]["fid_queue"].put(fids_to_dump)
            curr_date += relativedelta(months=+1)
        self.inc_lock.acquire()
        self.max_es_threads += 1
        self.finished_es_threads += 1
        if self.debug > 3:
            print(f"Finished ElasticSearch data thread with pid {pid}")
        if self.debug > 2:
            print(f"{self.finished_es_threads}/{len(self.pids.keys())} ElasticSearch threads finished")
        self.inc_lock.release()

        #Gets the end time and duration for this thread
        end_time = datetime.now().timestamp()
        self.time_lock.acquire()
        self.times["elasticsearch_time"] = self.times["elasticsearch_time"] + end_time - start_time
        self.time_lock.release()

    def get_proj_list(self):
        start_time = datetime.now().timestamp()
        projects = self.samweb.listProjects(started_after=self.args["start_date"], started_before=self.args["end_date"])
        for proj in projects:
            if "prestage" in proj:
                continue
            md = self.samweb.projectSummary(proj)
            pid = md["project_id"]
            self.pids[pid] = {}
            self.pids[pid]["metadata"] = md
            self.pids[pid]["es_client"] = Elasticsearch([self.args["es_cluster"]], timeout=120)
            self.pids[pid]["write_queue"] = queue.Queue()
            self.pids[pid]["fid_queue"] = queue.Queue()
            self.pids[pid]["raw_filename"] = None
            self.pids[pid]["es_worker"] = None
            self.pids[pid]["sam_worker"] = None
            self.pids[pid]["compiler_worker"] = None
        self.pid_list = list(self.pids.keys())
        self.pid_list.sort()
        if self.debug > 2:
            print(f"Full pid list is: {' '.join(str(p) for p in self.pid_list)}")
        end_time = datetime.now().timestamp()
        self.times["sam_proj_time"] += end_time - start_time


if __name__ == "__main__":

    today = datetime.today()
    #Timings used primarily for debug and code improvements,
    #but could still be useful in the long-term.
    #Any calls of time.perf_counter are exclusively for this
    #program_init = time.perf_counter()

    #Arguments for the script. The help info in the add_argument functions detail their respective uses
    parser = ap.ArgumentParser()
    parser.add_argument('-S', '--start', dest="start_date", default=today.strftime("%Y-%m-%d"), help="The earlest date to search for matching transfers. Defaults to today's date. Must be in form yyyy-mm-dd")
    parser.add_argument('-E', '--end', dest="end_date", default="0", help="The latest date to search for matching transfers. Defaults to the same value as the start date, giving a 1 day search. Must be in form yyyy-mm-dd")
    parser.add_argument('-Z', '--size', dest="search_size", default=1000, help="Number of results returned from Elasticsearch at once. Must be between 1 and 1,000 to accomodate limits of SAM metadata access")
    parser.add_argument('-U', '--user', dest="user", default="", help="Searches for a specific user")
    parser.add_argument('-X', '--experiment', dest="experiment", default="dune", help="Searches for a specific experiment")
    parser.add_argument('-C', '--cluster', dest='es_cluster', default="https://fifemon-es.fnal.gov", help="Specifies the Elasticsearch cluter to target")
    parser.add_argument('-D', '--directory', dest='dirname', default=f"{Path.cwd()}/cache", help="Sets the cached searches directory")
    parser.add_argument('--debug-level', dest='debug_level', default=3, help="Determines which level of debug information to show. 1: Errors only, 2: Warnings and Errors, 3: Basic process info, 4: Advanced process info")
    parser.add_argument('--show-timing', action='store_true', help="Shows timing information if set")
    parser.add_argument('--simultaneous-pids', default=8, help="Defines how many project IDs the client will attempt to handle simultaneously")
    parser.add_argument('--output-for-display', action='store_true', help="If set, also outputs a file for use with the Network Visualizer frontend")
    parser.add_argument('--sitename-file', default=f"{Path.cwd()}/SiteNames.json", help="File to pull node-site associations from. Only needed if compiling for display")
    parser.add_argument('--clear-raws', action='store_true', help="If set, deletes all raw files from this run after summarizing them")
    parser.add_argument('--display-aggregate', action='store_true', help="If set, writes all events for display compilation instead of auto-summarizing")
    parser.add_argument('--make-csvs', action='store_true', help="If set, also writes summary information to CSV files")
    #parser.add_argument('--overwrite-files', action='store_true', help="If set, ignores checking for cached raw files and re-pulls all data")

    args = vars(parser.parse_args())

    client = XRootESClient()

    client.new_run(args)

    #program_end = time.perf_counter()
    #tot_time = program_end - program_init

    #print(f"Overall program time: {round(tot_time,2)} seconds")
    #print(f"---Total scroll time: {round(scroll_time,2)} seconds, {round(scroll_time/tot_time*100,2)}%")
    #print(f"---Total file IO time: {round(io_time,2)} seconds, {round(io_time/tot_time*100,2)}%")
    #print(f"---Other time usage: {round(other_time,2)}, {round(other_time/tot_time*100,2)}%")
