#!/usr/bin/env python3
#Developed by Zachary Lee
#with work by Dr. Heidi Schellman as reference

#JSON needed to process incoming Elasticsearch results
#and for output formatting for easier use by webserver.
import json
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

class XRootESClient():
    def __init__(self):
        #Debug levels:
        #   0: No debug message
        #   1: Show errors
        #   2: Show warnings
        #   3: Show process information
        #   4: Show timing data
        self.debug = 1
        self.args = None
        self.max_retries = 30

        self.fids = {}
        self.pids = {}
        self.finished_data = queue.Queue()

        self.fid_lock = threading.Lock()

        self.compiler_overseer = None
        self.sam_overseer = None
        self.es_overseer = None
        self.writer_thread = None

    def reset_vals(self):
        self.args = None
        self.fids = {}
        self.pids = {}
        self.finished_data = queue.Queue()
        self.fid_lock = threading.Lock()
        self.compiler_overseer = None
        self.sam_overseer = None
        self.es_overseer = None
        self.writer_thread = None


    def set_debug_level(self, level):
        self.debug = level

    def new_run(self, args):
        self.args = args
        if self.args.end_date == "0":
            self.args.end_date = self.args.start_date
        if self.debug > 2:
            print(f"Args recieved: {args}")
        self.samweb = samweb_client.SAMWebClient(experiment=args.experiment)
        self.get_proj_list()
        if self.debug > 2:
            print(f"Projects gotten: {self.pids}")
        self.es_overseer = threading.Thread(target=self.es_overseer_func, daemon=True)
        self.sam_overseer = threading.Thread(target=self.sam_overseer_func, daemon=True)
        self.compiler_overseer = threading.Thread(target=self.compiler_overseer_func, daemon=True)
        self.writer_thread = threading.Thread(target=self.writer, daemon=True)
        self.es_overseer.start()
        time.sleep(1)
        self.sam_overseer.start()
        time.sleep(1)
        self.compiler_overseer.start()
        time.sleep(1)
        self.writer_thread.start()
        time.sleep(1)
        self.writer_thread.join()


    def writer(self):
        proj_files = {}

        if not Path(f"{Path.cwd()}/cached_searches").is_dir():
            Path(f"{Path.cwd()}/cached_searches").mkdir(exist_ok=True)

        for pid in self.pids.keys():
            proj_files[pid] = open(f"{Path.cwd()}/cached_searches/raw_{self.args.start_date}_{self.args.end_date}_{pid}.jsonl", "w+")

        while self.compiler_overseer.is_alive() or not self.finished_data.empty():
            if self.finished_data.empty():
                time.sleep(1)
                continue
            to_write = self.finished_data.get()
            proj_files[to_write["project_id"]].write(json.dumps(to_write))
            proj_files[to_write["project_id"]].write("\n")
            self.finished_data.task_done()

        for pid in self.pids:
            proj_files[pid].close()

    #site_finder code here taken from XRootParser code by Dr. Heidi Schellman
    def site_finder(self, source):
        knownsites = [".gov",".fr",".es",".br",".edu",".in",".cz",".uk",".fr",".ch",".nl",".ru"]
        specials = {"dice":"dice.bristol.uk","CRUSH":"crush.syracuse.edu","gridpp.rl.ac.uk":"gridpp.rl.ac.uk","comp20-":"lancaster.uk","discovery":"nmsu.edu","nid":"nersc.lbnl.gov","wn-2":"unibe-lhcp.bern.ch","qmul":"esc.qmul.uk","uct2":"uct2-mwt2.uchicago.edu","nubes.stfc.ac.uk":"nubes.stfc.ac.uk","unl.edu":"unl.edu","wn0":"sheffield.uk","wn1":"sheffield.uk","wn2":"sheffield.uk","local-":"pr"}
        if "node" in source:
            node = source["node"]
            for s in specials:
                if s in node:
                    source["site"] = specials[s]
                    return source["site"]
            if not "." in node:
                source["site"] = "unknown"
                return source["site"]
            country = "."+node.split(".")[-1]
            #print ("country guess",country)
            if country in knownsites:
                source["site"]= node[node.find(".")+1:]
                return source["site"]
            else:
                #print ("unidentified site",node,specials)
                source["site"] = "mystery-site"
        else:
            source["site"] = "unknown"
        return source["site"]

    def compiler_overseer_func(self):
        for pid in self.pids.keys():
            self.pids[pid]["compiler_worker"] = threading.Thread(target=self.compiler_worker_func,args=(pid,),daemon=True)
            self.pids[pid]["compiler_worker"].start()
        for pid in self.pids.keys():
            self.pids[pid]["compiler_worker"].join()

    #Compiles all data into finished form for a given project
    def compiler_worker_func(self, pid):
        while self.pids[pid]["sam_worker"].is_alive() or not self.pids[pid]["write_queue"].empty():
            if self.pids[pid]["write_queue"].empty():
                time.sleep(1)
                continue
            next_file = self.pids[pid]["write_queue"].get()
            self.pids[pid]["write_queue"].task_done()
            self.fid_lock.acquire()
            retry_count = 0
            fid = next_file["file_id"]
            while not fid in self.fids and retry_count < self.max_retries:
                retry_count += 1
                self.fid_lock.release()
                time.sleep(1)
                self.fid_lock.acquire()
            if retry_count >= self.max_retries:
                if self.debug > 0:
                    print(f"Error: Timed out waiting for FID {fid} metadata. Skipping.")
            else:
                fid_md = self.fids[fid]
            self.fid_lock.release()
            try:
                if "eos" in next_file["file_url"]:
                    file_location = "eospublic.cern.ch"
                else:
                    file_location = next_file["file_url"].replace("https://","").replace("root://","").split("/")[0].split(":")[0]

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
                    "application" : self.pids[pid]["metadata"]["processes"][0]["application"]["name"]
                }

                self.finished_data.put(new_entry)

            except Exception as e:
                if self.debug > 1:
                    md = self.pids[pid]["metadata"]
                    print(f"Warning: Could not process entry FID {fid} for entry {next_file} and project metadata {md}")
                    print(f"Exception {e} was thrown")

    def sam_overseer_func(self):
        for pid in self.pids.keys():
            self.pids[pid]["sam_worker"] = threading.Thread(target=self.sam_worker_func,args=(pid,),daemon=True)
            self.pids[pid]["sam_worker"].start()
        for pid in self.pids.keys():
            self.pids[pid]["sam_worker"].join()

    def sam_worker_func(self, pid):
        while self.pids[pid]["es_worker"].is_alive() or not self.pids[pid]["fid_queue"].empty():
            if self.pids[pid]["fid_queue"].empty():
                time.sleep(1)
                continue
            to_search = self.pids[pid]["fid_queue"].get()
            data = self.samweb.getMultipleMetadata(to_search)
            self.fid_lock.acquire()
            for item in data:
                try:
                    self.fids[item['file_id']] = item
                except:
                    print(f"Could not process item {item}")
            self.fid_lock.release()

    def es_overseer_func(self):
        for pid in self.pids.keys():
            self.pids[pid]["es_worker"] = threading.Thread(target=self.es_worker_func,args=(pid,),daemon=True)
            self.pids[pid]["es_worker"].start()
        for pid in self.pids.keys():
            self.pids[pid]["es_worker"].join()

    def es_worker_func(self, pid):
        curr_date = date_parser.parse(self.args.start_date)
        target_date = date_parser.parse(self.args.end_date)
        y0,m0,d0 = curr_date.strftime("%Y-%m-%d").split('-')
        es_template = {
            "query" : {
                "bool" : {
                    "filter" : {
                        "range" : {
                            "@timestamp" : {
                                "gte" : f"{y0}-{m0}-{d0}"
                            }
                        }
                    },
                    "must": {
                        "match": {
                             "project_id" : pid
                        }
                    },
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
                for data in scroll(self.pids[pid]["es_client"], index, es_template, "5m", self.args.search_size):
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

    def get_proj_list(self):
        projects = self.samweb.listProjects(started_after=self.args.start_date, started_before=self.args.end_date)
        for proj in projects:
            if "prestage" in proj:
                continue
            md = self.samweb.projectSummary(proj)
            pid = md["project_id"]
            self.pids[pid] = {}
            self.pids[pid]["metadata"] = md
            self.pids[pid]["es_client"] = Elasticsearch([self.args.es_cluster], timeout=120)
            self.pids[pid]["write_queue"] = queue.Queue()
            self.pids[pid]["fid_queue"] = queue.Queue()

    #Function called when a fatal error is encountered
    def errorHandler(self, type):
        print(f"Error of type {type} occured")
        exit()

if __name__ == "__main__":

    today = datetime.today()
    #Timings used primarily for debug and code improvements,
    #but could still be useful in the long-term.
    #Any calls of time.perf_counter are exclusively for this
    program_init = time.perf_counter()

    #Arguments for the script. The help info in the add_argument functions detail their respective uses
    parser = ap.ArgumentParser()
    parser.add_argument('-S', '--start', dest="start_date", default=today.strftime("%Y-%m-%d"), help="The earlest date to search for matching transfers. Defaults to today's date. Must be in form yyyy-mm-dd")
    parser.add_argument('-E', '--end', dest="end_date", default="0", help="The latest date to search for matching transfers. Defaults to the same value as the start date, giving a 1 day search. Must be in form yyyy-mm-dd")
    parser.add_argument('-Z', '--size', dest="search_size", default=1000, help="Number of results returned from Elasticsearch at once. Must be between 1 and 1,000 to accomodate limits of SAM metadata access")
    parser.add_argument('-U', '--user', dest="user", default="", help="Searches for a specific user")
    parser.add_argument('-X', '--experiment', dest="experiment", default="dune", help="Searches for a specific experiment")
    parser.add_argument('-C', '--cluster', dest='es_cluster', default="https://fifemon-es.fnal.gov", help="Specifies the Elasticsearch cluter to target")
    parser.add_argument('projects', nargs='*')

    args = parser.parse_args()

    client = XRootESClient()

    client.set_debug_level(3)
    client.new_run(args)

    program_end = time.perf_counter()
    tot_time = program_end - program_init

    print(f"Overall program time: {round(tot_time,2)} seconds")
    #print(f"---Total scroll time: {round(scroll_time,2)} seconds, {round(scroll_time/tot_time*100,2)}%")
    #print(f"---Total file IO time: {round(io_time,2)} seconds, {round(io_time/tot_time*100,2)}%")
    #print(f"---Other time usage: {round(other_time,2)}, {round(other_time/tot_time*100,2)}%")
