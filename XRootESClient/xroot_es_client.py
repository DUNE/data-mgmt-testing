#!/usr/bin/env python3
#Developed by Zachary Lee

#JSON needed to process incoming Elasticsearch results
#and for output formatting for easier use by webserver.
import json
import os
import sys
import time

from pathlib import Path

#ElasticSearch API is needed for interaction with Fermilab's ElasticSearch system
from elasticsearch import Elasticsearch

#Used to check that we're not looking for dates in the future
from datetime import datetime
from dateutil.relativedelta import relativedelta

import urllib
from urllib.request import urlopen, HTTPError, URLError

import argparse as ap


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

#Processes all of our transfers as individual transfers instead of summarizing
#them
def get_individual(client, curr_date, target_date, es_template, output_file, search_size):
    scroll_time = 0
    io_time = 0
    #Create output file object
    f = open(output_file, "w+")
    f.write('{ "data" : [\n')

    data_exists = False

    xfer_count = 0

    #Scrolls through the ES client and adds all information to the info array
    #compile_info now runs inside of get_speeds, so data in "info" will be in its
    #final state before export.
    while curr_date <= target_date:
        #Generates an index for the new month
        y = curr_date.strftime("%Y")
        m =  curr_date.strftime("%m")
        index = f"sam-events-v1-{y}.{m}"
        try:
            #Non-existent indices will crash the program if not properly handled
            if client.indices.exists(index=index):
                #Calling "scroll" gets the next set of results. The number
                #of results depends on the "size" parameter passed to the
                #initial search
                scroll_start = time.perf_counter()

                for data in scroll(client, index, es_template, "10m", search_size):
                    scroll_end = time.perf_counter()
                    scroll_time += scroll_end - scroll_start
                    xfer_count += len(data)
                    if len(data) > 0:
                        data_exists = True
                    start = time.perf_counter()
                    #With individual results, we write each processed transfer
                    #individually to a file. We write them after each scroll
                    #to reduce memory usage
                    for res in data:
                        if not (res == data[0] and xfer_count == len(data)):
                            f.write(",\n")
                        f.write(json.dumps(res, indent=2))
                    f.write("\n")
                    end = time.perf_counter()
                    io_time += end - start
                    scroll_start = time.perf_counter()
        except:
            print("Error: Uncaught error when looping through scroll, couldn't process response (if any), exiting...")
            f.close()
            errorHandler("scroll error", output_file)
        curr_date += relativedelta(months=+1)
    #Checks to make sure we have at least one transfer during the timeframe
    #we were handed and exports the error template if not.
    if not data_exists:
        print(f"Warning: No transfers fitting required parameters for Elasticsearch template {es_template}\n")
        f.write("]}")
        f.close()
    else:
        print(f"Search for template {es_template} contained {xfer_count} processable records.\n")
        f.write("]}")
        f.close()
    return scroll_time, io_time

#Function called when a fatal error is encountered
def errorHandler(type, output_file):
    error_out_object = [[{
            "name" : "ERROR"
    }]]

    if os.path.exists(output_file):
        os.remove(output_file)

    with open(output_file, "w+") as f:
        f.write(json.dumps({"data" : json.dumps(error_out_object)}, indent=2))
    exit()

def main(args):
    search_size = args.search_size
    scroll_time = 0
    io_time = 0

    if len(args.projects) > 0:
        projects = args.projects
    else:
        projects = [0]
    for project in projects:
        #If the end date is still the default value, it gets changed to be the start date
        start = args.start_date
        end = args.end_date
        if end == "0":
            end = start

        #End of initial variable and template setup

        #Start of main process
        target_date = datetime.strptime(end, "%Y/%m/%d")
        start_date = datetime.strptime(start, "%Y/%m/%d")
        #curr_date changes while start_date should remain constant for reuse
        curr_date = start_date

        #Makes sure the end date is later than the start date
        if target_date < curr_date:
            swap = target_date
            target_date = curr_date
            curr_date = swap

        #Sets up our initial index for the result_cutoff check
        y = curr_date.strftime("%Y")
        m =  curr_date.strftime("%m")
        index = f"sam-events-v1-{y}.{m}"



        #Changes dates from strings to individual Y M and D, and changes to datetime format
        y0,m0,d0 = curr_date.strftime("%Y/%m/%d").split('/')
        y1,m1,d1 = target_date.strftime("%Y/%m/%d").split('/')

        #Output file name
        output_file = f"{Path.cwd()}/cached_searches/out_P{project}_{y0}_{m0}_{d0}_to_{y1}_{m1}_{d1}.json"

        if Path(output_file).exists():
            print(f"Cached version of search exists. Skipping project id {project}")
            continue

        #URL of the DUNE Elasticsearch cluster
        es_cluster = "https://fifemon-es.fnal.gov"

        #Makes the Elasticsearch client
        client = Elasticsearch([es_cluster])

        #Checks if we can contact the elasticsearch cluster
        try:
            myURL = urlopen(es_cluster)
        except HTTPError as e:
            print("Error, couldn't contact elasticsearch db at: " + es_cluster + " , exiting...")
            errorHandler("network down", output_file)

        #Checks length of date arguments to ensure they're long enough
        #TODO: Replace with a better method. This doesn't cover nearly enough.
        if len(start) < 10:
            print('start date must be in format yyyy/mm/dd')
            errorHandler("date format", output_file)

        if len(end) < 10:
            print('end date must be in format yyyy/mm/dd')
            errorHandler("date format", output_file)






        #Error message is genuinely self-documenting
        if target_date > today:
            print("Error: Cannot read data from future dates, exiting...")
            errorHandler("future date",output_file)


        #Search template for Elasticsearch client
        #Queries with multiple conditions need multiple levels of
        #wrapping. Everything should be in a query, exact matches
        #should be in a "must" tag, and ranges should be in a "filter" tag.
        #Both "must" and "filter" should be wrapped in a single "bool" tag.

        if not project == 0:
            es_template = {
                "query" : {
                    "bool" : {
                        "filter" : {
                            "range" : {
                                "@timestamp" : {
                                    "gte" : f"{y0}-{m0}-{d0}",
                                    "lte" : f"{y1}-{m1}-{d1}"
                                }
                            }
                        },
                        "must" : {
                            "match": {
                                 "project_id" : project
                            }
                        }
                    }
                }
            }
        else:
            es_template = {
                "query" : {
                    "bool" : {
                        "filter" : {
                            "range" : {
                                "@timestamp" : {
                                    "gte" : f"{y0}-{m0}-{d0}",
                                    "lte" : f"{y1}-{m1}-{d1}"
                                }
                            }
                        }
                    }
                }
            }

        if not Path(f"{Path.cwd()}/cached_searches").is_dir():
            Path(f"{Path.cwd()}/cached_searches").mkdir(exist_ok=True)


        #If we don't have too many results (e.g. over 10,000, depending on configuration)
        #we process and write them to disc individually. Otherwise, we process them and write
        #them in a summarized form
        scroll_time_new, io_time_new = get_individual(client, curr_date, target_date, es_template, output_file, search_size)
        scroll_time += scroll_time_new
        io_time += io_time_new

    return scroll_time, io_time

if __name__ == "__main__":

    today = datetime.today()
    #Timings used primarily for debug and code improvements,
    #but could still be useful in the long-term.
    #Any calls of time.perf_counter are exclusively for this
    program_init = time.perf_counter()



    #Arguments for the script. The help info in the add_argument functions detail their respective uses
    parser = ap.ArgumentParser()
    parser.add_argument('-S', '--start', dest="start_date", default=today.strftime("%Y/%m/%d"), help="The earlest date to search for matching transfers. Defaults to today's date. Must be in form yyyy/mm/dd")
    parser.add_argument('-E', '--end', dest="end_date", default="0", help="The latest date to search for matching transfers. Defaults to the same value as the start date, giving a 1 day search. Must be in form yyyy/mm/dd")
    parser.add_argument('-Z', '--size', dest="search_size", default=10000, help="Number of results returned from Elasticsearch at once. Must be between 1 and 10,000")
    parser.add_argument('projects', nargs='*')


    args = parser.parse_args()

    if args.search_size > 10000 or args.search_size < 1:
        print("Please select a search size from 1 to 10,000")
        errorHandler("search_size")

    #if len(args.projects) < 1:
    #    print("Please enter at least one project id")
    #    errorHandler("project_ids")

    scroll_time, io_time = main(args)

    program_end = time.perf_counter()
    tot_time = program_end - program_init
    other_time = tot_time - scroll_time - io_time

    print(f"Overall program time: {round(tot_time,2)} seconds")
    print(f"---Total scroll time: {round(scroll_time,2)} seconds, {round(scroll_time/tot_time*100,2)}%")
    print(f"---Total file IO time: {round(io_time,2)} seconds, {round(io_time/tot_time*100,2)}%")
    print(f"---Other time usage: {round(other_time,2)}, {round(other_time/tot_time*100,2)}%")
