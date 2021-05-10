#!/usr/bin/env python3


#JSON needed to process incoming Elasticsearch results
#and for output formatting for easier use by webserver.
import json

import os
import sys
import urllib



#ElasticSearch API is needed (alternatively commands could be manually written
#with REST Python API) for interaction with Fermilab's ElasticSearch system
from elasticsearch import Elasticsearch

#Used to check that we're not looking for dates in the future
from datetime import datetime
from dateutil.relativedelta import relativedelta



from urllib.request import urlopen, HTTPError, URLError


#Argparse will be used for proper argument handling in the long-term.
import argparse as ap

today = datetime.today()



print("These were the paramters passed to es_client.py: \n")
print(sys.argv)
print("\n")




#Arguments for the script. Help info should explain uses
#TODO: Add help info
parser = ap.ArgumentParser()
parser.add_argument('-S', '--start', dest="start_date", default=today.strftime("%Y/%m/%d"))
parser.add_argument('-E', '--end', dest="end_date", default="0")
#parser.add_argument('-R', '--rule_id', dest="rule_id")
#parser.add_argument('-U', '--user', dest="user")
parser.add_argument('-M', '--mode', dest="mode", default=0)


args = parser.parse_args()

start = args.start_date
end = args.end_date
if end == "0":
    end = start

#Hard-coded value for what we think we remember the connection speed
#being. Used to calculate our network use percentage.
#Completely inaccurate and was never actually used. Should likely be removed.
#Only haven't because need to check if changes to JSON format would cause
#issues with other code.
max_speed = 100000000000 #100 gb/s



#How many search results we want to return.
#Capping search results at 2,500 since API documentation recommends keeping searches
#relatively small and stepping through with search_after instead of using massive search
#volumes and scroll.
search_size = 5000


if len(start) < 10:
    print('start date must be in format yyyy/mm/dd')
    errorHandler("date format")

if len(end) < 10:
    print('end date must be in format yyyy/mm/dd')
    errorHandler("date format")


y0,m0,d0 = start.split('/')
y1,m1,d1 = end.split('/')


#Search template for Elasticsearch client
#Queries with multiple conditions need multiple levels of
#wrapping. Everything should be in a query, exact matches
#should be in a "must" tag, and ranges should be in a "filter" tag.
#Both "must" and "filter" should be wrapped in a single "bool" tag.

mode = int(args.mode)



#Hardcoded output file name
if mode in [0, 1, 2, 3]:
    output_file = "out.json"
elif mode == 4:
    output_file = "fails.json"





#URL of the DUNE Elasticsearch cluster
es_cluster = "https://fifemon-es.fnal.gov"

#Mode codes:
#   0 : All events of type "transfer-done" that are not from the regular checkup
#   1 : All events of type "transfer-done" from the regular checkup
#   2 : All events of type "transfer-done" regardless of whether source
#   3 : Not currently remembered. Will work on that.
#   4 : All events of types "transfer-failed" and "transfer-queued_failed"
if mode == 0:
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
                         "event_type" : "transfer-done"
                    }
                },
                "must_not" : {
                    "wildcard" : {
                        "name" : "1gbtestfile.*"
                    }
                }
            }
        }
    }

elif mode == 1 or mode == 3:
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
                         "event_type" : "transfer-done"
                    },
                },
                "should" : {
                    "wildcard" : {
                        "name" : "1gbtestfile.*"
                    }
                },
                "minimum_should_match" : 1,
            }
        }
    }
elif mode == 2:
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
                         "event_type" : "transfer-done"
                    }
                }
            }
        }
    }
elif mode == 4:             #Checks failures
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
                    }
                ],
                "minimum_should_match" : 1
            }
        }
    }







def errorHandler(type):
    error_out_object = [[{
            "name" : type,
            "source" : "ERROR",
            "destination" : "ERROR",
            "file_size" : 0,
            "start_time" : "1970-01-01 00:00:00",
            "file_transfer_time" : "0.0",
            "transfer_speed(b/s)" : "0.0",
            "transfer_speed(MB/s)" : "0.0",
            "max_usage_percentage" : "0.0"
    }]]

    if os.path.exists(output_file):
        os.remove(output_file)

    with open(output_file, "w+") as f:
        f.write(json.dumps({"data" : json.dumps(error_out_object)}, indent=2))
    exit()









target_date = datetime.strptime(end, "%Y/%m/%d")
curr_date = datetime.strptime(start, "%Y/%m/%d")

if target_date < curr_date:
    swap = target_date
    target_date = curr_date
    curr_date = swap


if target_date > today:
    print("Error: Cannot read data from future dates, exiting...")
    errorHandler("future date")









#check if network is up
try:
    myURL = urlopen(es_cluster)
except HTTPError as e:
    print("Error, couldn't contact elasticsearch db at: " + es_cluster + " , exiting...")
    errorHandler("network down")



#Makes the Elasticsearch client
client = Elasticsearch([es_cluster])

#End of initial variable and template setup

#Beginning of function definitions

#Function to compile all of our transfer info and speed info into a
#dictionary for conversion to JSON and export.
def compile_info(transfers, speed_info):
    json_strings = []
    #Compiles each dictionary
    for i in range(len(transfers)):
        new_json = {
            "name": transfers[i]["_source"]["name"],
            "source": transfers[i]["_source"]["src-rse"],
            "destination": transfers[i]["_source"]["dst-rse"],
            "file_size": transfers[i]["_source"]["file-size"],
            "start_time": transfers[i]["_source"]["started_at"],
            "file_transfer_time": str(speed_info[i]["file_transfer_time"]),
            "transfer_speed(b/s)": str(speed_info[i]["transfer_speed(b/s)"]),
            "transfer_speed(MB/s)": str(speed_info[i]["transfer_speed(MB/s)"]),
            "max_usage_percentage": str(speed_info[i]["max_usage_percentage"])
        }
        #Appends it to the output list
        json_strings.append(new_json)
    #Returns the output list
    return json_strings

def get_errs(transfers):
    json_strings = []
    for transfer in transfers:
        try:
            new_json = {
                "name": transfer["_source"]["name"],
                "source": transfer["_source"]["src-rse"],
                "destination": transfer["_source"]["dst-rse"]
            }
            if transfer["_source"]["event_type"] == "transfer-failed":
                new_json["reason"] = "rx_error"
            else:
                new_json["reason"] = "tx_error"
            json_strings.append(new_json)
        except:
            print(f"transfer {transfer} caused a problem")
    return json_strings

#Function to calculate the relevant times and speeds from each transfer
#in our list of JSONS (which at this point have been converted to dictionaries)
def get_speeds(transfers):
    speed_info = []
    to_remove = []
    for transfer in transfers:
        if transfer["_source"]["event_type"] != "transfer-done":
            transfers.remove(transfer)
            continue
        #Pulls request creation time
        c_time = transfer["_source"]["created_at"]
        #Pulls the (transfer request?) submission time, the transfer start time,
        #and the transfer end time, as well as the file size
        sub_time = transfer["_source"]["submitted_at"]
        start_time = transfer["_source"]["started_at"]
        fin_time = transfer["_source"]["transferred_at"]
        f_size = float(transfer["_source"]["bytes"]) * 8

        #Places our relevant times into an array for processing
        time_arr = [c_time, sub_time, start_time, fin_time]
        len_arr = []

        #Finds the time differences for each set of times (creation to submission,
        #submission to starting, and transmission start to transmission end)
        #Initial time format is YYYY:M:DTH:M:SZ where T separates the year-month-day
        #portion from the hours-minutes-second portion and the Z denotes UTC
        for i in range(len(time_arr)-1):
            #Gets our times from the JSON's format into a workable form
            #First divides
            split_1 = time_arr[i].split()
            split_2 = time_arr[i + 1].split()
            #Splits the hour-minute-second portion into individual pieces
            #Note: In the future, with very long transmissions or transmissions
            #started around midnight we may need to account for days, but I doubt it.
            t_1 = split_1[1].split(':')
            t_2 = split_2[1].split(':')

            #Pulls the difference between each individual number set
            #The hours, minutes, and seconds being greater in
            #the start time than in the end time are accounted for later on
            h_diff = float(t_2[0]) - float(t_1[0])
            m_diff = float(t_2[1]) - float(t_1[1])
            s_diff = float(t_2[2]) - float(t_1[2])

            #Accounts for the hour being higher in the start time, which would
            #mean that the day had turned over at some point.
            if h_diff < 0:
                h_diff += 24

            #The difference in minutes and seconds being negative is accounted
            #for here. The hour difference (thanks to the code above) should
            #always be 0 or greater. If the minutes and seconds are different,
            #then the hours should always be greater than zero, so the overall
            #time will still be positive.
            tot_time = (h_diff * 60 + m_diff) * 60 + s_diff

            #Appends each time to the time length array
            len_arr.append(tot_time)

        #Calculates the transfer speed from the transfer start to end time and
        #the file size
        transfer_speed = f_size/len_arr[2]
        #Filters out transfers with abnormally short transfer times
        if len_arr[2] < 10.0 or len_arr[2] > 12 * 60 * 60:
            to_remove.append(transfer)
            continue
        #Fills our speed information dictionary for this JSON object
        info = {
            "creation_to_submission": len_arr[0],
            "submission_to_started": len_arr[1],
            "file_transfer_time": len_arr[2],
            "transfer_speed(b/s)": transfer_speed,
            "transfer_speed(MB/s)": f_size/8/len_arr[2]/1024/1024,
            "max_usage_percentage": transfer_speed/max_speed*100
        }
        #Appends the dictionary to our array of output dictionaries.
        speed_info.append(info)
    #Returns the speed info and the transfer array (in case it's been modified
    #and had badly formatted stuff or incorrect request types removed)
    for transfer in to_remove:
        transfers.remove(transfer)
    if len(transfers) > 0:
        return compile_info(transfers, speed_info)
    else:
        return []

#Based on Simplernerd's tutorial here: https://simplernerd.com/elasticsearch-scroll-python/
#Scrolls through all of the results in a given date range
def scroll(es, idx, body, scroll):
    page = es.search(index=idx, body=body, scroll=scroll, size=search_size)
    scroll_id = page['_scroll_id']
    hits = page['hits']['hits']
    while len(hits):
        yield hits
        page = es.scroll(scroll_id=scroll_id, scroll=scroll)
        scroll_id = page['_scroll_id']
        hits = page['hits']['hits']


#End of function definitions

#Start of main process

#Create output file object and empty info array
f = open(output_file, "w+")
f.write('{ "data" : [\n')

data_exists = False

xfer_count = 0

#Scrolls through the ES client and adds all information to the info array
#compile_info now runs inside of get_speeds, so data in "info" will be in its
#final state before export.
while curr_date <= target_date:
    y = curr_date.strftime("%Y")
    m =  curr_date.strftime("%m")
    #Index for the specified month
    index = f"rucio-transfers-v0-{y}.{m}"
    #Modes for transfer_done events with various templates
    if mode in [0, 1, 2, 3]:
        try:
            if client.indices.exists(index=index):
                for data in scroll(client, index, es_template, "5m"):
                    info = get_speeds(data)
                    xfer_count += len(info)
                    if len(info) > 0:
                        data_exists = True
                    for res in info:
                        f.write(json.dumps(res, indent=2))
                        f.write(",\n")
        except:
            print("Error: Uncaught error when looping through scroll, couldn't process response (if any), exiting...")
            f.close()
            errorHandler("scroll error")
    #Mode to process transfer_failed and transfer-queued_failed data
    elif mode == 4:
        try:
            if client.indices.exists(index=index):
                for data in scroll(client, index, es_template, "5m"):
                    try:
                        info = get_errs(data)
                    except:
                        print("get_errs failed")
                    xfer_count += len(info)
                    if len(info) > 0:
                        data_exists = True
                    for res in info:
                        f.write(json.dumps(res, indent=2))
                        f.write(",\n")
        except:
            print("Error: Uncaught error when looping through scroll, couldn't process response (if any), exiting...")
            f.close()
            errorHandler("scroll error")
    curr_date += relativedelta(months=+1)
#Checks to make sure we have at least one transfer during the timeframe
#we were handed and exports the error template if not.
if not data_exists:
    print("Error: No transfers fitting required parameters found")
    f.close()
    errorHandler("no results")

print(f"Period contained {xfer_count} processable records.")

#the JSON output was wrong and crashing, so this removes the last trailling comma 
f.seek(0, os.SEEK_END)
f.seek(f.tell()-2, os.SEEK_SET)
f.truncate()


f.write("]}")

f.close()
