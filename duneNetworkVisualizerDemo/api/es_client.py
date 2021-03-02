#!/usr/bin/env python3
import sys
import json
import pprint
from elasticsearch import Elasticsearch

printer = pprint.PrettyPrinter(indent=4)

#Hard-coded value for what we think we remember the connection speed
#being. Used to calculate our network use percentage.
#Needs refining
max_speed = 100000000000 #100 gb/s

y,m,d = sys.argv[2].split('/')

output_file = "out.json"

es_cluster = "https://fifemon-es.fnal.gov"
index = f"rucio-transfers-v0-{y}.{m}"

client = Elasticsearch([es_cluster])

print("test")

es_template = {
    "query" : {
        #"bool" : {
        #    "range" : {
        #        "timestamp" : {
        #            "gt" : f"2020-12-19T00:00:00",
        #            "lte" : f"2020-12-19T23:59:59"
        #        }
        #    },
            "match": {
                "event_type" : "transfer-done"
            }
        #}
    }
}

#Function to calculate the relevant times and speeds from each transfer
#in our list of JSONS (which at this point have been converted to dictionaries)
def get_speeds(transfers):
    speed_info = []
    for transfer in transfers:
        if transfer["_source"]["event_type"] != "transfer-done":
            transfers.remove(transfer)
            continue
        #Try/except that was supposed to catch and remove the wrong types of
        #JSONs. Didn't account for a LOT of potential issues like errors.
        #Needs rewriting to handle those.
        #Also pulls the (transfer request?) creation time
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
            #Note: In the future, with stupidly long transmissions, we may
            #need to account for days, but I doubt it.
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
            transfers.remove(transfer)
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
    return speed_info, transfers

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

data_in = client.search(index = index, body=es_template)["hits"]["hits"]

if(len(data_in) == 0):
    print("Error: No finished transfers found in specified date range")
    speeds = []
else:
    speeds, data_in = get_speeds(data_in)

if(len(speeds) == 0):
    print("Error: No transfers fitting required parameters found")
    error_out = {
        "name" : "ERROR",
        "source" : "ERROR",
        "destination" : "ERROR",
        "file_size" : 0,
        "start_time" : "1970-01-01 00:00:00",
        "file_transfer_time" : "0.0",
        "transfer_speed(b/s)" : "0.0",
        "transfer_speed(MB/s)" : "0.0",
        "max_usage_percentage" : "0.0"
    }
    f = open(output_file, "w+")
    f.write(json.dumps({"data" : error_out}, indent=2))
    f.close()
    exit()



info = compile_info(data_in, speeds)
jres = json.dumps({"data": info}, indent=2)



f = open(output_file, "w+")

if jres
{
    f.write(jres)
}
else
{
    f.write("empty")
}

f.close()
