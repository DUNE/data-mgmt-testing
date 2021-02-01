#!/usr/bin/env python3
import sys
from elasticsearch import Elasticsearch


y,m,d = sys.argv[1].split('/')

output_file = sys.argv[2]

es_cluster = "https://fifemon-es.fnal.gov"
index = f"rucio-transfers-v0-{y}.{m}"

client = Elasticsearch([es_cluster])

es_template = {
    "query" : {
        "range" : {
            "@timestamp" : {
                "gt" : f"{y}-{m}-{d}, 00:00:00",
                "lte" : f"{y}-{m}-{d}, 23:59:59"
            }
        },
        "match": {
            "event_type" : "transfer-done"
        }
    }
}

res = client.search_template(index = index, body = es_template)

output_string = ""

#Formats for the previously made JSON parser
for result in res:
    output_string += str(result) + "\n"

f = open(output_file, "w+")
f.write(output_string)
f.close()
