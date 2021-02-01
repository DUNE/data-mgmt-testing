#!/usr/bin/env python3
import os

os.system("curl -XGET 'https://fifemon-es.fnal.gov/rucio-transfers-v0-2020.01/_search?q=event_type:transfer-done%20and%20country:GB&size=1'|jq > out.json")
