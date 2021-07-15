#!/usr/local/opt/python/libexec/bin/python
import os,sys,csv,string,json,datetime,dateutil
import requests


from datetime import date,timezone,datetime
from dateutil import parser

import samweb_client
samweb = samweb_client.SAMWebClient(experiment='dune')

p = samweb.projectSummary("ehinkle_RITM0986948_MC_1GeV_reco_sce_off_20210204093952")

s = json.dumps(p,indent=4)
f = open("dump.json",'w')
f.write(s)

