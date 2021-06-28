#!/usr/local/opt/python/libexec/bin/python
import os,sys,csv,string,json,datetime,dateutil,jsonlines
import requests


from datetime import date,timezone,datetime
from dateutil import parser

import samweb_client
samweb = samweb_client.SAMWebClient(experiment='dune')
# get list of sam project ID's to send to GetProjectInfo

def getProjectList(begin,end,n=10):
  cleaned = []
  names = []
  projects = samweb.listProjects(started_after=begin,started_before=end)
  c = 0
  for i in range(0,len(projects)):
    p = projects[i].decode('UTF-8')
    #if "prestage" in p:
    #  continue
    md = samweb.projectSummary(p)
    pid = md["project_id"]
     
    name = md["project_name"]
    print (name)
    url = samweb.findProject(name)
    print (md)
    for item in md:
      #print ("item",item,md[item])
      if item == "processes":
        l = md[item]
        #print ("list",l)
        for p in l:
          print (p)
          prid = p["process_id"]
          processurl = "%s/processes/%d"%(url,prid)
          print (pid,prid,processurl)
    
    if c < n:
      names.append(p)
      cleaned.append(pid)
    # limit the list
    c += 1
    #print ("cleaned",cleaned[0:min(len(cleaned),n)])
  return [cleaned[0:min(len(cleaned),n)],names[0:min(len(cleaned),n)]]

getProjectList("2021-05-01","2021-05-03")
