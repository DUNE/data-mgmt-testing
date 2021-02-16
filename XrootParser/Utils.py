#!/usr/local/opt/python/libexec/bin/python
import os,sys,csv,string,json,datetime,dateutil
import requests

from datetime import date,timezone,datetime
from dateutil import parser

import samweb_client
samweb = samweb_client.SAMWebClient(experiment='dune')

# read in the json from a file
def jsonReader(configfile):
  if os.path.exists(configfile):
    with open(configfile,'r') as f:
      myjson= json.load(f)
  else:
    print ("no config file",configfile)
    sys.exit(0)
  return myjson


# clean up a record from elastic search - require file_id and drop prestage activities.
def Cleaner(info):

  drops = ["kafka","type","station","@version"]
  dropevents = ["start_cache_check","end_cache_check","end_stage_file","file_staged","update_process_state","end_process"]

  #print ('info',info)
  clean = []
  layer1 = info["hits"]
  layer2 = layer1["hits"]
  count = 0
  for item in layer2:
    count += 1
    source = item["_source"]
    if source["event"] in dropevents:
      continue
    for a in drops:
      source.pop(a)
    
    source["timestamp"] = human2number(source["@timestamp"])
    
    if not "file_id" in source:
      continue
    if "file_url" in source:
      source["file_location"] = source["file_url"].split(":")[1][2:]
    if( count < 2):
      jsonprint (source)
    clean.append(source)
  return clean
  
# print a dictionary as pretty json
def jsonprint(j):
  print(json.dumps(j, indent=4, sort_keys=True))

#def isofixer(stamp):
#  #stamp = stamp.replace("T"," ")
#  stamp = stamp.replace("Z","+00.00")
#  return stamp

# transform a number from human to epoch format
def human2number(stamp):
  parsed = parser.isoparse(stamp)
  #print ( parsed.timestamp())
  return parsed.timestamp()

# invert the transfom back to iso human UTC
def number2human(stamp):
    
    t = datetime.fromtimestamp(stamp,tz=timezone.utc)
    return t.isoformat() + 'Z'
    
    
# get info from the sam-events elasticsearch for a given project
def getProjectInfo(projectID):
 
  urltemplate = "https://fifemon-es.fnal.gov/sam-events-v1-2021.0*/_search?q=experiment:dune%20and%20project_name:ID&size=10000"
  theurl = urltemplate.replace("ID",projectID)
  print (theurl)
  result = requests.get(theurl)
  return json.loads(result.text)

# get list of sam project ID's to send to GetProjectInfo

def samProjectIDs(begin,end):
  result = []
  projects = samweb.listProjects(user="dunepro",started_after=begin,started_before=end)
  for bp in list(projects):
    p = bp.decode('UTF-8')
    print ("project",p)
    if "prestage" in p:
      print ("skip prestage",p )
      continue
    result += Cleaner(getProjectInfo(p))
    #//summary = samweb.projectSummary(p)
    #print (summary)
  return result
  
def cleanRecord(record,uselist):
  newrecord = record
  for key in record:
    if key not in uselist:
      newrecord.pop(key)
  return newrecord
      
  

# test the stuff above
def test():
#  test = "2021-02-01T01:59:00.804Z"
#  print (test,human2number(test),number2human(human2number(test)))
#  new = jsonReader("494483.json")
#  print ("result",Cleaner(new)[3])
 # a =  (getProjectInfo("494483"))
#  b = Cleaner(a)
#  jsonprint(b)
  jsonprint(samProjectIDs("2021-01-01","2021-02-15"))
  

  
test()
