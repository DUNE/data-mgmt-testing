#!/usr/local/opt/python/libexec/bin/python
import os,sys,csv,string,json,datetime,dateutil,jsonlines
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

def fileFinder(source):
  if "file_url" in source:
    if "eos" in source["file_url"]:
      source["file_location"] = "eospublic.cern.ch"
      return source
    tmp = source["file_url"].replace("https://","")
    tmp = tmp.replace("root://","")
    tmp = tmp.split("/")[0]
    tmp = tmp.split(":")[0]
#    print ("file",tmp)
    source["file_location"] = tmp
    
  return source

def siteFinder(source):
  
  knownsites = [".gov",".fr",".es",".br",".edu",".in",".cz",".uk",".fr",".ch",".nl"]
  specials = {"dice":"dice.bristol.uk","CRUSH":"crush.syracuse.edu","gridpp.rl.ac.uk":"gridpp.rl.ac.uk","comp20-":"lancaster.uk","discovery":"nmsu.edu","nid":"nersc.lbnl.gov","wn-2":"unibe-lhcp.bern.ch","qmul":"esc.qmul.uk","uct2":"uct2-mwt2.uchicago.edu","nubes.stfc.ac.uk":"nubes.stfc.ac.uk","unl.edu":"unl.edu" }
  if "node" in source:
    node = source["node"]
    for s in specials:
      if s in node:
        source["site"] = specials[s]
        return source
    if not "." in node:
      return source
    country = "."+node.split(".")[-1]
    #print ("country guess",country)
    if country in knownsites:
      source["site"]= node[node.find(".")+1:]
      return source
    else:
      print ("unidentified site",node,specials)
      source["site"] = "mystery-site"
  else:
    source["site"] = "unknown"
  return source
  
def Cleaner(info,projectmeta):

  drops = ["type","station","@version","kafka"]
  dropevents = ["start_cache_check","end_cache_check","start_stage_file","end_stage_file","file_staged","update_process_state","end_process","handle_storage_system_error"]
  drops += ["files in snapshot", "first_name","group_id","group_name","last_name","person_id","processes","project_desc","station_id","experiment","file_name","event_time"]
 
  #print ('info',info)
  clean = []
  project_id = projectmeta["project_id"]
  layer1 = info["hits"]
  layer2 = layer1["hits"]
  count = 0
  for item in layer2:
    count += 1
    source = item["_source"]
    if source["event"] in dropevents:
      continue
    
    if source["project_id"] != project_id:
      #print (" got the wrong project? ",project_id, source["project_id"])
      continue
    source["timestamp"] = human2number(source["@timestamp"])
    
    if not "file_id" in source:
      continue
    if "file_state" in source and source["file_state"] == "delivered":
      continue
    source = fileFinder(source)
    source = siteFinder(source)
    # add project metadata
    for meta in projectmeta:
      source[meta] = projectmeta[meta]
    # clean out stuff we don't need
    for a in drops:
      if a in source:
        source.pop(a)
        
    if( count < 2 or "eos" in source["file_location"]):
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
 
  urltemplate = "https://fifemon-es.fnal.gov/sam-events-v1-202*/_search?q=experiment:dune%20and%20project_id:ID&size=10000"
  theurl = urltemplate.replace("ID","%s"%projectID)
  print (theurl)
  try:
    result = requests.get(theurl)
  except:
    print("request failed - maybe you need the VPN")
    result={}
  return json.loads(result.text)

# get list of sam project ID's to send to GetProjectInfo

def getProjectList(begin,end,n=10):
  cleaned = []
  projects = samweb.listProjects(started_after=begin,started_before=end)
  c = 0
  for i in range(0,len(projects)):
    p = projects[i].decode('UTF-8')
    if "prestage" in p:
      continue
    if c < n:
      cleaned.append(p)
    # limit the lists
    c += 1
    #print ("cleaned",cleaned[0:min(len(cleaned),n)])
  return cleaned[0:min(len(cleaned),n)]
    
def getProjectMeta(p):
  md = samweb.projectSummary(p)
  
  processes = md["processes"]
  
  brief = md
  brief["processes"] = None
  if len(processes) > 0:
    application = processes[0]["application"]["name"]
    brief["application"]=application
  return brief

def findProjectInfo(projects):
  result = []
  for p in projects:
    print ("project",p)
    if "prestage" in p:
      print ("skip prestage",p )
      continue
    m = getProjectMeta(p)
    id = m["project_id"]
    record =  Cleaner(getProjectInfo(id),m)
    
    result += record
      
    #//summary = samweb.projectSummary(p)
    #print (summary)
  return result
  
# remove records we don't need
  
def cleanRecord(record,uselist):
  newrecord = record
  for key in record:
    if key not in uselist:
      newrecord.pop(key)
  return newrecord
      
# build a map with timestamps sorted
def buildMap(records):
# sort the records into buckets by project_id, file_id and timestamp
  infomap = {}
  sortedmap = {}
  for record in records:
    if type(record) != type({}):
      print ("strange record",record)
      continue
    if "project_id" not in record or "file_id" not in record:
      continue
    pid = record["project_id"]
    fid = record["file_id"]
    t = record["timestamp"]
    if type(pid) == type([]):
      print ("strange type",pid,type(pid),type(1))
      pid = pid[0]
      record["project_id"] = pid
      jsonprint (record)
      continue
    
    if pid not in infomap.keys():
      infomap[pid] = {}
      sortedmap[pid] = {}
    if fid not in infomap[pid]:
      infomap[pid][fid] = {}
      sortedmap[pid][fid] = []
    if t in infomap[pid][fid]:
      print ("duplicate record",pid,fid,t)
      continue
    infomap[pid][fid][t] = record
# make a sorted list of times that share the file_size info
  for p in infomap:
      for f in infomap[p]:
        times = infomap[p][f].keys()
        # try to recover missing information from one of the records
        md = samweb.getMetadata(f)
        file_size = md["file_size"]
         
        version = None
        campaign = None
        if "DUNE.campaign" in md:
          campaign = md["DUNE.campaign"]
        
        data_tier = md["data_tier"]
        
        for t in times:
          infomap[p][f][t]["file_size"] = file_size
          infomap[p][f][t]["Campaign"] = campaign
          
          infomap[p][f][t]["data_tier"] = data_tier
        sortedtimes = sorted(times)
        #print ("sorted times", times, sortedtimes)
        for s in range(0,len(sortedtimes)):
#          if "file_size" not in infomap[p][f][sortedtimes[s]] and file_size != None:
#            infomap[p][f][sortedtimes[s]]["file_size"] = file_size
          sortedmap[p][f].append ( infomap[p][f][sortedtimes[s]])
          
  return sortedmap
      
# log first and last records and calculate duration

def sequence(info):
  actions = []
  for pid in info:
    for fid in info[pid]:
      records = info[pid][fid]
      sum = {}
      f = 0
      first = records[0]
      last = records[len(records)-1]
      sum = first
      #print (sum["file_size"])
      sum["actions"] = len(records)
      sum["last_file_state"]=last["file_state"]
      sum["last_timestamp"]=last["timestamp"]
      sum["duration"]=last["timestamp"]-first["timestamp"]
      if "file_size" in sum and "duration" in sum and sum["file_size"] != None and sum["duration"] != 0:
        sum["rate"]=sum["file_size"]/sum["duration"]*0.000001
      actions.append(sum)
      #jsonprint (sum)
  return actions

# test the stuff above
def test(first = "2021-02-01", last = "2021-02-15", n=10000):

#  ids = getProjectList(first,last,n)
#  print ("this many projects",len(ids))
#  if len(ids) > 2:
#    getProjectMeta(ids[1])
#  # first get the info
#  info = findProjectInfo(ids)
#  e = open("raw_%s_%s.json"%(first,last),'w')
#  s = json.dumps(info, indent=2)
#  e.write(s)
#  with jsonlines.open("raw_%s_%s.jsonl"%(first,last), mode='w') as writer:
#    for i in info:
#      writer.write(i)
#  e.close
#  info = None
#  
  # then make it into a map
  e = open("raw_%s_%s.json"%(first,last),'r')
  info = json.load(e)
  result = buildMap(info)
  info = None
  #f = open("results_%s_%s.json"%(first,last),'w')
 # s  = json.dumps(result, indent=2)
  #f.write(s)
  #f.close()
  #result = None
  
  # then use the time sorted info to record first and last actions for each file
  
 # f = open("results_%s_%s.json"%(first,last),'r')
  info = result
  new = sequence(info)
  g = open("summary_%s_%s.json"%(first,last),'w')
  s = json.dumps(new,indent=2)
  g.write(s)
  g.close()
  with jsonlines.open("summary_%s_%s.jsonl"%(first,last), mode='w') as writer:
    for i in new:
      writer.write(i)
  
if __name__ == '__main__':

  if len(sys.argv) < 3:
    print (" need data range <first> <last> ")
    sys.exit(1)
  n = 1000000
  if len(sys.argv) >= 4:
    n = int(sys.argv[3])
  test(sys.argv[1],sys.argv[2],n)
