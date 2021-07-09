#!/usr/local/opt/python/libexec/bin/python
import os,sys,csv,string,json,datetime,dateutil,jsonlines
import requests

DEBUG=True
from datetime import date,timezone,datetime
from dateutil import parser
expt = "minerva"

import samweb_client
samweb = samweb_client.SAMWebClient(experiment=expt)

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
  
  knownsites = [".gov",".fr",".es",".br",".edu",".in",".cz",".uk",".fr",".ch",".nl",".ru"]
  specials = {"dice":"dice.bristol.uk","CRUSH":"crush.syracuse.edu","gridpp.rl.ac.uk":"gridpp.rl.ac.uk","comp20-":"lancaster.uk","discovery":"nmsu.edu","nid":"nersc.lbnl.gov","wn-2":"unibe-lhcp.bern.ch","qmul":"esc.qmul.uk","uct2":"uct2-mwt2.uchicago.edu","nubes.stfc.ac.uk":"nubes.stfc.ac.uk","unl.edu":"unl.edu","wn0":"sheffield.uk","wn1":"sheffield.uk","wn2":"sheffield.uk","local-":"pr"}
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
  
# merge the elasticsearch and project info and remove events/fields we don't need

def Cleaner(info,projectmeta):
  if DEBUG:
    print ("Start Cleaner ", projectmeta)
  drops = ["type","station","@version","kafka"]
  
  dropevents = ["start_cache_check","end_cache_check","start_stage_file","end_stage_file","file_staged","update_process_state","end_process","handle_storage_system_error","delivered"]
  drops += ["files in snapshot", "first_name","group_id","group_name","last_name","person_id","processes","project_desc","station_id","experiment","file_name","event_time"]
 
  #print ('info',info)
  clean = []
  project_id = projectmeta["project_id"]
  if not "hits" in info:
      print ("no hits in layer0")
      return {}
    
  layer1 = info["hits"]
  layer2 = layer1["hits"]
  if not "hits" in layer1:  # minerva data fails this
    print ("no hits in this layer")
    return {}
  count = 0
  print ("got the data",len(layer2))
  for item in layer2:
    count += 1
    source = item["_source"]
    #if DEBUG:
      #print ("an event", source["event"])
    if source["event"] in dropevents:
      #if DEBUG:
      #  print ("drop event",source["event"])
      continue
    
    if source["project_id"] != project_id:
      if DEBUG:
        print (" got the wrong project? ",project_id, source["project_id"],source["@timestamp"])
        print (source)
      continue
      
    source["timestamp"] = human2number(source["@timestamp"])
    
    if not "file_id" in source:
      if DEBUG:
        print ("No file_id")
      continue
      
#      version 3 move to dropevents?
#    if "file_state" in source and source["file_state"] == "delivered":
#      #print ("drop delivered")
#      continue
      
    if DEBUG:
      print ("got here",len(source))
      
    source = fileFinder(source)
    source = siteFinder(source)
    #print ("got here",len(source))
    # add project metadata
    for meta in projectmeta:
      source[meta] = projectmeta[meta]
      if DEBUG: print ("got here with meta",len(source))
    # clean out stuff we don't need
    for a in drops:
      if a in source:
        source.pop(a)
    if DEBUG:
      print ("after clean",len(source))
    if( count < 2 or "eos" in source["file_location"]):
      jsonprint (source)
    clean.append(source)
    if DEBUG:
      print (" size of clean record ",len(clean))
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
def getProjectInfo(projectID,datestring):
  #print (datestring[0:4],datestring[5:7])
  urltemplate = "https://fifemon-es.fnal.gov/sam-events-v1-%4s.%2s/_search?q=experiment:%s%%20and%%20project_id:%s&size=10000"%(datestring[0:4],datestring[5:7],expt,projectID)
  theurl = urltemplate
  print (theurl)
  try:
    result = requests.get(theurl)
  except:
    print("request failed - maybe you need the VPN")
    result={}
  if "Forbidden" in result.text:
    print("request failed - maybe you need the VPN")
    result={}
  print ("size of result ", len(result.text))
  if DEBUG:
    print ("result", result.text)
  return json.loads(result.text)

# get list of sam project ID's to send to GetProjectInfo

def getProjectList(begin,end,n=10):
  cleaned = []
  names = []
  projects = samweb.listProjects(started_after=begin,started_before=end)
  c = 0
  for i in range(0,len(projects)):
    p = projects[i].decode('UTF-8')
    if "prestage" in p:
      continue
    md = samweb.projectSummary(p)
    pid = md["project_id"]
    if c < n:
      names.append(p)
      cleaned.append(pid)
    # limit the list
    c += 1
    #print ("cleaned",cleaned[0:min(len(cleaned),n)])
  return [cleaned[0:min(len(cleaned),n)],names[0:min(len(cleaned),n)]]

def getProjectMeta(pname):
  md = samweb.projectSummary(pname)
  
  processes = md["processes"]
  
  brief = md
  brief["processes"] = None
  if len(processes) > 0:
    application = processes[0]["application"]["name"]
    version = processes[0]["application"]["version"]
    brief["version"]=version
    brief["application"]=application
  return brief

def findProjectInfo(projects,tag="date"):
  result = []
  for p in projects:
    m = getProjectMeta(p)
    #print ("project",p,m)
    if "prestage" in m["project_name"]:
      print ("skip prestage",p )
      continue
    print ("FindProject",m)
    id = m["project_id"]
    record =  Cleaner(getProjectInfo(id,m["project_start_time"]),m)
    print (" made a record",len(record))
    outname = "%s_raw_%s_%d.jsonl"%(expt,tag,id)
    with jsonlines.open(outname, mode='w') as writer:
      for i in record:
        writer.write(i)
    print ("wrote a record to ",outname)
    result += record
      
    #//summary = samweb.projectSummary(p)
    #print (summary)
    writer.close()
  return result
  
# remove records we don't need
  
def cleanRecord(record,uselist):
  newrecord = record
  for key in record:
    if key not in uselist:
      newrecord.pop(key)
  return newrecord
      
# build a map with timestamps sorted
#def buildMap(records):
## sort the records into buckets by project_id, file_id and timestamp
#  infomap = {}
#  sortedmap = {}
#  for record in records:
#    if type(record) != type({}):
#      print ("strange record",record)
#      continue
#    if "project_id" not in record or "file_id" not in record:
#      continue
#    pid = record["project_id"]
#    fid = record["file_id"]
#    t = record["timestamp"]
#    if type(pid) == type([]):
#      print ("strange type",pid,type(pid),type(1))
#      pid = pid[0]
#      record["project_id"] = pid
#      jsonprint (record)
#      continue
#
#    if pid not in infomap.keys():
#      infomap[pid] = {}
#      sortedmap[pid] = {}
#    if fid not in infomap[pid]:
#      infomap[pid][fid] = {}
#      sortedmap[pid][fid] = []
#    if t in infomap[pid][fid]:
#      print ("duplicate record",pid,fid,t)
#      continue
#    infomap[pid][fid][t] = record
## make a sorted list of times that share the file_size info
#  for p in infomap:
#      for f in infomap[p]:
#        times = infomap[p][f].keys()
#        # try to recover missing information from one of the records
#        md = samweb.getMetadata(f)
#        file_size = md["file_size"]
#
#        version = None
#        campaign = None
#        if "DUNE.campaign" in md:
#          campaign = md["DUNE.campaign"]
#
#        data_tier = md["data_tier"]
#
#        for t in times:
#          infomap[p][f][t]["file_size"] = file_size
#          infomap[p][f][t]["Campaign"] = campaign
#
#          infomap[p][f][t]["data_tier"] = data_tier
#        sortedtimes = sorted(times)
#        #print ("sorted times", times, sortedtimes)
#        for s in range(0,len(sortedtimes)):
##          if "file_size" not in infomap[p][f][sortedtimes[s]] and file_size != None:
##            infomap[p][f][sortedtimes[s]]["file_size"] = file_size
#          sortedmap[p][f].append ( infomap[p][f][sortedtimes[s]])
#
#  return sortedmap
      
# log first and last records and calculate duration

def sequence(firstdate,lastdate,ids):
  #print (ids)
  actions = []
  for pid in ids:
  # build a small map for each project
    record = {}
    print (pid)
    fname = "%s_raw_%s_%s_%d.jsonl"%(expt,firstdate,lastdate,pid)
    print ("fname",fname)
    with jsonlines.open(fname, mode='r') as reader:
      for obj in reader:
        fid = obj["file_id"]
        
        t = obj["timestamp"]
        if not fid in record:
          record[fid] = {}
        record[fid][t] = obj
    reader.close()
    
    for fid in record:
      times = record[fid].keys()
      sortedtimes = sorted(times)
      md = samweb.getMetadata(fid)
      file_size = md["file_size"]
      
      sum = {}
      
      f = 0
      first = record[fid][sortedtimes[0]]
      last = record[fid][sortedtimes[len(times)-1]]
      sum = first
      sum["file_size"] = file_size
      #print (sum["file_size"])
      if not "data_tier" in md:
        print (" no data-tier - this is strange ", md)
        continue
      sum["data_tier"] = md["data_tier"]
      campaign = None
      if "DUNE.campaign" in md:
        sum["campaign"] = md["DUNE.campaign"]
      sum["actions"] = len(record[fid])
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
  tag= first+"_"+last
  [pids,names] = getProjectList(first,last,n)
  print (pids)
#  print ("this many projects",len(pids))
#  if len(pids) > 2:
#    getProjectMeta(ids[1])
# first get the info
  print (names)

  info = findProjectInfo(names,tag)
#  e = open("raw_%s_%s.json"%(first,last),'w')
#  s = json.dumps(info, indent=2)
#  e.write(s)
#  with jsonlines.open("raw_%s_%s.jsonl"%(first,last), mode='w') as writer:
#    for i in info:
#      writer.write(i)
#  e.close
#  info = None
#
#  # then make it into a map
#  e = open("raw_%s_%s.json"%(first,last),'r')
#  info = json.load(e)
#  result = buildMap(info)
#  info = None

  
  # then use the time sorted info to record first and last actions for each file
  

  #info = result
  new = sequence(first,last,pids)
  g = open("%s_summary_%s_%s.json"%(expt,first,last),'w')
 
   
  s = json.dumps(new,indent=2)
  g.write(s)
  
  g.close()
  with jsonlines.open("%s_summary_%s_%s.jsonl"%(expt,first,last), mode='w') as writer:
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
