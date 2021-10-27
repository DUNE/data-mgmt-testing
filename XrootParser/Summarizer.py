#!/usr/local/opt/python/libexec/bin/python
# works with python2
import os,sys,csv,string,json,datetime,dateutil,jsonlines
import math

from datetime import date,datetime,timedelta
from dateutil import parser

DEBUG=True

#xroot = True  # only xrootd urls
# loop over a range of input jsonl summary files and output summaries

# inputs are start and end dates, requires that summary files for those inputs be made by Utils.py

# truncate long floats
def truncate(f):
  return round(f*1000)/1000.
 
# translate site names into CRIC names
def translate_site(name):
  f = open("SiteNames.json",'r')
  nodename_sitename =json.load(f)
  f.close()
  if name in nodename_sitename:
    return nodename_sitename[name]
  else:
    return "UNKNOW_"+name

def countrify(loc):
  country = loc.split(".")[-1]
  if country == "edu" or country == "gov":
    country = "us"
  return country+"_"+loc

# this reads in the summaries and reformats in a shorter version for plotting/analysis

def summarizeRecord(item):
      sumrec={}
      if "site" not in item or "file_location" not in item:
        #print("missing: ",item["node"])
        return None
      site = countrify(item["site"])
      if "rate" not in item:
        return None
      finalstate = item["last_file_state"]
      application = "unknown"
      if "application" in item:
        application = item["application"]
      disk = item["file_location"]
      user = item["username"]
      date = item["@timestamp"][0:10]
      duration = truncate(item["duration"])
      sumrec["source"] = translate_site(disk)
      sumrec["user"] = user
      sumrec["date"] = date
      process_id = 0
      if "process_id" in item:
        sumrec["process_id"] = item["process_id"]
      sumrec["start_time"] = item["@timestamp"]
      sumrec["file_transfer_time"] = duration
      sumrec["file_size"] = item["file_size"]
      sumrec["username"] = user
      sumrec["application"] = application
      sumrec["final_state"] = finalstate
      sumrec["destination"] = translate_site(site)
      sumrec['transfer_speed(MB/s)'] = truncate(item["rate"])
      sumrec['transfer_speed(B/s)'] = truncate(item["rate"]*1000000)
      sumrec["project_name"] = item["project_name"]
      #sumrec["file_name"] = os.path.basename(item["file_url"])
      sumrec["name"] = os.path.basename(item["file_url"])
      sumrec["data_tier"] = item["data_tier"]
      sumrec["node"] = item["node"]
      return sumrec
       

def summarize(start_date,end_date,delta ):

  start_range = start_date

  out_name = "dune_%s_%s"%(start_date.strftime("%Y-%m-%d"),end_date.strftime("%Y-%m-%d"))
  out_name= "data/" + out_name

  data = []
  while start_range < end_date:
    end_range = start_range + delta
    inputfilename = "data/dune_summary_%s_%s.jsonl"%(start_range.strftime("%Y-%m-%d"),end_range.strftime("%Y-%m-%d"))
    print(inputfilename)
    if not os.path.exists(inputfilename):
      print ("No such file a ",inputfilename)
      start_range += delta
      continue
    
    with jsonlines.open(inputfilename, mode='r') as reader:
      for obj in reader:
        data.append(obj)
    start_range += delta

 
  summary = []
  
  
  start_range = start_date
  days = 1.0
  count = 0
  while start_range < end_date:
    end_range = start_range + delta

    inputfilename = "summary_%s_%s.jsonl"%(start_range,end_range)

    if not os.path.exists(inputfilename):
      start_range += delta
      continue

    days += 1.0
    start_range += delta
    
  for item in data:

      sumrec = summarizeRecord(item)
      
      if sumrec != None:
        summary.append(sumrec)

  data = None
  
  header = summary[0].keys()
   
  try:
      with open(out_name+".csv", 'w') as csvfile:
          writer = csv.DictWriter(csvfile, fieldnames=header)
          writer.writeheader()
          for data in summary:
              writer.writerow(data)
  except IOError:
      print("csv I/O error")
  try:
    with jsonlines.open("%s.jsonl"%(out_name), mode='w') as writer:
      for i in summary:
        writer.write(i)
        
  except IOError:
      print("jsonl I/O error")
  
  try:
      g = open("%s.json"%(out_name),'w')

      json.dump(summary,g)
        
  except IOError:
      print("json I/O error")

if __name__ == '__main__':

   
 
  start_date = datetime.strptime(sys.argv[1], "%Y-%m-%d")
  start_range = start_date
  delta = timedelta(days=1)
  end_date =datetime.strptime(sys.argv[2], "%Y-%m-%d") if len(sys.argv)>=3 else start_date+delta
  summarize(start_date,end_date,delta)
  
