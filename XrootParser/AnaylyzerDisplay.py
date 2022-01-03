#!/usr/local/opt/python/libexec/bin/python
# works with python2
import os,sys,csv,string,json,datetime,dateutil,jsonlines
import math
DEBUG=True
DUNEPRO=False  # only dunepro
DISPLAY=True
expt="dune"
#xroot = True  # only xrootd urls
# loop over a range of input json files and histogram data flow vs various characteristics
# inputs are start and end dates, requires that summary files for those inputs be made by Utils.py

# translate nodenames - this should be a json file.
def truncate(f):
  return round(f*1000)/1000.
def translate_site(name):
  f = open("../XRootESClient/SiteNames.json",'r')
  nodename_sitename=json.load(f)
  
  if name in nodename_sitename:
    return nodename_sitename[name]
  else:
    return name

import numpy as np
from datetime import date,datetime,timedelta
from dateutil import parser

def countrify(loc):
  country = loc.split(".")[-1]
  if country == "edu" or country == "gov":
    country = "us"
  return country+"_"+loc
  
def country(loc):
  country = loc.split(".")[-1]
  if "fnal" in loc:
    return "fnal"
  if "jinr" in loc:
    return "ru"
  if "wn" in loc and "hep" in loc:
    return  "uk"
  if country == "edu" or country == "gov":
    country = "us"
  if "dice" in loc:
    country = "uk"
  if "sandy" in loc or "local" in loc:
    country = "pr"
  if "cern" in loc:
    return "cern"
  if "arbutus" in loc:
    return "ca"
  if "qmul" in loc:
    return "uk"

  return country.lower()

def getListOfTypes(data,key,inlist):
  l = list(inlist.keys())
  print ("length", len(data))
  for item in data:
  
    if not key in item:
      continue
    val = item[key]#.decode('UTF-8')
    #if key == "destination":
    #  val = countrify(val)
    
    if val in l:
      continue
    
    l = np.append(l,val)
  sl = sorted(l)
  m = {}
  for i in range(0,len(l)):
    m[sl[i]] = i+1
  
  return m

def getListOfDates(data,inlist):
  key = "date"
  l = list(inlist.keys())
  for item in data:
    if not key in item:
      continue
    val = item[key][0:10]#.decode('UTF-8')
    if val in l:
      continue
    l = np.append(l,val)
  sl  =sorted(l)
  m = {}
  for i in range(0,len(l)):
    m[sl[i]] = i+1
  
  return m
  
def setYLabels(h,labels):
  for bin in labels:
    h.GetYaxis().SetBinLabel(labels[bin], bin);

def setXLabels(h,labels):
  for bin in labels:
    h.GetXaxis().SetBinLabel(labels[bin], bin);
    if len(labels) > 6:
      h.GetXaxis().LabelsOption("v");
    
def setXYLabels(h,x,y):
  setXLabels(h,x)
  setYLabels(h,y)

def analyze(start_date,end_date,delta ):

# first get lists of variables

  sites = {}
  disks = {}
  users = {}
  dates = {}
  states = {}
  apps = {"unknown":0}
  start_range = start_date
  if (not DUNEPRO):
    out_name = "display_%s_%s"%(start_date.strftime("%Y-%m-%d"),end_date.strftime("%Y-%m-%d"))
  else:
    out_name = "dunepro_%s_%s"%(start_date.strftime("%Y-%m-%d"),end_date.strftime("%Y-%m-%d"))
  out_name= "data/" + out_name

#  if not xroot:
#       out_name = out_name + "_notxroot"
  data = []
  while start_range < end_date:
    
    end_range = start_range + delta
    print (end_range)
    year = start_range.strftime("%Y")
    month = start_range.strftime("%m")
    inputfilename = "../XRootESClient/cache/%s/%s/out_%s_%s_%s.json"%(year,month,expt,start_range.strftime("%Y-%m-%d"),end_range.strftime("%Y-%m-%d"))
    if not os.path.exists(inputfilename):
      print ("no input file",inputfilename)
      start_range += delta
      continue
    inputfile = open(inputfilename,'r')
    print ("input:",inputfilename)
    start_range += delta
    datain = json.load(inputfile)
    data = datain["data"]
    start_range += delta
  
    
    apps = getListOfTypes(data,"application",apps)
    sites = getListOfTypes(data,"destination",sites)
    states = getListOfTypes(data,"final_state",states)
    disks = getListOfTypes(data,"source",disks)
    users = getListOfTypes(data,"user",users)
    dates = getListOfDates(data,dates)
    if DUNEPRO:
      users = {"dunepro":1}
      inputfile.close()
  print ("sites",sites)
  #sites = sorted(sites,reverse=True)
  print (sites)
  ns = len(sites)
  nd = len(disks)
  nu = len(users)
  ndt = len(dates)
  na = len(apps)
  print ("sites",sites)
  print ("disks",disks)
  print ("dates",dates)
  print ("users",users)
  print ("states",states)
  print ("apps",apps)
  nstate = len(states)
   
  summary = []
  start_range = start_date
  days = 1.0
  count = 0
#  while start_range < end_date:
#    end_range = start_range + delta
#
#    inputfilename = "out_dune_%s_%s.json"%(start_range,end_range)
#
#    if not os.path.exists(inputfilename):
#      start_range += delta
#      continue
#
#    days += 1.0
#    start_range += delta
    
  for item in data:
      sumrec={}
#      if not xroot and "root:" in item["file_url"]:
#        continue
#      if xroot and "http:" in item["file_url"]:
#        continue

#      if "site" not in item or "file_location" not in item:
#        #print("missing: ",item["node"])
#        continue
#
      
      site = item["destination"]
      #if "rate" not in item:
      #  continue
      finalstate = item["final_state"]
      application = "unknown"
      if "application" in item:
        application = item["application"]
      
#    isite = float(sites[site])-.5
      disk = item["source"]
#      idisk = float(disks[disk])-.5
      user = item["username"]
#      iuser = float(users[user])-.5
      date = item["date"][0:10]
#      idate = float(dates[date])-.5
#      istate = float(states[finalstate]) -.5
#      iapp = float(apps[application]) -.5

      duration = truncate(item["file_transfer_time"])
#   zac_dict['name'] = row['file_name']
#source=row['disk']
#newsource=nodename_sitename[source]
#zac_dict['source']=newsource
#destination=row['site']
#newdestination=nodename_sitename[destination]
#zac_dict['destination']=newdestination
#zac_dict['file_size'] = row['file_size']
#zac_dict['start_time'] = row['timestamp']
#zac_dict['file_transfer_time']=row['duration']
#zac_dict['transfer_speed(MB/s)']=row['rate']
#Brate=int(float(row['rate'])*1000000)
#zac_dict['transfer_speed(B/s)']=Brate

      sumrec["source"] = translate_site(disk)
      sumrec["user"] = user
      sumrec["date"] = date
      process_id = 0
      if "process_id" in item:
        sumrec["process_id"] = item["process_id"]
      sumrec["start_time"] = item["start_time"]
      sumrec["file_transfer_time"] = duration
      sumrec["file_size"] = item["file_size"]
      sumrec["username"] = user
      sumrec["application"] = application
      sumrec["final_state"] = finalstate
      sumrec["destination"] = translate_site(site)
      sumrec["country"] = country(item["node"])
      sumrec['transfer_speed_MBpers'] = truncate(item["transfer_speed(MB/s)"])
      sumrec['transfer_speed_Bpers)'] = truncate(item["transfer_speed(MB/s)"]*1000000)
      sumrec["project_name"] = item["project_name"]
      #sumrec["file_name"] = os.path.basename(item["file_url"])
      #sumrec["name"] = os.path.basename(item["file_url"])
      sumrec["data_tier"] = item["data_tier"]
      sumrec["node"] = item["node"]
      summary.append(sumrec)

  data = None
  
  header = summary[0].keys()
  out_name="analysis_%s_%s_%s"%(expt,start_date.strftime("%Y-%m-%d"),end_date.strftime("%Y-%m-%d"))
  print ("out_name",out_name)
  print ("header",header)
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
    
  
#  cross.Scale(1./days)
 # state.Scale(1./days)
#  totalbytes.Scale(1./days)
#  totalbytes_user.Scale(1./days)
#  totalbytes_user_failed.Scale(1./days)
#  c = ROOT.TCanvas()
#  c.SetLeftMargin(0.2)
#  c.SetBottomMargin(0.2)
#  c.SetRightMargin(0.15)
  #cross.SetMinimum(1)
#  c.SetLogz()
#  ROOT.gStyle.SetPaintTextFormat("5.2f")
  #cross.Print("ALL")
  
  
 # cross.Draw("COLZ")

#  cross.Draw("TEXT SAME")
#  c.Print(out_name+"_traffic.png")
#  state.Draw("COLZ")
#  state.Draw("TEXT SAME")
#  c.Print(out_name+"_states.png")
  
  
#  c.SetLogz(0)
#  c.SetLogy(1)
#  totalbytes.SetMinimum(1.)
#  totalbytes.Draw("")
#  c.Print(out_name+"_totalbytes_site.png")
#  leg = ROOT.TLegend(0.75,0.75,0.85,0.87)
#  leg.SetBorderSize(0)
  
  
#  totalbytes_user.SetMinimum(1.)
#  totalbytes_user.SetMarkerColor(ROOT.kBlue)
#  totalbytes_user.SetLineColor(ROOT.kBlue)
#  entry2 = leg.AddEntry(totalbytes_user,"Consumed","f")
#  totalbytes_user.Draw("PE")
#  totalbytes_user_failed.SetMarkerColor(ROOT.kRed)
#  totalbytes_user_failed.SetLineColor(ROOT.kRed)
#  totalbytes_user_failed.Draw("SAME")
#  entry3 = leg.AddEntry(totalbytes_user_failed,"Skipped","f")
#  leg.SetFillColor(0)
#  leg.Draw("same")
  
#  c.Print(out_name+"_totalbytes_user.png")
  
  
#  totalbytes_date.Draw()
#  c.Print(out_name+"_totalbytes_date.png")
#  c.SetLogy(0)
#  total = consumed.Clone("total")
#  total.Add(skipped)
#  efficiency = consumed.Clone("efficiency ")
#  efficiency.SetTitle("success rate "+out_name)
#  efficiency.Divide(total)
#  efficiency.Draw("COLZ NUM")
#  ROOT.gStyle.SetPaintTextFormat("5.2f")
#  efficiency.Draw("TEXT SAME")
#  c.Print(out_name+"_efficiency.png")
#  c.SetLogz(1)
#  rate.SetMaximum(100.)
#  rate.Divide(consumed)
#  rate.SetTitle(rate.GetTitle()+" " + out_name)
#  rate.Draw("COLZ")
#  ROOT.gStyle.SetPaintTextFormat("5.2f")
#  rate.Draw("TEXT SAME")
#  c.Print(out_name+"_rate.png")
#  rate_by_app.SetMaximum(100.)
#  rate_by_app.Divide(consumed_by_app)
#  rate_by_app.SetTitle(rate_by_app.GetTitle()+" " + out_name)
#  rate_by_app.Draw("COLZ")
#  ROOT.gStyle.SetPaintTextFormat("5.2f")
 # rate_by_app.Draw("TEXT SAME")
#  c.Print(out_name+"_rate_by_app.png")
  
#  c.SetLogz(0)
#  ratelog10.Draw("BOX")
#  c.Print(out_name+"_ratelog10.png")
#  print ("total count is ",count)


if __name__ == '__main__':

   
 
  start_date = datetime.strptime(sys.argv[1], "%Y-%m-%d")
  start_range = start_date
  delta = timedelta(days=1)
  end_date =datetime.strptime(sys.argv[2], "%Y-%m-%d")
  analyze(start_date,end_date,delta)
  
