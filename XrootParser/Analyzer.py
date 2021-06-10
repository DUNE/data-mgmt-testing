#!/usr/local/opt/python/libexec/bin/python
# works with python2
import os,sys,csv,string,json,datetime,dateutil
import math
DUNEPRO=False  # only dunepro
xroot = True  # only xrootd urls
# loop over a range of input json files and histogram data flow vs various characteristics
# inputs are start and end dates, requires that summary files for those inputs be made by Utils.py
import ROOT
import numpy as np
from datetime import date,datetime,timedelta
from dateutil import parser

def countrify(loc):
  country = loc.split(".")[-1]
  if country == "edu" or country == "gov":
    country = "us"
  return country+"_"+loc

def getListOfTypes(data,key,inlist):
  l = list(inlist.keys())
  
  for item in data:
    if not key in item:
      continue
    val = item[key]#.decode('UTF-8')
    if key == "site":
      val = countrify(val)
    
    if val in l:
      continue
    
    l = np.append(l,val)
  sl = sorted(l)
  m = {}
  for i in range(0,len(l)):
    m[sl[i]] = i+1
    
  return m

def getListOfDates(data,inlist):
  key = "@timestamp"
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
  
  while start_range < end_date:
    end_range = start_range + delta
    inputfilename = "summary_%s_%s.json"%(start_range,end_range)
    if not os.path.exists(inputfilename):
      start_range += delta
      continue
    inputfile = open(inputfilename,'r')
    start_range += delta
    data = json.load(inputfile)
    apps = getListOfTypes(data,"application",apps)
    sites = getListOfTypes(data,"site",sites)
    states = getListOfTypes(data,"last_file_state",states)
    disks = getListOfTypes(data,"file_location",disks)
    users = getListOfTypes(data,"username",users)
    dates = getListOfDates(data,dates)
    
    if DUNEPRO:
      users = {"dunepro":1}
    inputfile.close()
  print (dates)
  firstday = start_date
  lastday = end_date
  if (not DUNEPRO):
    out_name = "user_%s_%s"%(firstday,lastday)
  else:
    out_name = "dunepro_%s_%s"%(firstday,lastday)
  if not xroot:
       out_name = out_name + "_notxroot"
  print (sites)
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
  nstate = len(states)
  ROOT.gStyle.SetOptStat(0)
  
  cross = ROOT.TH2F("cross","transfers",nd,0,nd,ns,0,ns)
  setXYLabels(cross,disks,sites)
  state = ROOT.TH2F("state","transfers",nstate,0,nstate,ns,0,ns)
  setXYLabels(state,states,sites)
  consumed = ROOT.TH2F("consumed","consumed",nd,0,nd,ns,0,ns)
  setXYLabels(consumed,disks,sites)
  consumed_by_app = ROOT.TH2F("consumed_by_app","consumed",na,0,na,ns,0,ns)
  setXYLabels(consumed_by_app,apps,sites)
  rate = ROOT.TH2F("rate","rate for consumed, MB/s",nd,0,nd,ns,0,ns)
  setXYLabels(rate,disks,sites)
  rate_by_app = ROOT.TH2F("rate_by_app","rate for consumed, by app, MB/s",na,0,na,ns,0,ns)
  setXYLabels(rate_by_app,apps,sites)
  ratelog10 = ROOT.TH2F("rates","rate for consumed, MB/s;;Log10 Rate",24,-3,3.,ns,0,ns)
  setYLabels(ratelog10,sites)
  skipped = ROOT.TH2F("skipped","skipped",nd,0,nd,ns,0,ns)
  setXYLabels(skipped,disks,sites)
  totalbytes = ROOT.TH1F("totalbytes_size","GB",ns,0,ns )
  setXLabels(totalbytes,sites)
  totalbytes_user = ROOT.TH1F("totalbytes_user","consumed GB",nu,0,nu )
  setXLabels(totalbytes_user,users)
  totalbytes_user_failed = ROOT.TH1F("totalbytes_user_failed","skipped GB",nu,0,nu )
  setXLabels(totalbytes_user_failed,users)
  totalbytes_date = ROOT.TH1F("totalbytes_date","GB/day",ndt,0,ndt )
  setXLabels(totalbytes_date,dates)
#  setXLabels(cross,sites)
#  setXLabels(consumed,sites)
#  setXLabels(skipped,sites)
#  setXLabels(rate,sites)
#  setYLabels(cross,disks)
#  setYLabels(consumed,disks)
#  setYLabels(skipped,disks)
#  setYLabels(rate,disks)
#  setXLabels(totalbytes,sites)
  cross.GetXaxis().LabelsOption("v");
  cross.GetYaxis().LabelsOption("v");
  
  summary = []
  start_range = start_date
  days = 0.0
  count = 0
  while start_range < end_date:
    end_range = start_range + delta
    
    inputfilename = "summary_%s_%s.json"%(start_range,end_range)
    
    if not os.path.exists(inputfilename):
      start_range += delta
      continue
    inputfile = open(inputfilename,'r')
    print ("read ",inputfilename)
    days += 1.0
    start_range += delta
    data = json.load(inputfile)
    for item in data:
      sumrec={}
      
      if not xroot and "root:" in item["file_url"]:
         
        continue
      if xroot and "http:" in item["file_url"]:
        continue
      
      if "site" not in item or "file_location" not in item:
        #print("missing: ",item["site"])
        continue
      
      
      site = countrify(item["site"])
      if "duration" not in item:
        continue
      if "rate" not in item:
        continue
      #rate = item["file_size"]/item["duration"]/1000000.
      #print ("got here")
      finalstate = item["last_file_state"]
      application = "unknown"
      if "application" in item:
        application = item["application"]
      
      isite = float(sites[site])-.5
      disk = item["file_location"]
      idisk = float(disks[disk])-.5
      user = item["username"]
      iuser = float(users[user])-.5
      date = item["@timestamp"][0:10]
      idate = float(dates[date])-.5
      istate = float(states[finalstate]) -.5
      iapp = float(apps[application]) -.5
      #print (type(isite),type(idisk))
      #print ("item",site,isite,disk,idisk)
      
      state.Fill(istate,isite,1.0)
      duration = item["duration"]
      sumrec["disk"] = disk
      sumrec["user"] = user
      sumrec["date"] = date
      process_id = 0
      if "process_id" in item:
        sumrec["process_id"] = item["process_id"]
      sumrec["timestamp"] = item["@timestamp"]
      sumrec["duration"] = duration
      sumrec["file_size"] = item["file_size"]
      sumrec["username"] = user
      sumrec["application"] = application
      sumrec["final_state"] = finalstate
      sumrec["site"] = site
      sumrec["rate"] = item["rate"]
      sumrec["project_name"] = item["project_name"]
      sumrec["file_name"] = os.path.basename(item["file_url"])
      sumrec["data_tier"] = item["data_tier"]
      sumrec["node"] = item["node"]
      sumrec["country"] = site[0:2]
      if "fnal" in site:
        sumrec["country"]="fnal"
      if "cern" in site:
        sumrec["country"]="cern"
      if "campaign" in item:
        sumrec["campaign"] = item["campaign"]
      else:
        sumrec["campaign"] = None
      #print (sumrec)
      summary.append(sumrec)
      if item["username"] == "dunepro" and not DUNEPRO :
        continue
      if not item["username"] == "dunepro" and DUNEPRO :
        continue
      if duration < 10:
        continue
      if finalstate not in ["consumed","skipped"]:
        continue
      count += 1
      cross.Fill(idisk,isite,1.0)
      totalbytes.Fill(isite,item["file_size"]*0.000000001)
      
      totalbytes_date.Fill(idate,item["file_size"]*0.000000001)
      if(item["last_file_state"] == "consumed"):
       
          consumed.Fill(idisk,isite,1.0)
          consumed_by_app.Fill(iapp,isite,1.0)
          totalbytes_user.Fill(iuser, item["file_size"]*0.000000001)
        
          rate.Fill(idisk,isite,item["rate"])
          rate_by_app.Fill(iapp,isite,item["rate"])
          ratelog10.Fill(math.log10(item["rate"]),isite,1.)
      if(item["last_file_state"] == "skipped" or item["last_file_state"] == "transferred"):
        skipped.Fill(idisk,isite,1.0)
        totalbytes_user_failed.Fill(iuser, item["file_size"]*0.000000001)
    data = None
  
  if len(summary) <= 0:
    print ( " nothing going")
    sys.exit(1)
    
  header = summary[0].keys()
   
  try:
      with open(out_name+".csv", 'w') as csvfile:
          writer = csv.DictWriter(csvfile, fieldnames=header)
          writer.writeheader()
          for data in summary:
              writer.writerow(data)
  except IOError:
      print("I/O error")
  
 # cross.Scale(1./days)
 # state.Scale(1./days)
 # totalbytes.Scale(1./days)
 # totalbytes_user.Scale(1./days)
 # totalbytes_user_failed.Scale(1./days)
  c = ROOT.TCanvas()
  c.SetLeftMargin(0.2)
  c.SetBottomMargin(0.2)
  c.SetRightMargin(0.15)
  #cross.SetMinimum(1)
  c.SetLogz()
  ROOT.gStyle.SetPaintTextFormat("5.2f")
  #cross.Print("ALL")
  
  
  cross.Draw("COLZ")

  cross.Draw("TEXT SAME")
  c.Print(out_name+"_traffic.png")
  state.Draw("COLZ")
  state.Draw("TEXT SAME")
  c.Print(out_name+"_states.png")
  
  
  c.SetLogz(0)
  c.SetLogy(1)
  totalbytes.SetMinimum(1.)
  totalbytes.Draw("")
  c.Print(out_name+"_totalbytes_site.png")
  leg = ROOT.TLegend(0.75,0.75,0.85,0.87)
  leg.SetBorderSize(0)
  
  
  totalbytes_user.SetMinimum(1.)
  totalbytes_user.SetMarkerColor(ROOT.kBlue)
  totalbytes_user.SetLineColor(ROOT.kBlue)
  entry2 = leg.AddEntry(totalbytes_user,"Consumed","f")
  totalbytes_user.Draw("PE")
  totalbytes_user_failed.SetMarkerColor(ROOT.kRed)
  totalbytes_user_failed.SetLineColor(ROOT.kRed)
  totalbytes_user_failed.Draw("SAME")
  entry3 = leg.AddEntry(totalbytes_user_failed,"Skipped","f")
  leg.SetFillColor(0)
  leg.Draw("same")
  
  c.Print(out_name+"_totalbytes_user.png")
  
  
  totalbytes_date.Draw()
  c.Print(out_name+"_totalbytes_date.png")
  c.SetLogy(0)
  total = consumed.Clone("total")
  total.Add(skipped)
  efficiency = consumed.Clone("efficiency ")
  efficiency.SetTitle("success rate "+out_name)
  efficiency.Divide(total)
  efficiency.Draw("COLZ NUM")
  ROOT.gStyle.SetPaintTextFormat("5.2f")
  efficiency.Draw("TEXT45 SAME")
  c.Print(out_name+"_efficiency.png")
  c.SetLogz(1)
  rate.SetMaximum(100.)
  rate.Divide(consumed)
  rate.SetTitle(rate.GetTitle()+" " + out_name)
  rate.Draw("COLZ")
  ROOT.gStyle.SetPaintTextFormat("5.2f")
  rate.Draw("TEXT SAME")
  c.Print(out_name+"_rate.png")
  rate_by_app.SetMaximum(100.)
  rate_by_app.Divide(consumed_by_app)
  rate_by_app.SetTitle(rate_by_app.GetTitle()+" " + out_name)
  rate_by_app.Draw("COLZ")
  ROOT.gStyle.SetPaintTextFormat("5.2f")
  rate_by_app.Draw("TEXT45 SAME")
  c.Print(out_name+"_rate_by_app.png")
  
  c.SetLogz(0)
  ratelog10.Draw("BOX")
  c.Print(out_name+"_ratelog10.png")
  print ("total count is ",count)


if __name__ == '__main__':

   
 
  start_date = date(2021,1 , 1)
  end_date = date(2021, 5, 31)
  delta = timedelta(days=1)
  analyze(start_date,end_date,delta)
