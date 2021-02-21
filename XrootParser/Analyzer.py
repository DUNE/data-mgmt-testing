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

def getListOfTypes(data,key,inlist):
  l = list(inlist.keys())
  
  for item in data:
    if not key in item:
      continue
    val = item[key].decode('UTF-8')
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
    val = item[key][0:10].decode('UTF-8')
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
  start_range = start_date
  if (not DUNEPRO):
    out_name = "user_%s_%s"%(start_date,end_date)
  else:
    out_name = "dunepro_%s_%s"%(start_date,end_date)
  if not xroot:
       out_name = out_name + "_notxroot"
  while start_range < end_date:
    end_range = start_range + delta
    inputfilename = "summary_%s_%s.json"%(start_range,end_range)
    if not os.path.exists(inputfilename):
      start_range += delta
      continue
    inputfile = open(inputfilename,'r')
    start_range += delta
    data = json.load(inputfile)
    sites = getListOfTypes(data,"site",sites)
    states = getListOfTypes(data,"last_file_state",states)
    disks = getListOfTypes(data,"file_location",disks)
    users = getListOfTypes(data,"username",users)
    dates = getListOfDates(data,dates)
    if DUNEPRO:
      users = {"dunepro":1}
    inputfile.close()
  
  ns = len(sites)
  nd = len(disks)
  nu = len(users)
  ndt = len(dates)
  print ("sites",sites)
  print ("disks",disks)
  print ("dates",dates)
  print ("users",users)
  print ("states",states)
  nstate = len(states)
  ROOT.gStyle.SetOptStat(0)
  cross = ROOT.TH2F("cross","transfer/day",ns,0,ns,nd,0,nd)
  setXYLabels(cross,sites,disks)
  state = ROOT.TH2F("state","transfer/day",ns,0,ns,nstate,0,nstate)
  setXYLabels(state,sites,states)
  consumed = ROOT.TH2F("consumed","consumed",ns,0,ns,nd,0,nd)
  setXYLabels(consumed,sites,disks)
  rate = ROOT.TH2F("rate","rate for consumed, MB/s",ns,0,ns,nd,0,nd)
  setXYLabels(rate,sites,disks)
  ratelog10 = ROOT.TH2F("rates","rate for consumed, MB/s;;Log10 Rate",ns,0,ns,24,-3,3.)
  setXLabels(ratelog10,sites)
  skipped = ROOT.TH2F("skipped","skipped",ns,0,ns,nd,0,nd)
  setXYLabels(skipped,sites,disks)
  totalbytes = ROOT.TH1F("totalbytes_size","GB/day",ns,0,ns )
  setXLabels(totalbytes,sites)
  totalbytes_user = ROOT.TH1F("totalbytes_user","GB/day",nu,0,nu )
  setXLabels(totalbytes_user,users)
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
  start_range = start_date
  days = 0.0
  while start_range < end_date:
    end_range = start_range + delta
    
    inputfilename = "summary_%s_%s.json"%(start_range,end_range)
   
    if not os.path.exists(inputfilename):
      start_range += delta
      continue
    inputfile = open(inputfilename,'r')
    days += 1.0
    start_range += delta
    data = json.load(inputfile)
    for item in data:
      if not xroot and "root:" in item["file_url"]:
        continue
      if xroot and "http:" in item["file_url"]:
        continue
      if item["username"] == "dunepro" and not DUNEPRO :
        continue
      if not item["username"] == "dunepro" and DUNEPRO :
        continue
      if "site" not in item or "file_location" not in item:
        #print("missing: ",item["node"])
        continue
      site = item["site"]
      if "rate" not in item:
        continue
      isite = float(sites[site])-.5
      disk = item["file_location"]
      idisk = float(disks[disk])-.5
      user = item["username"]
      iuser = float(users[user])-.5
      date = item["@timestamp"][0:10]
      idate = float(dates[date])-.5
      istate = float(states[item["last_file_state"]]) -.5
      #print (type(isite),type(idisk))
      #print ("item",site,isite,disk,idisk)
      cross.Fill(isite,idisk,1.0)
      state.Fill(isite,istate,1.0)
      totalbytes.Fill(isite,item["file_size"]*0.000000001)
      totalbytes_user.Fill(iuser,item["file_size"]*0.000000001)
      totalbytes_date.Fill(idate,item["file_size"]*0.000000001)
      if(item["last_file_state"] == "consumed"):
        consumed.Fill(isite,idisk,1.0)
       
        rate.Fill(isite,idisk,item["rate"])
        ratelog10.Fill(isite,math.log10(item["rate"]),1.)
      if(item["last_file_state"] == "skipped"):
        skipped.Fill(isite,idisk,1.0)
    data = None
  
  cross.Scale(1./days)
  state.Scale(1./days)
  totalbytes.Scale(1./days)
  totalbytes_user.Scale(1./days)
  c = ROOT.TCanvas()
  c.SetLeftMargin(0.2)
  c.SetBottomMargin(0.2)
  c.SetRightMargin(0.15)
  #cross.SetMinimum(1)
  c.SetLogz()
  ROOT.gStyle.SetPaintTextFormat("5.2f")
  #cross.Print("ALL")
  cross.Draw("COLZ")
  c.Print(out_name+"_traffic.png")
  state.Draw("COLZ")
  state.Draw("TEXT99 SAME")
  c.Print(out_name+"_states.png")
  c.SetLogz(0)
  c.SetLogy(1)
  totalbytes.SetMinimum(1.)
  totalbytes_user.SetMinimum(1.)
  totalbytes.Draw()
  c.Print(out_name+"_totalbytes_site.png")
  totalbytes_user.Draw()
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
  efficiency.Draw("TEXT99 SAME")
  c.Print(out_name+"_efficiency.png")
  c.SetLogz(1)
  rate.SetMaximum(100.)
  rate.Divide(consumed)
  rate.SetTitle(rate.GetTitle()+" " + out_name)
  rate.Draw("COLZ")
  ROOT.gStyle.SetPaintTextFormat("5.2f")
  rate.Draw("TEXT99 SAME")
  c.Print(out_name+"_rate.png")
  c.SetLogz(0)
  ratelog10.Draw("BOX")
  c.Print(out_name+"_ratelog10.png")
  


if __name__ == '__main__':

   
 
  start_date = date(2021, 1, 1)
  end_date = date(2021, 1, 31)
  delta = timedelta(days=1)
  analyze(start_date,end_date,delta)
