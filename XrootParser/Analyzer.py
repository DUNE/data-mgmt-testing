#!/usr/local/opt/python/libexec/bin/python
# works with python2
import os,sys,csv,string,json,datetime,dateutil

# loop over a range of input json files and histogram data flow vs various characteristics
# inputs are start and end dates, requires that summary files for those inputs be made by Utils.py
import ROOT
import numpy as np
from datetime import date,datetime,timedelta
from dateutil import parser

def getListOfTypes(data,key,inlist):
  l = inlist.keys()
  for item in data:
    if not key in item:
      continue
    val = item[key]
    if val in l:
      continue
    l = np.append(l,val)
  m = {}
  for i in range(0,len(l)):
    m[l[i]] = i+1
    
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
  start_range = start_date
  out_name = "%s_%s_"%(start_date,end_date)
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
    disks = getListOfTypes(data,"file_location",disks)
    users = getListOfTypes(data,"username",users)
    inputfile.close()
  
  ns = len(sites)
  nd = len(disks)
  print ("sites",sites)
  print ("disks",disks)
  ROOT.gStyle.SetOptStat(0)
  cross = ROOT.TH2F("cross","transfer",ns+1,0,ns,nd,0,nd)
  consumed = ROOT.TH2F("consumed","consumed",ns,0,ns,nd,0,nd)
  rate = ROOT.TH2F("rate","rate for consumed, MB/s",ns,0,ns,nd,0,nd)
  skipped = ROOT.TH2F("skipped","skipped",ns,0,ns,nd,0,nd)
  totalbytes = ROOT.TH1F("totalbytes","total GB",ns,0,ns )
  setXLabels(cross,sites)
  setXLabels(consumed,sites)
  setXLabels(skipped,sites)
  setXLabels(rate,sites)
  setYLabels(cross,disks)
  setYLabels(consumed,disks)
  setYLabels(skipped,disks)
  setYLabels(rate,disks)
  setXLabels(totalbytes,sites)
  cross.GetXaxis().LabelsOption("v");
  #cross.GetYaxis().LabelsOption("v");
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
    for item in data:
      if item["username"] == "dunepro":
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
      #print (type(isite),type(idisk))
      #print ("item",site,isite,disk,idisk)
      cross.Fill(isite,idisk,1.0)
      totalbytes.Fill(isite,item["file_size"]*0.000000001)
      if(item["last_file_state"] == "consumed"):
        consumed.Fill(isite,idisk,1.0)
       
        rate.Fill(isite,idisk,item["rate"])
      if(item["last_file_state"] == "skipped"):
        skipped.Fill(isite,idisk,1.0)
    data = None
  
  c = ROOT.TCanvas()
  c.SetLeftMargin(0.2)
  c.SetBottomMargin(0.2)
  c.SetRightMargin(0.15)
  #cross.SetMinimum(1)
  c.SetLogz()
  cross.Print("ALL")
  cross.Draw("COLZ")
  c.Print(out_name+"traffic.png")
  c.SetLogz(0)
  c.SetLogy(1)
  totalbytes.Draw()
  c.Print(out_name+"totalbytes.png")
  c.SetLogy(0)
  total = consumed.Clone("total")
  total.Add(skipped)
  efficiency = consumed.Clone("efficiency ")
  efficiency.SetTitle("success rate "+out_name)
  efficiency.Divide(total)
  efficiency.Draw("COLZ")
  c.Print(out_name+"efficiency.png")
  c.SetLogz(1)
  rate.SetMaximum(100.)
  rate.Divide(consumed)
  rate.SetTitle(rate.GetTitle()+" " + out_name)
  rate.Draw("COLZ")
  c.Print(out_name+"rate.png")
  


if __name__ == '__main__':

   
 
  start_date = date(2021, 2, 1)
  end_date = date(2021, 2, 12)
  delta = timedelta(days=1)
  analyze(start_date,end_date,delta)
