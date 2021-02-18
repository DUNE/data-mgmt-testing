#!/usr/local/opt/python/libexec/bin/python
import os,sys,csv,string,json,datetime,dateutil

import ROOT
import numpy as np
from datetime import date,timezone,datetime
from dateutil import parser

def getListOfTypes(data,key):
  l = []
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

def analyze(data):
  sites = getListOfTypes(data,"site")
  disks = getListOfTypes(data,"file_location")
  ns = len(sites)
  nd = len(disks)
  print ("sites",sites)
  print ("disks",disks)
  ROOT.gStyle.SetOptStat(0)
  cross = ROOT.TH2F("cross","transfer;disk,cpu",ns+1,0,ns+1,nd+1,0,nd+1)
  for bin in sites:
    cross.GetXaxis().SetBinLabel(sites[bin], bin);
    print("label",sites[bin],bin)
  for bin in disks:
    cross.GetYaxis().SetBinLabel(disks[bin], bin);
    print("label",disks[bin],bin)
  cross.GetXaxis().LabelsOption("v");
  #cross.GetYaxis().LabelsOption("v");
  for item in data:
    if "site" not in item or "file_location" not in item:
      #print("missing: ",item["node"])
      continue
    site = item["site"]
    isite = float(sites[site])-.5
    disk = item["file_location"]
    idisk = float(disks[disk])-.5
    #print (type(isite),type(idisk))
    #print ("item",site,isite,disk,idisk)
    cross.Fill(isite,idisk,1.0)
  
  c = ROOT.TCanvas()
  c.SetLeftMargin(0.2)
  c.SetBottomMargin(0.2)
  c.SetRightMargin(0.15)
  cross.Print("ALL")
  cross.Draw("COLZ")
  c.Print("traffic.png")
  
  
  


if __name__ == '__main__':

  f = open(sys.argv[1],"r")
  data = json.load(f)
  sites = getListOfTypes(data,"site")
  print (sites)
  analyze(data)
