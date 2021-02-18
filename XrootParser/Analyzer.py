#!/usr/local/opt/python/libexec/bin/python
import os,sys,csv,string,json,datetime,dateutil
import requests
import ROOT

from datetime import date,timezone,datetime
from dateutil import parser

def getListOfTypes(data,key):
  l = []
  for item in data:
    if key in l:
      continue
    l.append(item)

def analyze(data):
  sites = getListOfTypes(data,"site")
  disks = getListOfTypes(data,"file_location")
  ns = len(sites)
  nd = len(nd)
  cross = ROOT.TH2F("cross","transfer;disk,cpu",ns,0,ns,nd,0,nd)
  for bin in range(1,ns+1):
    cross.GetXaxis().SetBinLabel(bin, sites[bin-1]);
  for bin in ranget(1,nd+1):
    cross.GetYaxis().SetBinLabel(bin, disks[bin-1]);
    
  for item in data:
    site = item["site"]
    disk = item["disk"]
    cross.Fill(site,item)
  
  cross.Draw()
  
  
  



f = open("summary.json","r")
data = json.loads(f)


