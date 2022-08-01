#!/usr/local/opt/python/libexec/bin/python
# works with python2
import os,sys,csv,string,json,datetime,dateutil,jsonlines
import math
FAST=False
SINGLE=True
durationcut = 50
#vetodisks = ["golias100.farm.particle.cz","eospublic.cern.ch","fal-pygrid-30.lancs.ac.uk","lancs.ac.uk"]
vetodisks=[]
if not FAST:
    durationcut = 200
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
    if FAST and item["application"]!= "pdspana":
        continue
    if FAST and item["file_location"] in vetodisks:
        continue
    if not key in item:
      continue
    val = item[key]#.decode('UTF-8')
    if key == "site":
      val = countrify(val)
    # try to figure out nodes without counting em all.
    if key == "node":
      if "sandy" in val:
        val = "sandy.local.pr"
      if "comp" in val:
        val = "lancaster.uk"
      if "gridpp.rl.ac.uk" in val:
        val = "gridpp.rl.ac.uk"
      tags = val.split(".")
      if len(tags)>1:
        val = val.replace(tags[0]+".","")
        
    if val in l:
      continue
    
    l = np.append(l,val)
  sl = sorted(l)
  m = {}
  for i in range(0,len(l)):
    m[sl[i]] = i+1
    
  return m

def shortdisk(disk):
    if "manchester" in disk:
        return "manchester"
    if "lancs" in disk:
      return "lancaster"
    if "fndca" in disk:
        return "fndca"
    if "echo" in disk:
        return "echo"
    if "cern" in disk:
      return "cern"
    return disk
    
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

def loadjsonlines(inputfilename):
  print (inputfilename)
  data = []
  with jsonlines.open(inputfilename, mode='r') as reader:
      for obj in reader:
        data.append(obj)
  reader.close()
  return data
  
def analyze(start_date,end_date,delta , expt, pid):

# first get lists of variables
  
  sites = {}
  disks = {}
  users = {}
  dates = {}
  states = {}
  apps = {"unknown":0}
  start_range = start_date
  hostnames = {}
  hostlist = open('hostnames.txt','w')
  while start_range < end_date:
    end_range = start_range + delta
    inputfilename = "data/%s_summary_%s_%s.jsonl"%(expt,start_range,end_range)
    if not os.path.exists(inputfilename):
      start_range += delta
      continue
    #inputfile = open(inputfilename,'r')
    #print ("input:",inputfilename)
    start_range += delta
    data = loadjsonlines(inputfilename)
    hostnames = getListOfTypes(data,"node",hostnames)
    apps = getListOfTypes(data,"application",apps)
    
    sites = getListOfTypes(data,"site",sites)
    states = getListOfTypes(data,"last_file_state",states)
    disks = getListOfTypes(data,"file_location",disks)
    users = getListOfTypes(data,"username",users)
    dates = getListOfDates(data,dates)
    
    if DUNEPRO:
      users = {"dunepro":1}
  for i in hostnames:
    hostlist.write("%s\n"%i)
  hostlist.close()
  print (dates)
  firstday = start_date
  lastday = end_date
  if (not FAST):
    out_name = "%s_slow_%s_%s_%d"%(expt,firstday,lastday,pid)
  else:
    out_name = "%s_fast_%s_%s_%d"%(expt,firstday,lastday,pid)
  if not xroot:
       out_name = out_name + "_notxroot"
  out_name = out_name.replace("-","_")
  #sites = sorted(sites,reverse=True)
  
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
  apptiming = {}
  remtiming = {}
  siterate = {}
  for app in apps:
    apptiming[app] = ROOT.TH1D(app,"log10 timing for app %s at FNAL; Log10  of streaming rate, MB/sec"%app,50,-2.,3.)
    remtiming[app] = ROOT.TH1D(app+"_rem","log10 timing for app %s; Log10 of streaming rate, MB/sec"%app,50,-2.,3.)
  for site in sites:
    for disk in disks:
        siterate[site+disk] = ROOT.TH1D(site+disk,"log10 timing for site %s and disk %s; Log10  of streaming rate, MB/sec"%(site,disk),20,-2.,2.)
  cross = ROOT.TH2F("cross","transfers",nd,0,nd,ns,0,ns)
  setXYLabels(cross,disks,sites)
  state = ROOT.TH2F("state","transfers",nstate,0,nstate,ns,0,ns)
  setXYLabels(state,states,sites)
  consumed = ROOT.TH2F("consumed","consumed",nd,0,nd,ns,0,ns)
  setXYLabels(consumed,disks,sites)
  invconsumed = ROOT.TH2F("invconsumed","consumed",nd,0,nd,ns,0,ns)
  setXYLabels(invconsumed,disks,sites)
  consumed_by_app = ROOT.TH2F("consumed_by_app","consumed",na,0,na,ns,0,ns)
  setXYLabels(consumed_by_app,apps,sites)
  rate = ROOT.TH2F("rate","rate for consumed, MB/s",nd,0,nd,ns,0,ns)
  setXYLabels(rate,disks,sites)
  inverserate = ROOT.TH2F("inverserate","1/rate for consumed, scaled to FNAL rate",nd,0,nd,ns,0,ns)
  setXYLabels(inverserate,disks,sites)
  rate_by_app = ROOT.TH2F("rate_by_app","rate for consumed, by app, MB/s",na,0,na,ns,0,ns)
  setXYLabels(rate_by_app,apps,sites)
  ratelog10 = ROOT.TH2F("rates","rate for consumed, MB/s;;Log10 Rate",24,-3,3.,ns,0,ns)
  h_duration = ROOT.TH1F("duration", "duration in seconds",100, 0,500.)
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
    
    inputfilename = "data/%s_summary_%s_%s.jsonl"%(expt,start_range,end_range)
    print ("read ",inputfilename)
    if not os.path.exists(inputfilename):
      print ("skipping missing",inputfilename)
      start_range += delta
      continue
    #inputfile = open(inputfilename,'r')
    #print ("read ",inputfilename)
    days += 1.0
    start_range += delta
    #data = json.load(inputfile)
    data = loadjsonlines(inputfilename)
    for item in data:
      sumrec={}
      
      # remove some special runs
      if "SCEsyst"  in item["file_url"]:
        continue
      #if "fnal.gov" in item["node"]:
      #  print (item)
      if not xroot and "root:" in item["file_url"]:
         
        continue
      if xroot and "http:" in item["file_url"]:
        continue
      if pid > 0:
        if item["project_id"] != pid:
            continue
      if "node" not in item:
        continue
      if "site" not in item or "file_location" not in item:
        print("missing a site",item["job_id"],item["node"])
        continue
      
       
        
      site = countrify(item["site"])
      if site == "_":
        print ("strange site",item["node"])
      if "duration" not in item:
        continue
      else:
        duration = item["duration"]
      h_duration.Fill(item["duration"])
      if "rate" not in item:
        continue
      #rate = item["file_size"]/item["duration"]/1000000.
      #print ("got here")
      finalstate = item["last_file_state"]
      if "gpvm" in item["node"]:
        continue
      application = "unknown"
      user = item["username"]
      if "application" in item:
        application = item["application"]
        if not FAST:
          if (application !="reco" or user != "dunepro" or duration < 500):
            continue
          if ("NP02" in item["file_url"] or item["data_tier"]!= "raw"):
            continue
          if item["rate"] > 2 and finalstate=="consumed":
            print (item)
        else:
          if user =="dunepro" or application == "reco":
            continue
          if "reconstructed" not in item["data_tier"]:
            continue
          if SINGLE and application != "pdspana" and user !="calcuttj":
            continue
          # skip unknown apps
      if application not in apps:
        continue
      version = "unknown"
      if "version" in item:
        version = item["version"]
      if "." in version:
        version = "invalid"
      isite = float(sites[site])-.5
      disk = item["file_location"]
      if FAST and disk in vetodisks:
        continue
      idisk = float(disks[disk])-.5
      user = item["username"]
      iuser = float(users[user])-.5
      
      date = item["@timestamp"][0:10]
      if date not in dates:
        print ("missing this date",date)
        continue
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
      else:
        sumrec["process_id"] = 0
      sumrec["timestamp"] = item["@timestamp"]
      sumrec["duration"] = duration
      sumrec["file_size"] = item["file_size"]
      if "file_type" in item: # added in later.
        sumrec["file_type"] = item["file_type"]
      sumrec["username"] = user
      sumrec["application"] = application
      sumrec["version"] = version
      sumrec["final_state"] = finalstate
      sumrec["site"] = site
      sumrec["rate"] = item["rate"]
      sumrec["project_name"] = item["project_name"]
      sumrec["file_name"] = os.path.basename(item["file_url"])
      sumrec["data_tier"] = item["data_tier"]
      sumrec["node"] = item["node"]
      sumrec["country"] = site[0:2]
      myrate = item["rate"]
      # get intrinsic rate
      if duration < durationcut:
        continue
      #if item["campaign"] != "PDSPProd4a":
      #   continue
      if finalstate == "consumed":
        siterate[site+disk].Fill(math.log10(myrate))
      if not "us" in sumrec["country"] and not "fnal" in sumrec["node"] and finalstate == "consumed":
        if  "fnal" in disk or "gsiftp" in disk:
          remtiming[application].Fill(math.log10(myrate))
      if "fnal" in site:
        sumrec["country"]="fnal"
      if "fnal" in site and finalstate=="consumed":
        if "fnal" in disk or "gsiftp" in disk:
          apptiming[application].Fill(math.log10(myrate))
      
      if "cern" in site:
        sumrec["country"]="cern"
      if "campaign" in item:
        sumrec["campaign"] = item["campaign"]
        
      else:
        sumrec["campaign"] = None
      #print (sumrec)
      summary.append(sumrec)
#      if item["username"] == "dunepro" and not DUNEPRO :
#        continue
#      if not item["username"] == "dunepro" and DUNEPRO :
#        continue
#     
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
          if FAST and item["rate"] > 1.0:
            inverserate.Fill(idisk,isite,1./item["rate"])
            invconsumed.Fill(idisk,isite,1.)
          if not FAST and item["rate"] < 1.0:
            inverserate.Fill(idisk,isite,1./item["rate"])
            invconsumed.Fill(idisk,isite,1.)
          
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
      print("csv I/O error")
  try:
      g = open("digest_%s.json"%(out_name),'w')
      s = json.dumps(summary,indent=2)
      g.write(s)
  except IOError:
      print("json I/O error")
      
  try:
    with jsonlines.open("digest_%s.jsonl"%(out_name), mode='w') as writer:
      for i in summary:
        writer.write(i)
        
  except IOError:
      print("jsonl I/O error")
  
  
 
  ROOT.gStyle.SetPalette(ROOT.kLightTemperature);
  ROOT.gStyle.SetPaintTextFormat("4.1f")
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
  #c.SetLogz()
  #ROOT.gStyle.SetPaintTextFormat("5.2f")
  #cross.Print("ALL")
  drates = open("rates.txt",'w')
  ROOT.gStyle.SetLegendFillColor(0);
  pdfname = "sites/ALL"+out_name

  pdfstart = pdfname + ".pdf("
  pdfend = pdfname + ".pdf)"
  c.Print(pdfstart,".pdf")
  h_duration.Draw()
  c.Print("duration",".pdf")
  mycolor = [ROOT.kBlack,ROOT.kBlue,9,38,ROOT.kCyan]
  myline = [1,2]
  for site in sites:
    max = 0
    for disk in disks:
        if max < siterate[site+disk].GetMaximum():
            max = siterate[site+disk].GetMaximum()
        #print ("max =", max)
    base = "fndca1.fnal.gov"
    siterate[site+base].SetMaximum(max*1.5)
    siterate[site+base].SetTitle(site)
    siterate[site+base].Draw("HIST")
     
    siterate[site+disk].SetLineStyle(disks[disk]%2)
    srates = "%32s\t"%(site)
    icolor = 0
    leg = ROOT.TLegend(0.22,0.75,0.4,0.89)
    leg.SetBorderSize(0)
    leg.SetFillColor(0)
    
    ibase = disks[base] -1
    siterate[site+base].SetLineStyle(myline[(ibase)%2])
    siterate[site+base].SetLineColor(mycolor[int(ibase/2)])
    ROOT.gStyle.SetLegendTextSize(0.03)
    for disk in disks:
        icolor = disks[disk] -1
        siterate[site+disk].SetLineWidth(2)
        siterate[site+disk].SetLineStyle(myline[(icolor%2)])
        siterate[site+disk].SetLineColor(mycolor[int(icolor/2)])
        siterate[site+disk].Draw("SAME HIST")
        
        mean = siterate[site+disk].GetMean()
        m = math.pow(10, mean)
        rms = siterate[site+disk].GetStdDev()
        l = math.pow(10,mean - rms )
        u = math.pow(10, mean + rms )
        n =siterate[site+disk].GetEntries()
        if n > 0:
            if FAST:
              entry = leg.AddEntry(siterate[site+disk],"%s %4.1f-%4.1f MB/s"%(shortdisk(disk),l,u),"f")
            else:
              entry = leg.AddEntry(siterate[site+disk],"%s %4.2f-%4.2f MB/s"%(shortdisk(disk),l,u),"f")
              
        else:
            entry = leg.AddEntry("","","")
            l = 0
            u = 0
            m = 0
        leg.Draw("SAME")
        #print (mean,rms,m,l,u)
        srates += " %s: %5d \t= %4.1f-%4.1f+%4.1f\t"%(shortdisk(disk),n,m,l,u)
    print (srates)
    srates += "\n"
    drates.write(srates)
    c.Print("sites/"+out_name+"_%s.png"%site)
    c.Print(pdfname+".pdf","%s"%site)
  c.Print(pdfend,".pdf)")
  cross.Draw("COLZ")

  cross.Draw("TEXT SAME")
  c.Print("pix/"+out_name+"_traffic.png")
  c.Print("C/"+out_name+"_traffic.C")
  state.Draw("COLZ")
  state.Draw("TEXT SAME")
  c.Print("pix/"+out_name+"_states.png")
  c.Print("C/"+out_name+"_states.C")
  
  
  c.SetLogz(0)
  c.SetLogy(1)
  totalbytes.SetMinimum(1.)
  totalbytes.Draw("")
  c.Print("pix/"+out_name+"_totalbytes_site.png")
  c.Print("C/"+out_name+"_totalbytes_site.C")
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
  
  c.Print("pix/"+out_name+"_totalbytes_user.png")
  c.Print("C/"+out_name+"_totalbytes_user.C")
  
  
  totalbytes_date.Draw()
  c.Print("pix/"+out_name+"_totalbytes_date.png")
  c.Print("C/"+out_name+"_totalbytes_date.C")
  c.SetLogy(0)
  total = consumed.Clone("total")
  total.Add(skipped)
  efficiency = consumed.Clone("efficiency ")
  efficiency.SetTitle("success rate "+out_name)
  efficiency.Divide(total)
  efficiency.Draw("COLZ NUM")
  ROOT.gStyle.SetPalette(ROOT.kLightTemperature);
  ROOT.gStyle.SetPaintTextFormat("4.2f")
  efficiency.Draw("TEXT SAME")
  c.Print("pix/"+out_name+"_efficiency.png")
  c.Print("C/"+out_name+"_efficiency.C")
  #c.SetLogz(1)
  #rate.SetMaximum(100.)
  rate.Divide(consumed)
  rate.SetTitle(rate.GetTitle()+" " + out_name)
  if FAST:
    rate.SetMaximum(20.)
  else:
    rate.SetMaximum(1.)
   
  rate.Draw("COLZ")
  #ROOT.gStyle.SetPaintTextFormat("5.2f")
  #rate.SetMaximum(math.Log10(20.))
  rate.Draw("TEXT SAME")
  
  c.Print("pix/"+out_name+"_rate.png")
  c.Print("C/"+out_name+"_rate.C")
  inverserate.Divide(invconsumed)
  inverserate.Scale(5.0)
  inverserate.SetTitle(inverserate.GetTitle()+" " + out_name)
  if FAST:
    inverserate.SetMaximum(5.)
    inverserate.SetMinimum(0.)
  else:
    inverserate.SetMaximum(100.)
   
  inverserate.Draw("COLZ")
  #ROOT.gStyle.SetPaintTextFormat("5.2f")
  #rate.SetMaximum(math.Log10(20.))
  inverserate.Draw("TEXT SAME")
  
  c.Print("pix/"+out_name+"_inverserate.png")
  c.Print("C/"+out_name+"_inverserate.C")
  #rate_by_app.SetMaximum(100.)
  rate_by_app.Divide(consumed_by_app)
  rate_by_app.SetTitle(rate_by_app.GetTitle()+" " + out_name)
  rate_by_app.Draw("COLZ")
  #ROOT.gStyle.SetPaintTextFormat("5.2f")
  rate_by_app.Draw("TEXT45 SAME")
  c.SetLogz(1)
  c.Print("pix/"+out_name+"_rate_by_app.png")
  c.Print("C/"+out_name+"_rate_by_app.C")
  
  c.SetLogz(0)
  ratelog10.Draw("BOX")
  c.Print("pix/"+out_name+"_ratelog10.png")
  c.Print("C/"+out_name+"_ratelog10.C")
  print ("total count is ",count)
  speeds = ROOT.TFile.Open(out_name+"_speeds.root","RECREATE")
  stat = np.zeros(4)
  remstat = np.zeros(4)
  r = open("pix/"+out_name+"_stats.csv",'w')
  
  str = "%20s\t%10s\t %5s\t -\t%5s\t +\t%5s\t %10s\t %5s\t -\t%5s\t +\t%5s\t  ratio = \t%5s"%("application","n FNAL","mean","1sig","1sig","nremote","mean","1sig","1sig","ratio")
  print (str)
  r.write(str+"\n")
  for app in apptiming:
    apptiming[app].Write()
    remtiming[app].Write()
    n = apptiming[app].GetEntries()
    if n< 100 and remtiming[app].GetEntries() < 100:
      continue
    apptiming[app].GetStats(stat)
    mean = math.pow(10,apptiming[app].GetMean())
    varplus =  math.pow(10,apptiming[app].GetMean()+apptiming[app].GetStdDev()) -mean
    varminus = mean - math.pow(10,apptiming[app].GetMean()-apptiming[app].GetStdDev())
    nrem = remtiming[app].GetEntries()
    remtiming[app].GetStats(stat)
    remmean = math.pow(10,remtiming[app].GetMean())
    remvarplus =  math.pow(10,remtiming[app].GetMean()+remtiming[app].GetStdDev()) -remmean
    remvarminus = remmean - math.pow(10,remtiming[app].GetMean()-remtiming[app].GetStdDev())
#f    print (math.pow(10,apptiming[app].GetMean()-apptiming[app].GetStdDev()),mean)
    #print ("%20s %10d %5.2f -%5.2f +%5.2f "%(app,n,mean,varminus,varplus))
    
#f    print (math.pow(10,apptiming[app].GetMean()-apptiming[app].GetStdDev()),mean)
    ratio = remmean/mean
    if nrem < 100 or n < 100:
      ratio = 0.0
    if nrem == 0:
      remmean = 0.0
    if n == 0:
      mean = 0.0
    str = "%20s\t%10d\t %5.2f\t -\t%5.2f\t +\t%5.2f\t %10d\t %5.2f\t -\t%5.2f\t +\t%5.2f\t  ratio = \t%5.2f"%(app,n,mean,varminus,varplus,nrem,remmean,remvarminus,remvarplus,ratio)
    print (str)
    r.write(str+"\n")
    c.SetLogz(0)
    leg = ROOT.TLegend(0.5,0.8,0.8,0.9)
    leg.AddEntry(remtiming[app],"non-FNAL")
    leg.AddEntry(apptiming[app],"FNAL")
    max = apptiming[app].GetMaximum()
    if remtiming[app].GetMaximum() > max:
      max = remtiming[app].GetMaximum()
    remtiming[app].SetMaximum(max*1.5)
    remtiming[app].Draw("hist")
    apptiming[app].SetLineColor(2)
    apptiming[app].Draw("same hist")
    leg.Draw()
    c.Print("pix/"+out_name+"_"+app+".png")
    c.Print("C/"+out_name+"_"+app+".C")
    
  
  r.close()
  speeds.Close()

if __name__ == '__main__':

  
  if not os.path.exists("./data"):
    print (" expects data to have been moved to ./data directiory")
    sys.exit(1)
  if not os.path.exists("./pix"):
    print (" making a pix directory")
    os.mkdir("./pix")

  # determine the dates
  
  start_date = date(2021,1 , 1)
  end_date = date(2021, 1, 30)
  if len(sys.argv) >= 3:
    start = sys.argv[1].split("-")
    end = sys.argv[2].split("-")
    start_date = date(int(start[0]), int(start[1]) ,int(start[2]))
    end_date = date(int(end[0]), int(end[1]) ,int(end[2]))
  else:
    print ("expects YYYY-MM-DD YYYY-MM-DD for start and end dates, using default dates",start_date,end_date)
  delta = timedelta(days=1)
  if len(sys.argv) >=4:
    expt = sys.argv[3]
  else:
    print ("assuming you want expt==dune")
    expt = "dune"
  if len(sys.argv) >=5:
    pid = sys.argv[4]
  else:
    print ("doing all PID's")
    pid = 0
  analyze(start_date,end_date,delta,expt,int(pid))
