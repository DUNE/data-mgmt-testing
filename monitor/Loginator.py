
"""! @brief Art logfile parser """
##
# @mainpage Loginator.py
#
# @section description_main Description
# A program for parsing art logs to put information into DUNE job monitoring.
#
#
# Copyright (c) 2022 Heidi Schellman, Oregon State University
##
# @file Loginator.py

import string,time,datetime,json,os,sys

DEBUG=False

class Loginator:
    def __init__(self,logname):
        if not os.path.exists(logname):
            print ("no such file exists, quitting",logname)
            sys.exit(1)
        self.logname = logname
        self.logfile = open(logname,'r')
        self.outobject ={}
        self.info = self.getinfo()
        self.tags = ["Opened input file", "Closed input file","Peak resident set size usage (VmHWM)"]
        self.template = {
            "source_rse":None,  #
            "user":None,  # (who’s request is this)
            "job_id":None, # (jobsubXXX03@fnal.gov)
            "timestamp_for_start":None,  #
            "timestamp_for_end":None,  #
            "duration":None,  # (difference between end and start)
            "input_file_size":None,  #
            "application_name":None,  #
            "application_version":None,  #
            "final_state":None,  # (what happened?)
            "cpu_site":None,  # (e.g. FNAL":None,  # RAL)
            "project_name":None, #(wkf request_id?)"
            "file_name":None,  # (including the metacat namespace)
            "data_tier":None,  # (from metacat)
            "job_node":None,  # (name within the site)
            "job_site":None,  # (name of the site)
            "country":None,  # (nationality of the site)
            "campaign":None,  # (DUNE campaign)
            "access_method":None, #(stream/copy)
            "workflow_method":None,
            "path":None,
            "real_memory":None
        }
       
## return the first tag or None in a line
    def findme(self,line):
        for tag in self.tags:
            if tag in line:
                if DEBUG: print (tag,line)
                return tag
        return None
        
## get system info for the full job
    def getinfo(self):
        info = {}
        # get a bunch of system thingies.
        info["application_version"]=os.getenv("DUNESW_VERSION")
        info["job_node"] = os.getenv("HOST")
        info["job_site"] = os.getenv("GLIDEIN_DUNESite")
        info["POMSINFO"] = os.getenv("poms_data")  # need to parse this further
        return info

## read in the log file and parse it, add the info
    def readme(self):
        object = {}
        for line in self.logfile:
            tag = self.findme(line)
            if DEBUG: print (tag,line)
            if tag == None:
                continue
            if "file" in tag:
                data = line.split(tag)
                filefull = data[1].strip().replace('"','')
                timestamp = data[0].strip()
                filename = os.path.basename(filefull).strip()
                filepath = os.path.dirname(filefull).strip()
                if "Opened" in tag and not filename in object.keys():
                    object[filename] = self.template
                    object[filename]["timestamp_for_start"] = timestamp
                    object[filename]["Path"]=filepath
                    object[filename]["file_name"] = filename
                    print ("filepath",filepath)
                    if "root" in filepath[0:10]:
                        print ("I am root")
                        tmp = filepath.split("//")
                        object[filename]["source_rse"] = tmp[1]
                        object[filename]["access_method"] = "xroot"
                    for thing in self.info:
                        object[filename][thing] = self.info[thing]
                    object[filename]["final_state"] = "Opened"
                if "Closed" in tag:
                    object[filename]["timestamp_for_end"] = timestamp
                    object[filename]["final_state"] = "Closed"
                continue
            if "size usage" in tag:
                data = line.split(":")
                for thing in object:
                    object[thing]["real_memory"]=data[1].strip()
        self.outobject=object
        
    def metacatinfo(self,namespace,filename):
        print ("do something here")
    

    def writeme(self):
        for thing in self.outobject:
            outname = thing+".process.json"
            outfile = open(outname,'w')
            json.dump(self.outobject[thing],outfile,indent=4)
            outfile.close()

def envScraper():
    env = os.environ
    if "apple" in env["CLANGXX"]:
        f = open("bigenv.txt")
        env = {}
        for a in f.readlines():
            line = a.split("=")
            env[line[0]] = line[1]
    digest = {}
    for k in env.keys():
        if "SETUP_" in k:
            it = env[k].split(" ")
            digest[k] = {"Product":it[0],"Version":it[1]}
    return digest
    


parse = Loginator(sys.argv[1])
print ("looking at",sys.argv[1])
parse.readme()
parse.writeme()



