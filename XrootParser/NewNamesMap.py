#!/usr/bin/python3
import sys
import getopt
import csv
import json
import os
import datetime as dt


def csv_to_json(csvfilepaths, jsonFilePath):
	jsonArray = []
	nodename_sitename = {"br_cat.cbpf.br": "BR_CBPF", 
	 "ch_cern.ch": "CERN-PROD", 
	 "ch_unibe-lhcp.bern.ch": "UNIBE-LHEP", 
	 "cz_farm.particle.cz": "FZU", 
	 "es_ciemat.es": "CIEMAT-LCG2", 
	 "es_pic.es": "PIC", 
	 "fr_in2p3.fr": "IN2P3-CC", 
	 "nl_farm.nikhef.nl": "NIKHEF-ELPROD", 
	 "uk_brunel.ac.uk": "BRUNEL", 
	 "uk_ecdf.ed.ac.uk": "UKI-SCOTGRID-EDCF", 
	 "uk_esc.qmul.uk": "UKI-LT2-QMUL", 
	 "uk_grid.hep.ph.ic.ac.uk": "UKI-LT2-IC-HEP", 
	 "uk_gridpp.rl.ac.uk": "RAL-LCG2", 
         "xrootd.echo.stfc.ac.uk": "RAL-LCG2",
	 "uk_in.tier2.hep.manchester.ac.uk": "UKI-NORTHGRID-MAN-HEP", 
	 "uk_nubes.stfc.ac.uk": "RAL-LCG2", 
	 "uk_ph.liv.ac.uk": "UKI-NORTHGRID-LIV-HEP", 
	 "uk_physics.ox.ac.uk": "UKI-SOUTHGRID-OX-HEP", 
	 "uk_pp.rl.ac.uk": "UKI-SOUTHGRID-RALPP", 
	 "uk_sheffield.uk": "UKI-NORTHGRID-SHEF-HEP", 
	 "us_campuscluster.illinois.edu": "MWT2", 
	 "us_chtc.wisc.edu": "GLOW", 
	 "us_colorado.edu": "COLORADO", 
	 "us_crc.nd.edu": "NWICG_NDCMS", 
	 "us_crush.syracuse.edu": "SU-ITS-CE2", 
	 "us_fnal.gov": "US_FNAL", 
	 "us_iu.edu": "MWT2", 
	 "us_nmsu.edu": "SLATE_US_NMSU_DISCOVERY", 
	 "us_rcf.bnl.gov": "BNL-SDCC-CE01", 
	 "us_sdcc.bnl.gov": "BNL-SDCC-CE01", 
	 "us_uct2-mwt2.uchicago.edu": "MWT2", 
	 "us_unl.edu": "Nebraska", 
	 "us_usatlas.bnl.gov": "BNL-SDCC-CE01",
	 "fndca1.fnal.gov": "US_FNAL",
	 "eospublic.cern.ch": "CERN-PROD", }
	for file in csvfilepaths:
		if os.path.isfile(file):
			with open(file, encoding='utf-8') as csvf:
				csvReader = csv.DictReader(csvf)

				for row in csvReader:
					zac_dict={}
					zac_dict['name'] = row['file_name']
					source=row['disk']
					newsource=nodename_sitename[source]
					zac_dict['source']=newsource
					destination=row['site']
					newdestination=nodename_sitename[destination]
					zac_dict['destination']=newdestination
					zac_dict['file_size'] = row['file_size']
					zac_dict['start_time'] = row['timestamp']
					zac_dict['file_transfer_time']=row['duration']
					zac_dict['transfer_speed(MB/s)']=row['rate']
					Brate=int(float(row['rate'])*1000000)
					zac_dict['transfer_speed(B/s)']=Brate	
					jsonArray.append(zac_dict)
				mydatadict={}
				mydatadict['data']=jsonArray

	with open(jsonFilePath, "w", encoding='utf-8') as jsonf:
		jsonString = json.dumps(mydatadict, indent=4)
		jsonf.write(jsonString)



def main(argv):
    csvfilepath =''
    jsonfilepath ='out.json'
    day1 = dt.datetime.strptime("2021-06-30", "%Y-%m-%d")
    delta = dt.timedelta(days=1)
    today = dt.datetime.today()
    csvfilepaths = []
    nextday=day1+delta


    try:
        opts, args = getopt.getopt(argv,"hs:e:S:E:",["sdate=","edate="])
    except getopt.GetoptError:
        print ('python3 csvtojsonmulti.py -s <date> -e <date>')
        sys.exit(2)
    print(opts)
    for opt, arg in opts:
        if opt == '-h':
            print ('python3 csvtojsonmulti.py -s <date> -e <date>')
            sys.exit()
        elif opt in ("-S","-s", "--sdate"):
            day1 = dt.datetime.strptime(arg, "%Y/%m/%d")
        elif opt in ("-E","-e", "--edate"):
            enddate= dt.datetime.strptime(arg, "%Y/%m/%d")
    nextday=day1+delta
    print(day1, nextday, enddate)
    while day1 < enddate:
        csvstring="/root/data-mgmt-testing/XrootParser/data/user_%s_%s.csv"%(day1.strftime("%Y-%m-%d"),nextday.strftime("%Y-%m-%d"))
        csvfilepaths.append(csvstring)
        print(csvstring)
        day1+=delta
        nextday+=delta
        if day1==enddate:
            break
#    jsonfilepath = os.path.splitext(csvfilepath)[0]+'.json' if jsonfilepath =='' else jsonfilepath
    csv_to_json(csvfilepaths, jsonfilepath)

if __name__ == "__main__":
   main(sys.argv[1:])


