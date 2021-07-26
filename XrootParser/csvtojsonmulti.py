#!/usr/bin/python3
import sys
import getopt
import csv
import json
import os
import datetime as dt


def csv_to_json(csvfilepaths, jsonFilePath):
	jsonArray = []
	for file in csvfilepaths:
		if os.path.isfile(file):
			with open(file, encoding='utf-8') as csvf:
				csvReader = csv.DictReader(csvf)

				for row in csvReader:
					zac_dict={}
					zac_dict['name'] = row['file_name']
					zac_dict['source'] = row['disk']
					zac_dict['destination'] = row['site']
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


