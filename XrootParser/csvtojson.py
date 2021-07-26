#!/usr/bin/python3
import sys
import getopt
import csv
import json
import os


def csv_to_json(csvFilePath, jsonFilePath):
	jsonArray = []

	with open(csvFilePath, encoding='utf-8') as csvf:
		csvReader = csv.DictReader(csvf)

		for row in csvReader:
			zac_dict={}
			zac_dict['Name'] = row['file_name']
			zac_dict['Source'] = row['disk']
			zac_dict['Destination'] = row['site']
			zac_dict['File_size'] = row['file_size']
			zac_dict['Start_time'] = row['timestamp']
			zac_dict['File_transfer_time']=row['duration']
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
   jsonfilepath =''
   try:
      opts, args = getopt.getopt(argv,"hi:o:",["ifile=","ofile="])
   except getopt.GetoptError:
      print ('test.py -i <csvfilepath> -o <jsonfilepath>')
      sys.exit(2)
   for opt, arg in opts:
      if opt == '-h':
         print ('test.py -i <csvfilepath> -o <outputfilepath>')
         sys.exit()
      elif opt in ("-i", "--ifile"):
         csvfilepath = arg
      elif opt in ("-o", "--ofile"):
         jsonfilepath = arg
   jsonfilepath = os.path.splitext(csvfilepath)[0]+'.json' if jsonfilepath =='' else jsonfilepath
   csv_to_json(csvfilepath, jsonfilepath)

if __name__ == "__main__":
   main(sys.argv[1:])


