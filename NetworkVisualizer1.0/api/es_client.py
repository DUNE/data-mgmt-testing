import sys
import json
import os
import shutil
from datetime import datetime, timedelta
import argparse as ap

today = datetime.today()
parser = ap.ArgumentParser()
parser.add_argument('-S', '--start', dest="start_date", default=today.strftime("%Y/%m/%d"))
parser.add_argument('-E', '--end', dest="end_date", default="0")
parser.add_argument('-T', '--transferType', dest="transferType", default="SAM")

args = parser.parse_args()

if args.transferType == "RUCIO":
    json_final = json.dumps({'data': 'good'}, indent=2)
    with open('success.json', 'w') as json_file:
        json_file.write(json_final)
else:
    path = '/dune/data/users/jyeung/samweb_outjsons/'
    start = args.start_date
    start = start.replace('/', '-')
    day = datetime.strptime(start, "%Y-%m-%d")
    delta = timedelta(days=1)
    end = args.end_date
    end = end.replace('/', '-')
    if end == "0":
        shutil.copyfile((path + str(day.year) + '/' + str(day.month).zfill(2) + '/' + 'out_dune_' + day.strftime("%Y-%m-%d") + '_to_' + (day+delta).strftime("%Y-%m-%d") + '.json'), (os.getcwd() + '/out.json'))
    else:
        json_total = []
        dayfinal = datetime.strptime(end, "%Y-%m-%d")
        dayfinal = dayfinal + delta

        assert day < dayfinal, 'Start date must be before end date'

        while day < dayfinal:
            json_path = path + str(day.year) + '/' + str(day.month).zfill(2) + '/' + 'out_dune_' + day.strftime("%Y-%m-%d") + '_to_' + (day+delta).strftime("%Y-%m-%d") + '.json'
            with open(json_path) as json_file:
                json_add = json.load(json_file)
            json_total = json_total + json_add['data']
            day = day + delta
        
        json_final = json.dumps({'data': json_total}, indent=2)
        with open('out.json', 'w', encoding='utf-8') as json_file:
            json_file.write(json_final)
