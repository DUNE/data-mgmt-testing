import sys
import json
import os
import shutil
from datetime import datetime, timedelta
import argparse as ap

# arguments from frotend inputs
today = datetime.today()
parser = ap.ArgumentParser()
parser.add_argument('-S', '--start', dest="start_date", default=today.strftime("%Y/%m/%d"))
parser.add_argument('-E', '--end', dest="end_date", default="0")
parser.add_argument('-T', '--transferType', dest="transferType", default="SAM")
parser.add_argument('-U', '--uuid', dest="uuid", default="")

args = parser.parse_args()

filename = ('out_jsons/' + str(args.uuid) + '-out.json')

# define start and end dates
start = args.start_date
start = start.replace('/', '-')
day = datetime.strptime(start, "%Y-%m-%d")
delta = timedelta(days=1)
end = args.end_date
end = end.replace('/', '-')

# create filler NaN json to handle case of empty json file
json_empty = [{
      "source": "No data for these dates",
      "user": "N/A",
      "date": "N/A",
      "process_id": "N/A",
      "start_time": "N/A",
      "file_transfer_time": "N/A",
      "file_size": "N/A",
      "username": "N/A",
      "application": "N/A",
      "version": "N/A",
      "final_state": "N/A",
      "destination": "No data for these dates",
      "transfer_speed(MB/s)": "N/A",
      "transfer_speed(B/s)": "N/A",
      "project_name": "N/A",
      "name": "N/A",
      "data_tier": ["N/A"],
      "node": "N/A",
      "file_count": "N/A"
      }]

# # if frotend input is Rucio Failed Transfers
# if args.transferType == "RUCIO_FAILED":
#     prefix = 'dune_failed_transfers_display_'
    
#     # current cache is in leeza directory
#     path = '/dune/data/users/jyeung/rucio_failure_testing/cache/'
#     if end == "0":
#         shutil.copyfile((path + str(day.year) + '/' + str(day.month).zfill(2) + '/' + prefix + day.strftime("%Y-%m-%d").replace('-', '_') + '_to_' + (day+delta).strftime("%Y-%m-%d").replace('-', '_') + '.json'), (os.getcwd() + '/out.json'))
#     else:
#         json_total = []
#         dayfinal = datetime.strptime(end, "%Y-%m-%d")
#         dayfinal = dayfinal + delta

#         assert day < dayfinal, 'Start date must be before end date'

#         # loop through dates
#         while day < dayfinal:
#             json_path = path + str(day.year) + '/' + str(day.month).zfill(2) + '/' + prefix + day.strftime("%Y-%m-%d").replace('-', '_') + '_to_' + (day+delta).strftime("%Y-%m-%d").replace('-', '_') + '.json'
#             try:
#                 with open(json_path) as json_file:
#                     json_add = json.load(json_file)
#                 json_total = json_total + json_add
#                 day = day + delta
#             except json.JSONDecodeError: # if json file is incorrect (e.g. missing a bracket)
#                 print('JSON Error:', json_path)
#                 day = day + delta
#             except FileNotFoundError: # if no data file exists (e.g. searching for a future date)
#                 print('No Data Available:', json_path)
#                 day = day + delta

#         if not json_total: # if there is no data, write the filler NaN json
#             json_final = json.dumps({'data': json_empty}, indent=2)
#             with open(filename, 'w', encoding='utf-8') as json_file:
#                 json_file.write(json_final)
#         else:
#             json_final = json.dumps({'data': json_total}, indent=2)
#             with open(filename, 'w', encoding='utf-8') as json_file:
#                 json_file.write(json_final)

# if frotend input is Rucio Transfers or Rucio Aggregate Transfers
if (args.transferType == "RUCIO") or (args.transferType == "RUCIO_AGGREGATE") or (args.transferType == "RUCIO_FAILED") or (args.transferType == "RUCIO_AGGREGATE_FAILED") or (args.transferType == "TEST_MODE") or (args.transferType == "TEST_MODE_FAILED"):
    if args.transferType == "RUCIO":
        prefix = 'dune_transfers_display_'
    elif args.transferType == "RUCIO_AGGREGATE":
        prefix = 'dune_transfers_aggregates_display_'
    elif args.transferType == "RUCIO_FAILED":
        prefix = 'dune_failed_transfers_display_'
    elif args.transferType == "RUCIO_AGGREGATE_FAILED":
        prefix = 'dune_failed_transfers_aggregates_display_'
    elif args.transferType == "TEST_MODE":
        prefix = 'dune_network_checkup_display_'
    elif args.transferType == "TEST_MODE_FAILED":
        prefix = 'dune_failed_network_checkup_display_'
    
    # current cache is in leeza directory
    path = '/dune/data/users/leeza/rucio_es_cache/'
    if end == "0":
        shutil.copyfile((path + str(day.year) + '/' + str(day.month).zfill(2) + '/' + prefix + day.strftime("%Y-%m-%d").replace('-', '_') + '_to_' + (day+delta).strftime("%Y-%m-%d").replace('-', '_') + '.json'), (os.getcwd() + '/out.json'))
    else:
        json_total = []
        dayfinal = datetime.strptime(end, "%Y-%m-%d")
        dayfinal = dayfinal + delta

        assert day < dayfinal, 'Start date must be before end date'

        # loop through dates
        while day < dayfinal:
            json_path = path + str(day.year) + '/' + str(day.month).zfill(2) + '/' + prefix + day.strftime("%Y-%m-%d").replace('-', '_') + '_to_' + (day+delta).strftime("%Y-%m-%d").replace('-', '_') + '.json'
            try:
                with open(json_path) as json_file:
                    json_add = json.load(json_file)
                json_total = json_total + json_add
                day = day + delta
            except json.JSONDecodeError: # if json file is incorrect (e.g. missing a bracket)
                print('JSON Error:', json_path)
                day = day + delta
            except FileNotFoundError: # if no data file exists (e.g. searching for a future date)
                print('No Data Available:', json_path)
                day = day + delta

        if not json_total: # if there is no data, write the filler NaN json
            json_final = json.dumps({'data': json_empty}, indent=2)
            with open(filename, 'w', encoding='utf-8') as json_file:
                json_file.write(json_final)
        else:
            json_final = json.dumps({'data': json_total}, indent=2)
            with open(filename, 'w', encoding='utf-8') as json_file:
                json_file.write(json_final)

# if frotend input is SAM Transfers
else:
    # current cache is in leeza directory
    path = '/dune/data/users/leeza/sam_es_cache/'
    if end == "0":
        shutil.copyfile((path + str(day.year) + '/' + str(day.month).zfill(2) + '/' + 'out_dune_' + day.strftime("%Y-%m-%d") + '_to_' + (day+delta).strftime("%Y-%m-%d") + '.json'), (os.getcwd() + '/out.json'))
    else:
        json_total = []
        dayfinal = datetime.strptime(end, "%Y-%m-%d")
        dayfinal = dayfinal + delta

        assert day < dayfinal, 'Start date must be before end date'

        # loop through dates
        while day < dayfinal:
            json_path = path + str(day.year) + '/' + str(day.month).zfill(2) + '/' + 'out_dune_' + day.strftime("%Y-%m-%d") + '_to_' + (day+delta).strftime("%Y-%m-%d") + '.json'
            try:
                with open(json_path) as json_file:
                    json_add = json.load(json_file)
                json_total = json_total + json_add['data']
                day = day + delta
            except json.JSONDecodeError: # if json file is incorrect (e.g. missing a bracket)
                print('JSON Error:', json_path)
                day = day + delta
            except FileNotFoundError: # if no data file exists (e.g. searching for a future date)
                print('No Data Available:', json_path)
                day = day + delta
        
        if not json_total: # if there is no data, write the filler NaN json
            json_final = json.dumps({'data': json_empty}, indent=2)
            with open(filename, 'w', encoding='utf-8') as json_file:
                json_file.write(json_final)
        else:
            json_final = json.dumps({'data': json_total}, indent=2)
            with open(filename, 'w', encoding='utf-8') as json_file:
                json_file.write(json_final)

        # json_final = json.dumps({'data': json_total}, indent=2)
        # with open(filename, 'w', encoding='utf-8') as json_file:
        #     json_file.write(json_final)
