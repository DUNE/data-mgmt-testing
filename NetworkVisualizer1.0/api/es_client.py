import json
import os
import sys
import time
import threading
import queue
import re


from pathlib import Path

#ElasticSearch API is needed for interaction with Fermilab's ElasticSearch system
from elasticsearch import Elasticsearch

from datetime import datetime
from dateutil import parser as date_parser
from dateutil.relativedelta import relativedelta

import argparse as ap

#Based on Simplernerd's tutorial here: https://simplernerd.com/elasticsearch-scroll-python/
#Scrolls through all of the results in a given date range
#scroll is a generator that will step through all of the page results for a
#given index. scroll maintains its internal state and returns a new page of results
#each time it runs.
def scroll(es, idx, body, scroll, search_size):
	page = es.search(index=idx, body=body, scroll=scroll, size=search_size)
	scroll_id = page['_scroll_id']
	hits = page['hits']['hits']
	while len(hits):
		yield hits
		page = es.scroll(scroll_id=scroll_id, scroll=scroll)
		scroll_id = page['_scroll_id']
		hits = page['hits']['hits']


class RucioESClient():
	def __init__(self):
		self.debug = 3

		self.day_semaphore = 4
		self.day_lock = threading.Lock()

		self.time_lock = threading.Lock()

		#If true, will display timing information for different aspects
		#of the program
		self.display_timing = False

		#Stores timing data
		self.times = {
			"run_time" : 0,
			"io_time" : 0,
			"generator_input_time" : 0,
			"generator_output_time" : 0,
			"function_output_time" : 0,
			"elasticsearch_time" : 0,
			"longest_day" : None,
			"longest_day_time" : 0,
			"get_info_time" : 0,
			"get_err_time" : 0,
			"es_thread_count" : 0
		}

		#Variable for individual run arguments
		self.args = {}

		#Stores the output directory name
		self.dirname = ""

		self.client = None

		'''
		dune_transfers_display_YYYY....
		dune_transfers_aggregates_display_YYYY...
		dune_network_checkup_display_YYYY...
		dune_failed_transfers_display_YYYY...
		dune_failed_transfers_aggregates_display_YYYY...
		dune_failed_network_checkup_display_YYYY...
		'''
		self.file_info = {
			"successful_transfers" : {
				"file_format" : 'dune_transfers_display_{}_{}_{}_to_{}_{}_{}.json',
				"conditions"  : {
					"event_type" : r"transfer-done"
				},
				"restrictions" : {
					"name" : r"1gbtestfile-"
				},
				"min_condition_count" : 1,
				"max_restriction_count" : 0,
				"process_func" : self.transfer_success,
				"process_type" : "function"
			},
			"aggregate_successes" : {
				"file_format" : 'dune_transfers_aggregates_display_{}_{}_{}_to_{}_{}_{}.json',
				"conditions"  : {
					"event_type" : r"transfer-done"
				},
				"restrictions" : {
					"name" : r"1gbtestfile-"
				},
				"min_condition_count" : 1,
				"max_restriction_count" : 0,
				"process_func" : self.aggregate_success,
				"process_type" : "generator"
			},
			"checkup_successes" : {
				"file_format" : 'dune_network_checkup_display_{}_{}_{}_to_{}_{}_{}.json',
				"conditions"  : {
					"event_type" : r"transfer-done",
					"name" : r"1gbtestfile-"
				},
				"restrictions" : {
				},
				"min_condition_count" : 2,
				"max_restriction_count" : 0,
				"process_func" : self.checkup_success,
				"process_type" : "function"
			},
			"failed_transfers" : {
				"file_format" : 'dune_failed_transfers_display_{}_{}_{}_to_{}_{}_{}.json',
				"conditions"  : {
					"event_type" : r"transfer-failed",
					"event_type" : r"transfer-submission_failed"
				},
				"restrictions" : {
					"name" : r"1gbtestfile-"
				},
				"min_condition_count" : 1,
				"max_restriction_count" : 0,
				"process_func" : self.transfer_failed,
				"process_type" : "function"
			},
			"aggregate_failures" : {
				"file_format" : 'dune_failed_transfers_aggregates_display_{}_{}_{}_to_{}_{}_{}.json',
				"conditions"  : {
					"event_type" : r"transfer-failed",
					"event_type" : r"transfer-submission_failed"
				},
				"restrictions" : {
					"name" : r"1gbtestfile-"
				},
				"min_condition_count" : 1,
				"max_restriction_count" : 0,
				"process_func" : self.aggregate_failed,
				"process_type" : "generator"
			},
			"checkup_failures" : {
				"file_format" : 'dune_failed_network_checkup_display_{}_{}_{}_to_{}_{}_{}.json',
				"conditions"  : {
					"event_type" : r"transfer-failed",
					"event_type" : r"transfer-submission_failed",
					"name" : r"1gbtestfile-"
				},
				"restrictions" : {
				},
				"min_condition_count" : 2,
				"max_restriction_count" : 0,
				"process_func" : self.checkup_failed,
				"process_type" : "function"
			}
		}

	def get_err(self, transfer):
		start_time = datetime.now().timestamp()
		try:
			new_json = {
				"name" : transfer["name"],
				"source" : transfer["src-rse"],
				"destination" : transfer["dst-rse"]
			}
			if transfer["event_type"] == "transfer-failed":
				new_json["reason"] = "rx_error"
			else:
				new_json["reason"] = "tx_error"
			new_json["count"] = 1
			end_time = datetime.now().timestamp()
			self.time_lock.acquire()
			self.times["get_err_time"] += float(end_time - start_time)
			self.time_lock.release()
			return new_json
		except e:
			print(f"Error processing failed transfer. Exception: {e}")
			end_time = datetime.now().timestamp()
			self.time_lock.acquire()
			self.times["get_err_time"] += float(end_time - start_time)
			self.time_lock.release()
			return {"name" : "ERROR"}

	def get_speed(self, transfer):
		if transfer["duration"] == 0:
			return {}
		#Pulls request creation time
		c_time = transfer["created_at"]
		#Pulls the (transfer request?) submission time, the transfer start time,
		#and the transfer end time, as well as the file size
		sub_time = transfer["submitted_at"]
		start_time = transfer["started_at"]
		fin_time = transfer["transferred_at"]
		f_size = float(transfer["bytes"])

		#Places our relevant times into an array for processing
		time_arr = [c_time, sub_time, start_time, fin_time]
		len_arr = []

		#Finds the time differences for each set of times (creation to submission,
		#submission to starting, and transmission start to transmission end)
		#Initial time format is YYYY:M:DTH:M:SZ where T separates the year-month-day
		#portion from the hours-minutes-second portion and the Z denotes UTC
		for i in range(len(time_arr)-1):
			#Gets our times from the JSON's format into a workable form
			#First divides
			split_1 = time_arr[i].split()
			split_2 = time_arr[i + 1].split()
			#Splits the hour-minute-second portion into individual pieces
			#Note: In the future, with very long transmissions or transmissions
			#started around midnight we may need to account for days, but that seems
			#like an edge case that would be rare and unlikely to meaningfully affect results
			t_1 = split_1[1].split(':')
			t_2 = split_2[1].split(':')

			#Pulls the difference between each individual number set
			#The hours, minutes, and seconds being greater in the start
			#time than in the end time are accounted for later on
			h_diff = float(t_2[0]) - float(t_1[0])
			m_diff = float(t_2[1]) - float(t_1[1])
			s_diff = float(t_2[2]) - float(t_1[2])

			#Accounts for the hour being higher in the start time, which would
			#mean that the day had turned over at some point.
			if h_diff < 0:
				h_diff += 24

			#The difference in minutes and seconds being negative is accounted
			#for here. The hour difference (thanks to the code above) should
			#always be 0 or greater. If the minutes and seconds are different,
			#then the hours should always be greater than zero, so the overall
			#time will still be positive.
			tot_time = (h_diff * 60 + m_diff) * 60 + s_diff

			#Appends each time to the time length array
			len_arr.append(tot_time)

		#Calculates the transfer speed from the transfer start to end time and
		#the file size
		transfer_speed = f_size/len_arr[2]
		#Filters out transfers with abnormally short or long transfer times
		if len_arr[2] < 10.0 or len_arr[2] > 12 * 60 * 60:
			return {}
		#Fills our speed information dictionary for this JSON object
		info = {
			"creation_to_submission": len_arr[0],
			"submission_to_started": len_arr[1],
			"file_transfer_time": len_arr[2],
			"transfer_speed(MB/s)": f_size/len_arr[2]/1024/1024
		}
		return info

	def get_info(self, transfer):
		start_time = datetime.now().timestamp()
		try:
			speed_info = self.get_speed(transfer)
		except:
			end_time = datetime.now().timestamp()
			self.time_lock.acquire()
			self.times["get_info_time"] += float(end_time - start_time)
			self.time_lock.release()
			if self.debug_level > 2:
				print(f"Transfer caused error in get_info: {transfer}")
			return {}
		if speed_info == {}:
			end_time = datetime.now().timestamp()
			self.time_lock.acquire()
			self.times["get_info_time"] += float(end_time - start_time)
			self.time_lock.release()
			return {}
		new_json = {
			"name": transfer["name"],
			"source": transfer["src-rse"],
			"destination": transfer["dst-rse"],
			"file_size": int(transfer["file-size"]),
			"start_time": transfer["started_at"],
			"file_transfer_time": str(speed_info["file_transfer_time"]),
			"transfer_speed(MB/s)": str(round(speed_info["transfer_speed(MB/s)"],2)),
		}
		end_time = datetime.now().timestamp()
		self.time_lock.acquire()
		self.times["get_info_time"] += float(end_time - start_time)
		self.time_lock.release()
		return new_json

	def transfer_success(self, data):
		return json.dumps(self.get_info(data))

	def aggregate_success(self, day):
		xfer_matrix = []
		keys = []
		while True:
			input = yield
			if input["operation"] == "STORE":
				data = self.get_info(input["data"])
				if data == {}:
					continue
				if not data["source"] in keys:
					xfer_matrix.append([])
					for key in keys:
						xfer_matrix[-1].append([0,0])
					keys.append(data["source"])
					for i in range(len(xfer_matrix)):
						xfer_matrix[i].append([0, 0])
				if not data["destination"] in keys:
					xfer_matrix.append([])
					for key in keys:
						xfer_matrix[-1].append([0, 0])
					keys.append(data["destination"])
					for i in range(len(xfer_matrix)):
						xfer_matrix[i].append([0, 0])
				#process input["data"] in whatever way's needed
				size = int(data["file_size"])
				transfer_time = float(data["file_transfer_time"])

				idx1 = keys.index(data["source"])
				idx2 = keys.index(data["destination"])

				xfer_matrix[idx1][idx2][0] += size/1045876
				xfer_matrix[idx1][idx2][1] += transfer_time

			elif input["operation"] == "GET":
				break

		for i in range(len(keys)):
			for j in range(len(keys)):
				if xfer_matrix[i][j][0] == 0:
					continue
				#We're using the earliest search date as our "transfer date" since
				#we don't actually have a date to associate with the summarized
				#transfers
				y = day.strftime("%Y")
				m = day.strftime("%m")
				d = day.strftime("%d")
				#We then format our data and append it to a list to be written
				#in bulk
				new_entry = {
					"name" : f"{keys[i]}_to_{keys[j]}",
					"source" : keys[i],
					"destination" : keys[j],
					"file_size" : xfer_matrix[i][j][0]*1048576,
					"start_time" : f"{y}-{m}-{d} 00:00:01",
					"file_transfer_time" : xfer_matrix[i][j][1],
					"transfer_speed(MB/s)" : round(float(xfer_matrix[i][j][0])/float(xfer_matrix[i][j][1]),2)
				}
				yield new_entry

		yield "FINISHED"

	def checkup_success(self, data):
		return json.dumps(self.get_info(data))

	def transfer_failed(self, data):
		return json.dumps(self.get_err(data))

	def aggregate_failed(self, day):
		xfer_matrix = [[],[]]
		keys = []

		while True:
			input = yield
			if input["operation"] == "STORE":
				data = self.get_err(input["data"])
				if data["source"] not in keys:
					xfer_matrix[0].append([])
					xfer_matrix[1].append([])
					for key in keys:
						xfer_matrix[0][-1].append(0)
						xfer_matrix[1][-1].append(0)
					keys.append(data["source"])
					for i in range(len(xfer_matrix[0])):
						xfer_matrix[0][i].append(0)
						xfer_matrix[1][i].append(0)
				if data["destination"] not in keys:
					xfer_matrix[0].append([])
					xfer_matrix[1].append([])
					for key in keys:
						xfer_matrix[0][-1].append(0)
						xfer_matrix[1][-1].append(0)
					keys.append(data["destination"])
					for i in range(len(xfer_matrix[0])):
						xfer_matrix[0][i].append(0)
						xfer_matrix[1][i].append(0)

				if data["reason"] == "tx_error":
					idx0 = 0
				else:
					idx0 = 1
				idx1 = keys.index(data["source"])
				idx2 = keys.index(data["destination"])

				xfer_matrix[idx0][idx1][idx2] += 1

			elif input["operation"] == "GET":
				break

		for i in range(len(keys)):
			for j in range(len(keys)):
				if xfer_matrix[0][i][j] == 0:
					continue
				#Format is slightly different, but we're still formatting
				#data from our matrices and preparing to write it to file
				new_entry = {
					"name" : f"{keys[i]}_to_{keys[j]}",
					"source" : keys[i],
					"destination" : keys[j],
					"reason" : "tx_error",
					"count" : xfer_matrix[0][i][j]
					}
				yield new_entry

				if xfer_matrix[1][i][j] == 0:
					continue
				new_entry["reason"] = "rx_error"
				new_entry["count"] = xfer_matrix[1][i][j]
				yield new_entry

		yield "FINISHED"

	def checkup_failed(self, data):
		return json.dumps(self.get_err(data))

	def check_args(self):
		#Defaults only applies to optional arguments
		default_optionals = {
			"debug_level" : 3,
			"show_timing" : False,
			"dirname" : f"{Path.cwd()}/cache",
			"es_cluster" : "https://fifemon-es.fnal.gov",
			"search_size" : 7500,
			"simultaneous_days" : 4,
			#end_date assignment is handled elsewhere
			"end_date"  : "0",
			"force_overwrite" : False
			}


		keys = self.args.keys()

		required_args = ["start_date"]

		missing = []

		for arg in required_args:
			if not arg in keys:
				missing.append(arg)

		if not len(missing) == 0:
			print(f"Error: Missing the following required arguments:\n{' '.join(arg for arg in missing)}")

		missing = []

		for arg in default_optionals:
			if not arg in keys:
				self.args[arg] = default_optionals[arg]

		self.day_semaphore = int(self.args["simultaneous_days"])
		self.client = Elasticsearch([self.args["es_cluster"]])
		self.show_timing = self.args["show_timing"]

	def day_overseer(self, day, filetypes):
		start_time = datetime.now().timestamp()
		'''divide each day into six hour chunks
		(gives us 4 threads per day for faster data pull for all time increments.
		Also convenient number since processing a week of data at once would lead to 28 total threads,
		which is manageable by most systems (with limited RAM being the most prominent concern at that point))'''

		y = day.strftime("%Y")
		m = day.strftime("%m")
		d = day.strftime("%d")
		index = f"rucio-transfers-v0-{y}.{m}"

		#Attempts to limit memory usage by setting a maximum queue size. Number based on limited testing and should probably be changed at some point
		data_queue = queue.Queue(maxsize=int(float(self.args["search_size"]) * 6.0 / float(self.args["simultaneous_days"])))

		hour = 0
		#Make sure hour_step is always a factor of 24 or may break things
		hour_step = 12
		thread_list = []
		'''for each chunk
			add es_worker thread to list with args passed
			generate query for given thread
			make es_worker thread with index, queue, and query'''
		while hour < 24:
			es_template = {
				"query" : {
					"bool" : {
						"filter" : {
							"range" : {
								"@timestamp" : {
									"gte" : f"{y}-{m}-{d}T{hour:02}:00:00",
									"lte" : f"{y}-{m}-{d}T{(hour + hour_step - 1):02}:59:59"
								}
							}
						},
						"should" : [
							{
								"match": {
									"event_type" : "transfer-failed"
								},
							},
							{
								"match": {
									"event_type" : "transfer-submission_failed"
								}
							},
							{
								"match": {
									"event_type" : "transfer-done"
								}
							}
						],
						"minimum_should_match" : 1
					}
				}
			}

			new_thread = threading.Thread(target=self.es_worker, args=(index, data_queue, es_template, day), daemon=True)
			thread_list.append(new_thread)

			if self.debug_level > 3:
				print(f"Hour in day thread for {day} is {hour}")

			hour += hour_step


		'''start all threads and data_processor with index, day, filetypes, queue, and thread list'''

		for t in thread_list:
			t.start()

		data_processor_thread = threading.Thread(target=self.data_processor, args=(day, filetypes, data_queue, thread_list), daemon=True)

		data_processor_thread.start()

		for t in thread_list:
			t.join()

		data_processor_thread.join()

		self.day_lock.acquire()
		self.day_semaphore += 1
		self.day_lock.release()

		end_time = datetime.now().timestamp()
		day_time = float(end_time - start_time)
		self.time_lock.acquire()
		if self.times["longest_day_time"] == None or self.times["longest_day_time"] < day_time:
			self.times["longest_day_time"] = day_time
			self.times["longest_day"] = day.strftime("%Y-%m-%d")
		self.time_lock.release()

	def es_worker(self, index, data_queue, query, day):
		start_time = datetime.now().timestamp()
		'''Difference from original: Instead of writing directly, sends data to data processing queue'''

		try:
			for data in scroll(self.client, index, query, "15m", self.args["search_size"]):
				if self.debug_level > 3:
					print(f"Got scroll of size {len(data)} for {day}")
				for entry in data:
					data_queue.put(entry["_source"])
		except Exception as e:
			if self.debug_level > 1:
				print(f"Unhandled exeption in es_worker on day {day}")
			if self.debug_level > 2:
				print(f"Exception: {repr(e)}")

		end_time = datetime.now().timestamp()
		self.time_lock.acquire()
		self.times["elasticsearch_time"] += float(end_time - start_time)
		self.times["es_thread_count"] += 1
		self.time_lock.release()

	def data_processor(self, day, filetypes, data_queue, thread_list):
		'''set up summary matrices
		set up file objects
			-Check which file objects needed here. Have checks later for writing
			(basically, is the file name blank) since processing overhead time will
			be miniscule compared to overhead time for ES response'''

		if self.debug_level > 3:
			print(f"Starting data processor thread for day {day} with required filetypes {filetypes}")

		y1 = day.strftime("%Y")
		m1 = day.strftime("%m")
		d1 = day.strftime("%d")

		next_day = day + relativedelta(days=+1)

		y2 = next_day.strftime("%Y")
		m2 = next_day.strftime("%m")
		d2 = next_day.strftime("%d")


		generators = {}
		files = {}
		counts = {}

		for key in filetypes:
			#Initializes all generators
			if self.file_info[key]["process_type"] == "generator":
				generators[key] = self.file_info[key]["process_func"](day)
				next(generators[key])
			dir_path = f"{self.args['dirname']}/{day.year}/{day.month:02}"
			fname = self.file_info[key]["file_format"].format(y1, m1, d1, y2, m2, d2)
			counts[key] = 0
			files[key] = open(f"{dir_path}/{fname}", "w+")
			files[key].write('[\n')

		'''
		List of filenames:
		dune_transfers_display_YYYY....
		dune_transfers_aggregates_display_YYYY...
		dune_network_checkup_display_YYYY...
		dune_failed_transfers_display_YYYY...
		dune_failed_transfers_aggregates_display_YYYY...
		dune_failed_network_checkup_display_YYYY...
		'''

		#Spins until all elasticsearch threads have been shut down or
		while any([t.is_alive() for t in thread_list]) or not data_queue.empty():
			if data_queue.empty():
				time.sleep(0.1)
				continue

			next_data = data_queue.get()
			data_queue.task_done()

			for key in filetypes:
				con_count = 0
				res_count = 0
				for con in self.file_info[key]["conditions"].keys():
					if next_data[con].find(self.file_info[key]["conditions"][con]) != -1:
						con_count += 1
				for res in self.file_info[key]["restrictions"].keys():
					if next_data[res].find(self.file_info[key]["restrictions"][res]) != -1:
						res_count += 1
				if res_count <= self.file_info[key]["max_restriction_count"] and con_count >= self.file_info[key]["min_condition_count"]:
					counts[key] += 1
					if self.file_info[key]["process_type"] == "generator":
						start_time = datetime.now().timestamp()
						msg = {
							"data" : next_data,
							"operation" : "STORE"
						}
						generators[key].send(msg)
						end_time = datetime.now().timestamp()
						self.time_lock.acquire()
						self.times["generator_input_time"] += float(end_time - start_time)
						self.time_lock.release()
					elif self.file_info[key]["process_type"] == "function":
						start_time = datetime.now().timestamp()
						res = self.file_info[key]["process_func"](next_data)
						if res == {}:
							continue
						write_start = datetime.now().timestamp()
						files[key].write(f"{res},\n")
						end_time = datetime.now().timestamp()
						self.time_lock.acquire()
						self.times["function_output_time"] += float(end_time - start_time)
						self.times["io_time"] += float(end_time - write_start)
						self.time_lock.release()


			'''process data
				-Write directly where appropriate
				-Modify matrices where appropriate
				-Handle differentiation between successes and failures here by event type
				-Handle differentiation between normal and network test here
					-By handling both differentiation types here, we can use a single "should' query
					and process events individually on our end rather than making multiple queries or clients'''

		for key in filetypes:
			if self.file_info[key]["process_type"] == "generator":
				start_time = datetime.now().timestamp()
				data = generators[key].send({"operation" : "GET"})
				while data != "FINISHED":
					write_start = datetime.now().timestamp()
					files[key].write(f"{json.dumps(data)},\n")
					end_time = datetime.now().timestamp()
					self.time_lock.acquire()
					self.times["io_time"] += float(end_time - write_start)
					self.time_lock.release()
					data = generators[key].send({"operation" : "GET"})
				end_time = datetime.now().timestamp()
				self.time_lock.acquire()
				self.times["generator_output_time"] += float(end_time - start_time)
				self.time_lock.release()

		start_time = datetime.now().timestamp()
		for key in filetypes:
			if counts[key] == 0:
				files[key].write("]")
			else:
				#Handles trailing commas on last line
				files[key].seek(files[key].tell()-2)
				files[key].write("\n]")
				files[key].close()
		end_time = datetime.now().timestamp()
		self.time_lock.acquire()
		self.times["io_time"] += float(end_time - start_time)
		self.time_lock.release()

		if self.debug_level > 3:
			print(f"Data processor for day {day} ended")

	def show_timing_info(self):
		print("")
		print("===========")
		print("Timing Info")
		print("===========")
		print(f"Overall run time: {round(self.times['run_time'], 3)} seconds")
		print(f"Average Elasticsearch thread time: {round(self.times['elasticsearch_time']/self.times['es_thread_count'], 3)} seconds")
		print(f"Total number of Elasticsearch threads: {self.times['es_thread_count']}")
		print(f"Simultaneous days: {self.args['simultaneous_days']}")
		print(f"File IO Time: {round(self.times['io_time'], 3)} seconds")
		print(f"Generator input processing time: {round(self.times['generator_input_time'], 3)} seconds")
		print(f"Generator output processing time: {round(self.times['generator_output_time'], 3)} seconds")
		print(f"Processing function time: {round(self.times['function_output_time'])} seconds")
		print(f"Total \"get_info\" time: {round(self.times['get_info_time'], 3)} seconds")
		print(f"Total \"get_err\" time: {round(self.times['get_err_time'], 3)} seconds")
		print(f"Longest single day processing time: {round(self.times['longest_day_time'], 3)} seconds for day {self.times['longest_day']}")
		print("")

		'''self.times = {
			"run_time" : 0,
			"io_time" : 0,
			"generator_input_time" : 0,
			"generator_output_time" : 0,
			"function_output_time" : 0,
			"elasticsearch_time" : 0,
			"longest_day" : None,
			"longest_day_time" : 0,
			"get_info_time" : 0,
			"get_err_time" : 0,
			"es_thread_count" : 0
		}'''


	def main(self, args):
		#Start time for timing info
		start_time = datetime.now().timestamp()

		#Set up arguments and check if defaults needed
		self.args = dict(args)
		self.check_args()

		self.debug_level = int(self.args["debug_level"])

		if self.args["end_date"] == "0":
			self.args["end_date"] = date_parser.parse(self.args["start_date"]).strftime("%Y-%m-%d")
		if self.debug > 3:
			print(f"Args recieved: {self.args}")

		#Code to go day by day for output files as expected by other programs associated
		#with this project.
		curr_date = date_parser.parse(self.args["start_date"])
		target_date = date_parser.parse(self.args["end_date"]) + relativedelta(days=+1)

		if curr_date > target_date:
			temp_date = target_date
			target_date = curr_date
			curr_date = temp_date

		overseer_threads = []

		while (target_date - curr_date).days > 0:
			y = curr_date.strftime("%Y")
			m = curr_date.strftime("%m")
			d = curr_date.strftime("%d")

			if curr_date > datetime.now() + relativedelta(days=+1):
				break

			index = f"rucio-transfers-v0-{y}.{m}"

			if not self.client.indices.exists(index=index):
				curr_date += relativedelta(days=+1)
				if self.args["debug"] >= 2:
					print(f"Error: index {index} does not exist in cluster but is needed for date {curr_date}. Skipping date.")
				continue

			self.day_lock.acquire()
			if self.day_semaphore > 0:
				self.day_semaphore -= 1
			else:
				self.day_lock.release()
				continue
			self.day_lock.release()

			if self.debug_level > 3:
				print(f"Starting thread for day {curr_date}")

			#Makes sure that every step of the path from the provided directory onwards exists
			#and then sets self.dirname
			if not Path(self.args['dirname']).is_dir():
				Path(self.args['dirname']).mkdir(exist_ok=True)
			if not Path(f"{self.args['dirname']}/{curr_date.year}").is_dir():
				Path(f"{self.args['dirname']}/{curr_date.year}").mkdir(exist_ok=True)
			if not Path(f"{self.args['dirname']}/{curr_date.year}/{curr_date.month:02}").is_dir():
				Path(f"{self.args['dirname']}/{curr_date.year}/{curr_date.month:02}").mkdir(exist_ok=True)

			required_files = []


			y_next, m_next, d_next = (curr_date + relativedelta(days=+1)).strftime("%Y-%m-%d").split("-")

			#Checks which files need to be written
			for key in self.file_info.keys():
				dir_path = f"{self.args['dirname']}/{curr_date.year}/{curr_date.month:02}"
				fname = self.file_info[key]["file_format"].format(y, m, d, y_next, m_next, d_next)
				if self.debug_level > 3:
					print(f"Checking for file {dir_path}/{fname}")
				if not Path(f"{dir_path}/{fname}").exists() or self.args["force_overwrite"]:
					required_files.append(key)

			if self.debug_level > 3:
				print(f"Required files: {required_files}")

			if len(required_files) == 0:
				curr_date += relativedelta(days=+1)
				self.day_lock.acquire()
				self.day_semaphore += 1
				self.day_lock.release()
				continue

			overseer = threading.Thread(target=self.day_overseer, args=(curr_date, required_files,), daemon=True)
			overseer_threads.append(overseer)
			overseer_threads[-1].start()

			curr_date += relativedelta(days=+1)

		#Joins all overseer threads
		for t in overseer_threads:
			t.join()

		end_time = datetime.now().timestamp()
		self.time_lock.acquire()
		self.times["run_time"] += float(end_time - start_time)
		self.time_lock.release()

		if self.show_timing:
			self.show_timing_info()

if __name__ == "__main__":

	today = datetime.today()

	parser = ap.ArgumentParser()
	parser.add_argument('-S', '--start', dest="start_date", default=today.strftime("%Y-%m-%d"), help="The earlest date to search for matching transfers. Defaults to today's date. Must be in form yyyy-mm-dd")
	parser.add_argument('-E', '--end', dest="end_date", default="0", help="The latest date to search for matching transfers. Defaults to the same value as the start date, giving a 1 day search. Must be in form yyyy-mm-dd")
	parser.add_argument('-C', '--cluster', dest='es_cluster', default="https://fifemon-es.fnal.gov", help="Specifies the Elasticsearch cluster to target")
	parser.add_argument('-D', '--directory', dest='dirname', default=f"{Path.cwd()}/cache", help="Sets the cached searches directory")
	parser.add_argument('-Z', '--search-size', dest="search_size", default=7500, help="Number of results returned from Elasticsearch at once.")
	parser.add_argument('--debug-level', dest='debug_level', default=3, help="Determines which level of debug information to show. 1: Errors only, 2: Warnings and Errors, 3: Basic process info, 4: Advanced process info")
	parser.add_argument('--force-overwrite', dest='force_overwrite', action='store_true', help="Sets whether existing files will be overwritten. Only advised for regularly running backend systems and maintenance, not live users.")
	parser.add_argument('--simultaneous-days', default=4, help="Defines how many days the client will attempt to handle simultaneously. Advise keeping low to avoid timeout errors. Defaults to 4.")
	parser.add_argument('--show-timing', action='store_true', help="Shows timing information if set")

	args = vars(parser.parse_args())

	client = RucioESClient()

	client.main(args)
