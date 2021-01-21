#!/usr/bin/env python
import os
import sys
import datetime

def file_in(filename):
    f = open(filename)
    lines = f.readlines()
    f.close()
    return lines

def strip_newlines(lines):
    string = ""
    for line in lines:
        string += line.rstrip()
    return string

def file_out(lines, filename):
    f = open(filename, "w+")
    for line in lines:
        f.write(line + "\n")
    f.close()

directory_in = sys.argv[1]
directory_out = sys.argv[2]

files = []

for filename in os.listdir(directory_in):
    lines = file_in(directory_in + "/" + filename)
    files.append(strip_newlines(lines))
now = datetime.datetime.now()
f_out = directory_out + "/" + now.strftime("%Y_%m_%d_%H_%M_%S") + ".json"
file_out(files, f_out)
