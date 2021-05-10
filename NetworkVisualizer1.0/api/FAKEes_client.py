#!/usr/bin/env python3
import sys
import json
import pprint
from elasticsearch import Elasticsearch

output_file = "out.json"
y,m,d = sys.argv[2].split('/')

print("starting python")
print(sys.argv[0])
print(sys.argv[1])
print(sys.argv[2])
print(y,m,d)
#
#
# print(y,m,d)

f = open(output_file, "w")
f.write('{"name": "api","version": "0.0.0","dependencies": {"cookie-parser": "~1.4.4"} }')
f.close()
#
# # f2 = open("out.json", "r")
# # print(f2.read())
# # f2.close()
#
# print("done with test file")
# print("done with test file")
# print("done with test file")
# print("done with test file")
# print("done with test file")
# print("done with test file")
