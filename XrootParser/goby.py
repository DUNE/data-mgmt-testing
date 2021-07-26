import datetime
import os
import sys

day1 = datetime.datetime.strptime(sys.argv[1], "%Y-%m-%d")
delta = datetime.timedelta(days=1)
lastday = datetime.datetime.strptime(sys.argv[2], "%Y-%m-%d")

while day1 < lastday:
  command = "python3 Analyzer2test.py %s" %(datetime.datetime.strftime(day1,"%Y-%m-%d"))
  print(command)
  os.system(command)
  day1 += delta
  if day1 == lastday:
    break

