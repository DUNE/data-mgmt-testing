import Utils_multi, sys,datetime
from datetime import date,timedelta
import datetime

expt = "dune"
start_date = datetime.date(2021, 5,1)
start_range = start_date
end_date = datetime.date(2021, 5,10)
delta = datetime.timedelta(days=1)

if len(sys.argv) >= 3:
    start = sys.argv[1].split("-")
    end = sys.argv[2].split("-")
    
    start_date = date(int(start[0]), int(start[1]) ,int(start[2]))
    end_date = date(int(end[0]), int(end[1]) ,int(end[2]))
    print ("date range is ",start_date,end_date)
else:
    print ("expects YYYY-MM-DD YYYY-MM-DD for start and end dates, using default dates",start_date,end_date)

delta = timedelta(days=1)

if len(sys.argv) >=4:
    expt = sys.argv[3]
    print ("expt is ",expt)
else:
    print ("assuming you want expt==dune")
    expt = "dune"
start_range = start_date
while start_range < end_date:
    
  end_range = start_range + delta
  print (start_range,end_range,expt)
  Utils_multi.test(expt,start_range.strftime("%Y-%m-%d"),end_range.strftime("%Y-%m-%d"),100000)
  start_range += delta

