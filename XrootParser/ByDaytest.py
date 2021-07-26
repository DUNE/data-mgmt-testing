import Utils2
import datetime
import sys
import getopt

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)

if __name__ == '__main__':

  if len(sys.argv) < 3:
    print (" need data range <first> <last> ")
    sys.exit(1)
  n = 1000000
  if len(sys.argv) >= 4:
    n = int(sys.argv[3])
#  Utils2.test(sys.argv[1],sys.argv[2],n)


start_date = datetime.datetime.strptime(sys.argv[1],"%Y-%m-%d")
#start_range = start_date
end_date = datetime.datetime.strptime(sys.argv[2],"%Y-%m-%d")
#delta = datetime.timedelta(days=1)
#while start_range < end_date:
    
#    end_range = start_range + delta
#    Utils2.test(start_range.strftime("%Y-%m-%d"),end_range.strftime("%Y-%m-%d"),100000)
#    start_range += delta



for single_date in daterange(start_date, end_date):
    Utils2.test(single_date.strftime("%Y-%m-%d"), (single_date+datetime.timedelta(days=1)).strftime("%Y-%m-%d"),n)
