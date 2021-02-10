import string,datetime,dateutil

from datetime import date,timezone,datetime
from dateutil import parser

def isofixer(stamp):
  #stamp = stamp.replace("T"," ")
  stamp = stamp.replace("Z","+00.00")
  return stamp

def human2number(stamp):
  parsed = parser.isoparse(stamp)
  #print ( parsed.timestamp())
  return parsed.timestamp()

def number2human(stamp):
    
    t = datetime.fromtimestamp(stamp,tz=timezone.utc)
    return t.isoformat() + 'Z'
  
def __main__():
    test = "2021-02-01T01:59:00.804Z"
    print (test,human2number(test),number2human(human2number(test)))
