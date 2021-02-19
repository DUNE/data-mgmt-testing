import Utils
import datetime


start_date = datetime.date(2021, 2, 8)
start_range = start_date
end_date = datetime.date(2021, 2, 9)
delta = datetime.timedelta(days=1)
while start_range <= end_date:
    
    end_range = start_range + delta
    Utils.test(start_range,end_range,1)
    start_range += delta
