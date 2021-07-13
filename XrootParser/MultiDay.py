import Utils_multi
import datetime


start_date = datetime.date(2021, 5,1)
start_range = start_date
end_date = datetime.date(2021, 5,10)
delta = datetime.timedelta(days=1)
while start_range < end_date:
    
    end_range = start_range + delta
    Utils_multi.test(start_range.strftime("%Y-%m-%d"),end_range.strftime("%Y-%m-%d"),100000)
    start_range += delta

