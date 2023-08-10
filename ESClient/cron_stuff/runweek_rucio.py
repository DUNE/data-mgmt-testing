from datetime import datetime
from dateutil.relativedelta import relativedelta

import os

'''TODO: Switch to using config file instead of hardcoding'''


cmd_path = "/srv/dune/data/users/dunepro/dunedatamap/data-mgmt-testing/ESClient/rucio_es_client.py"
# "/dune/data/users/leeza/cron_stuff/temp_scripts/es_client.py"

today = datetime.now()
end_point = (today + relativedelta(days=-1)).strftime("%Y-%m-%d")
start_point = (today + relativedelta(weeks=-1)).strftime("%Y-%m-%d")

cmd_args = f"-E {end_point} -D /srv/dune/data/users/dunepro/dunedatamap/rucio_es_cache --simultaneous-days 4 --debug-level 3 --search-size 10000 --show-timing"

full_cmd = f"/usr/bin/python3 {cmd_path} -S {start_point} {cmd_args} --force-overwrite"

os.system(full_cmd)

# start_point = (today + relativedelta(months=-2)).strftime("%Y-%m-%d")

# full_cmd = f"/usr/bin/python3 {cmd_path} -S {start_point} {cmd_args}"

# os.system(full_cmd)
