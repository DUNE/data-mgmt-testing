from datetime import datetime
from dateutil.relativedelta import relativedelta

import os

'''TODO: Switch to using config file instead of hardcoding'''


cmd_path = "/srv/dune/data/users/dunepro/cache/data-mgmt-testing/ESClient/xroot_es_client.py"
# "/dune/data/users/leeza/data-mgmt-testing/XRootESClient/xroot_es_client.py"
sitename_path = "/srv/dune/data/users/dunepro/cache/data-mgmt-testing/ESClient/SiteNames.json"
# "/dune/data/users/leeza/data-mgmt-testing/XRootESClient/SiteNames.json"

today = datetime.now()
end_point = (today + relativedelta(days=-1)).strftime("%Y-%m-%d")
start_point = (today + relativedelta(weeks=-1)).strftime("%Y-%m-%d")

cmd_args = f"-S {start_point} -E {end_point} -D /srv/dune/data/users/dunepro/cache/sam_es_cache --sitename-file {sitename_path} --display-aggregate --output-for-display --simultaneous-pids 4"

full_cmd = f"/usr/bin/python3 {cmd_path} {cmd_args}"

os.system(full_cmd)
