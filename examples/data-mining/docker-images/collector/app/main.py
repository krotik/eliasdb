#!/usr/bin/env python3
#
# EliasDB - Data mining collector example
#
# Copyright 2020 Matthias Ladkau. All rights reserved.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

import schedule
import time
import requests
import json

ELIASDB_URL = "eliasdb1:9090"

requests.packages.urllib3.disable_warnings()

def job():
    global counter

    url = "https://devt.de"

    try:

        now = int(time.time())
        print("Running request for %s - timestamp: %s (%s)" %
            (url, now, time.strftime("%d-%m-%Y %H:%M:%S", time.gmtime(now))))

        r = requests.get(url)
        res_time = r.elapsed

        print ("    %s -> %s" % (url, res_time))

        result = {
            "key"     : str(now),
            "kind"    : "PingResult",
            "url"     : url,
            "success" : True,
            "result"  : str(res_time),
        }

    except Exception as e:
        print("Error: %s", e)

        result = {
            "key"     : str(now),
            "kind"    : "PingResult",
            "url"     : url,
            "success" : False,
            "result"  : str(e),
        }

    try:
        r = requests.post('https://%s/db/v1/graph/main/n' % ELIASDB_URL,
            json.dumps([result]),  verify=False)

        if r.status_code != 200:
            print("Could not store result: %s", r.text)

    except Exception as e:
        print("Error storing result: %s", e)


schedule.every(5).seconds.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
