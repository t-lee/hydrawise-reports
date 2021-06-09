#!/usr/bin/env python3

import json
import time
import subprocess
import sys
import requests
import mysql.connector
from mysql.connector import errorcode

## TODOs ##
# - start time of the query should be based on the latest found records in the database instead of static 7 days
# - more exception handling
# - logging
# - improve read of json config files

def get_data(cred):

    time_end = int(time.time())
    ## minus one week
    time_start = time_end - 604800


    reports_URL = 'https://app.hydrawise.com/api/v2/reports?format=json&option=3&start=%d000&end=%d000&type=FLOW_METER_MEASUREMENT_TYPE&controller_id=%s' % (time_start, time_end, cred['controller_id'])
    auth_URL = 'https://app.hydrawise.com/api/v2/oauth/access-token'


    print('\n<<<<<<<<<<<<<< INIT: token request from access-token API  >>>>>>>>>>>>>>>>\n')
    r = requests.post(auth_URL, data=cred['api-payload'])
    print(r.status_code, r.reason, r.text)
    token = json.loads(r.text)

    print('\n<<<<<<<<<<<<<< curl / get token from api >>>>>>>>>>>>>>>>\n')
    cmd = [
    'curl',
    reports_URL,
    '-H',
    'Authorization: %s' % token['access_token']
    ]

    #print(cmd)

    command = subprocess.run(cmd, capture_output=True)
    return json.loads(command.stdout.decode("utf-8"))







def parse_data(data, cred):
    
    rc = insert_data( ( 3, 1622952045, 10, 27), cred )
    rc = insert_data( ( 2, 1622952045, 9, 22 ), cred )
    rc = insert_data( ( 1, 1622952044, 10, 30 ), cred )
    rc = insert_data( ( 3, 1622952045, 10, 27 ), cred )




def insert_data(data, cred):

    add_metric = (
            "INSERT IGNORE INTO hydrawise_flow_meter "
            "(zone, metric_timestamp, runtime, litres) "
            "VALUES (%s, %s, %s, %s) "
            )

    print(add_metric)
    print(data)

    try:
        db = mysql.connector.connect(**cred)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print("failed to connect to database server: %s" % err)

        return(1)

    cursor = db.cursor()

    cursor.execute(add_metric, data)
    db.commit()
    cursor.close()

    db.close()




if __name__ == "__main__":

    with open("./test-config.json") as json_file:
        cred = json.load(json_file)

    test = True
    test = False

    if test:
        hydrawise_data = get_data(cred['hydrawise'])
    else:
        with open("./test-data.json") as json_file:
            hydrawise_data = json.load(json_file)

    print('\nvvvvvvv Result: vvvvvvv\n')
    print(json.dumps(hydrawise_data, indent=4, sort_keys=True))
    print('\n^^^^^^^^^^^^^^^^^^^^^^^\n')

    parse_data(hydrawise_data, cred['mysql'])
