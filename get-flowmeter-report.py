#!/usr/bin/env python3

import os
import json
import time
import subprocess
import sys
import requests
import re
import mysql.connector
from mysql.connector import errorcode

MAXZONES = 12

## TODOs ##
# - more exception handling
# - logging
# - improve read of json config files
# - into config: number of past days to query
# - count number of lines in the table before and after INSERT statements
# - log report of lines addded to DB and number of metrics found in api response
# - find out why the POST with python requests fails, while it works with curl
# - replace print() statements with verbose/warn/error methods and do logging
# - check for other usefull reports from hydrawise api and maybe implement them
# - write help function / add argparse
# - write README.md


def get_data(cred):
    ## now
    time_end = int(time.time())
    ## minus 365 days
    time_start = time_end - 86400*365


    reports_URL = 'https://app.hydrawise.com/api/v2/reports?format=json&option=3&start=%d000&end=%d000&type=FLOW_METER_MEASUREMENT_TYPE&controller_id=%s' % (time_start, time_end, cred['controller_id'])
    auth_URL = 'https://app.hydrawise.com/api/v2/oauth/access-token'


#    print('\n<<<<<<<<<<<<<< INIT: token request from access-token API  >>>>>>>>>>>>>>>>\n')
    r = requests.post(auth_URL, data=cred['api-payload'])
#    print(r.status_code, r.reason, r.text)
    token = json.loads(r.text)

#    print('\n<<<<<<<<<<<<<< curl / get token from api >>>>>>>>>>>>>>>>\n')
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

    zone_names = {}
    if not isinstance(data, list):
        print('data expected to be a list')
        return None

    db = connectDB(cred)
    if not db:
        return None

    i = 0
    for zone in data:
        ###print(zone)
        i += 1
        if not isinstance(zone, dict):
            print('data element %d expected to be a dict' % (i))
            continue

        ################################################################################
        ## checking now for presence and validity of zone name and zoneId ##############
        if not 'name' in zone:
            print('field "name" expected in dictionary of list element %d' % i)
            continue

        if not isinstance(zone['name'], str):
            print('field "name" of data element %d contains no string' % (i))
            continue

        zoneId = zone['name'].split(sep=':')[0]
        try:
            zoneId = int(zoneId)
        except ValueError as e:
            print('field "name" of data element %d contains no zone ID in the beginning' % (i))
            continue

        if zoneId > MAXZONES:
            print('field "name" of data element %d contains an invalid zone ID "%d". Only %d zones are supported' % (i, zoneId, MAXZONES))
            continue

        if zoneId in zone_names:
            if zone_names[zoneId] != zone['name']:
                print('duplicate zone IDs found for different zones.\n  %s\n  %s' % (zone_names[zoneId], zone['name']))
                continue
        else:
            zone_names[zoneId] = zone['name']



        #################################################################################
        ## checking now for presence and validity of data elements ######################
        if not 'data' in zone:
            print('field "data" expected in dictionary of zone: "%s"' % zone['name'])
            continue

        if not isinstance(zone['data'], list):
            print('field "data" of data element %d of zone "%s" contains no list' % (i, zone['name']))
            continue

        j = -1
        for datapoint in zone['data']:
            j += 1
            skip = False
            for key in ("note", "units", "x", "y"):
                if not key in datapoint:
                    print('zone ("{}") / data point ({}):    key "{}" not found. Skipping this data point'.format(zone['name'], j, key))
                    skip = True
                    break

            if skip:
                continue


            ###########################################
            ## extract runtime from data point ########
            if not isinstance(datapoint['note'], str):
                print('zone ("{}") / data point ({}):    content of key "note" is not a string'.format(zone['name'], j))
                continue

            runtime = extract_runtime(datapoint['note'])
            if runtime == None:
                print('zone ("{}") / data point ({}):    cannot extract the runtime from note: "{}". Will write Null to database.'.format(zone['name'], j, datapoint['note']))

            #######################################
            ## verify that the unit is litres #####
            if not isinstance(datapoint['units'], str):
                print('zone ("{}") / data point ({}):    content of key "units" is not a string'.format(zone['name'], j))
                continue

            if datapoint['units'] != "litres":
                print('zone ("{}") / data point ({}):    measured unit is not litres'.format(zone['name'], j))
                continue

            ################################################
            ## check that element 'x' is an integer ########
            if not isinstance(datapoint['x'], int):
                print('zone ("{}") / data point ({}):    content of key "x" is not an integer'.format(zone['name'], j))
                continue

            ############################################################################
            ## 'x' is a unixtimestamp with microseconds, which have to be removed ######
            x = int(datapoint['x'] / 1000)

            ##############################################
            ## check that element 'y' is an integer ######
            if not isinstance(datapoint['y'], int):
                print('zone ("{}") / data point ({}):    content of key "y" is not an integer'.format(zone['name'], j))
                continue

            if not insert_data( ( zoneId, x, x, runtime, datapoint['y'] ), db ):
                print("failed to write data to database: ({}, {}, {}, {})".format( zoneId, x, runtime, datapoint['y'] ) )

    closeDB(db)


def extract_runtime(string):
    p = re.compile("^Run time: (\d+) Minuten?$")
    r = p.search(string)
    if r != None:
        return(int(r.group(1))*60)

    p = re.compile("^Run time: (\d+) Sekunden?$")
    r = p.search(string)
    if r != None:
        return(int(r.group(1)))

    p = re.compile("^Run time: (\d+) Stunden?$")
    r = p.search(string)
    if r != None:
        return(int(r.group(1))*3600)

    return(None)


def connectDB(cred):
    try:
        db = mysql.connector.connect(**cred)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print("failed to connect to database server: %s" % err)

        return(None)
    return db

def closeDB(db):
    try:
        db.close()
    except Exception as e:
        print(e)


def insert_data(data, db):

    add_metric = (
            "INSERT IGNORE INTO hydrawise_flow_meter "
            "(zone, metric_timestamp, metric_datetime, runtime, litres) "
            "VALUES (%s, %s, from_unixtime(%s), %s, %s) "
            )

    #print(add_metric)
    #print(data)

    try:
        cursor = db.cursor()
        cursor.execute(add_metric, data)
        db.commit()
        cursor.close()

        return True

    except Exception as e:
        print(e)
        return False





if __name__ == "__main__":

    dir_path = os.path.dirname(os.path.realpath(__file__))
    with open("{}/test-config.json".format(dir_path)) as json_file:
        cred = json.load(json_file)

    test = False
    #test = True

    if not test:
        hydrawise_data = get_data(cred['hydrawise'])
    else:
        with open("./test-data.json") as json_file:
            hydrawise_data = json.load(json_file)

    #print('\nvvvvvvv Result: vvvvvvv\n')
    #print(json.dumps(hydrawise_data, indent=4, sort_keys=True))
    #print('\n^^^^^^^^^^^^^^^^^^^^^^^\n')

    parse_data(hydrawise_data, cred['mysql'])
