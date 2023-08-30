import argparse

import math
import datetime
import time
import json
import csv
import subprocess
import os
import calendar
from datetime import datetime
from calendar import monthrange


from influxdb import InfluxDBClient
import locale
from locale import (
    LC_ALL,
    LC_NUMERIC,
    setlocale,
    getlocale,
    getdefaultlocale,
    currency
    )
from locale import format_string as lformat

setlocale(LC_ALL, 'sv_SE.utf8')

USER = ''
PASSWORD = ''
DBNAME = 'evmeters'

months = ['', 'januari', 'februari', 'mars', 'april', 'maj', 'juni', 'juli',
          'augusti', 'september', 'oktober', 'november', 'december' ]


def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)  # this will never fail
    return next_month - datetime.timedelta(days=next_month.day)

def result_to_kWh(result, value, id):
    kWh = result.get_points()
    # print("Result: {0}".format(result))
    for item in kWh:
        print("%f" % (item['max_min']))
        #return item[value]
    return 11

def download_evse_config(sheetid, pageid):
    url = "curl -s \"https://docs.google.com/spreadsheets/d/%s/gviz/tq?tqx=out:csv&gid=%s\" -o evse.csv" % (sheetid, pageid)
    # print(url)
    os.system(url)
    #subprocess.run(["curl", url, "-v", "-o", "evse.csv"])
    return

def main(host, port, year, month, tariff, sheetid, pageid):

    if ((sheetid != "") and (pageid != "")):
        download_evse_config(sheetid, pageid)
    # download_evse_config(sheetid, pageid)

    # Open the configuration file
    with open("evse.csv", "r") as evseConfig:
        #Set up CSV reader and process the header
        csvReader = csv.reader(evseConfig)
        header = next(csvReader)
        ppidIndex = header.index("PPlatsId")
        brukareIndex = header.index("Brukare")
        epostIndex = header.index("Epost")
        onIndex = header.index("Objektsnummer")

        ppidList = []
        onList = []
        for row in csvReader:
            ppid    = row[ppidIndex]
            brukare = row[brukareIndex]
            epost   = row[epostIndex]
            on      = row[onIndex]
            ppidList.append(ppid)
            onList.append(on)

    now = datetime.today()
    days_in_month = monthrange(int(year), int(month))[1]
    print("days_in_month: %d" % days_in_month)


    billing_year = int(year)
    billing_month = int(month)+4
    if billing_month > 12:
        billing_month = billing_month%12
        billing_year = billing_year+1
    billing_date_str_from = '%d-%02d-01' % (billing_year,billing_month)
    billing_date_str_tom = '%d-%02d-%02d' % (billing_year,billing_month,calendar.monthrange(int(billing_year),int(billing_month))[1])
    billing_date_from = datetime.strptime(billing_date_str_from, '%Y-%m-%d')
    billing_date_tom = datetime.strptime(billing_date_str_tom, "%Y-%m-%d")

    last_day_string = "%s-%s-%d%s" % (year, month, calendar.monthrange(int(year),int(month))[1],'T23:59:00Z')

    client = InfluxDBClient(host, port, USER, PASSWORD, DBNAME)
    client.switch_database(DBNAME)

    query = 'SELECT max("energy")-min("energy") FROM "evmeters" WHERE time >= 1633039200000ms and time <= 1635721199999ms GROUP BY id'

    result = client.query(query, database=DBNAME)
    # print("Result: {0}".format(result))

    #last_day = str(last_day_of_month(datetime.date(int(year), int(month), 1)))

    #last_day_string = "%s%s" % (last_day, 'T23:59:00Z')

    billing_report_file = "7729-EVSE2206-%s-%s.csv" % (year, month)
    brf = open(billing_report_file, "w")
    brf.write("Hyresid; Datum fr.o.m; Datum t.o.m; Artikel 1; Belopp; Avitext\n")

    s_query = "SELECT max(\"energy\")-min(\"energy\") FROM \"evmeters\" WHERE time >= '%s-%s-01' and time <= '%s' GROUP BY id " % (year, month, last_day_string)
    #print(s_query)
    result = client.query(s_query, database=DBNAME)
    # print("Result: {0}".format(result))

    # per id loop
    i=0
    for idi in ppidList:
        print("idi: %s" % idi)

    # per day loop
    for day_idx in range(1, days_in_month+1):
        #print(day_idx)
        for hour_idx in range(24):
            #print("%d-%d" % (day_idx, hour_idx))
            date_start = "%s-%s-%02d %02d:00:00" % (year, month, day_idx, hour_idx)
            date_end = "%s-%s-%02d %02d:59:59" % (year, month, day_idx, hour_idx)
            ts_s = time.mktime(time.strptime(date_start, '%Y-%m-%d %H:%M:%S'))
            ts_e = time.mktime(time.strptime(date_end, '%Y-%m-%d %H:%M:%S'))

            #ts_e = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

            s_query = "SELECT max(\"energy\")-min(\"energy\") FROM \"evmeters\" WHERE time >= %d000ms and time <= %d000ms GROUP BY id" % (ts_s, ts_e)
            print(s_query)
            result = client.query(s_query, database=DBNAME)

            for idi in ppidList:
                tagfilter = {}
                tagfilter['id'] = idi
                q_res = result.get_points(tags=tagfilter)

                for item in q_res:
                    kWh = item['max_min']
                    print("%d-%d [%s]: %f" % (day_idx, hour_idx, idi, kWh))
                    #brf.write("7729-%05d;%s;%s;ELM;%s;%s kWh tariff 22-06 %s p-plats %s\n" % (int(onList[i]),billing_date_str_from,billing_date_str_tom,lformat('%.2f', tariff*kWh), lformat('%.2f', kWh), months[int(month)], idi))
                i=i+1
    exit()


def parse_args():
    """Parse the args."""
    parser = argparse.ArgumentParser(
        description='Report_gen for InfluxDB')
    parser.add_argument('--host', type=str, required=False,
                        default='192.168.1.6',
                        help='hostname influxdb http API')
    parser.add_argument('--port', type=int, required=False, default=8086,
                        help='port influxdb http API')
    parser.add_argument('--year', type=str, required=False,
                        default='2019',
                        help='year of the queried period')
    parser.add_argument('--month', type=str, required=False, default='08',
                        help='month of the queried period')
    parser.add_argument('--tariff', type=float, required=False, default=1.48,
                        help='tariff to use')
    parser.add_argument('--sheetid', type=str, required=False, default="",
                        help='Google Docs sheetid to use')
    parser.add_argument('--pageid', type=str, required=False, default="",
                        help='Google Docs sheet pageid to use')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(host=args.host, port=args.port, year=args.year, month=args.month, tariff=args.tariff, sheetid=args.sheetid, pageid=args.pageid)
