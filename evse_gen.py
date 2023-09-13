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
import xlsxwriter


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
DBNAME2 = 'pricewatch'


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

def download_csv_data(sheetid, pageid, file):
    url = "curl -s \"https://docs.google.com/spreadsheets/d/%s/gviz/tq?tqx=out:csv&gid=%s\" -o %s" % (sheetid, pageid, file)
    # print(url)
    os.system(url)
    #subprocess.run(["curl", url, "-v", "-o", "evse.csv"])
    return

def main(host, port, year, month, tariff, sheetid, pageid, pricepageid):

    elnat=''
    elskatt=''
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

   # open billing xls
    billing_report_file_xslx = "7729-EVSE-%s-%s.xlsx" % (year, month)
    brf_evse_book = xlsxwriter.Workbook(billing_report_file_xslx, {'strings_to_numbers': True})
    brf_sheet = brf_evse_book.add_worksheet('BRF')
    brf_sheet.write('A1', 'Objektsnummer')
    brf_sheet.write('B1', 'Fr.o.m')
    brf_sheet.write('C1', 'T.o.m')
    brf_sheet.write('D1', 'Typ')
    brf_sheet.write('E1', 'Belopp i kronor')
    brf_sheet.write('F1', 'Avitext')
    format_text = brf_evse_book.add_format()
    format_text.set_num_format('@') # @ - This is text format in excel
    format_date = brf_evse_book.add_format({'num_format': 'yyyy-mm-dd'})

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
        costList = []
        row_num=1
        for row in csvReader:
            ppid    = row[ppidIndex]
            brukare = row[brukareIndex]
            epost   = row[epostIndex]
            on      = row[onIndex]
            ppidList.append(ppid)
            onList.append(on)
            costList.append(float(0.0))
            brf_sheet.write(row_num, 0, '7729-%05d' % (int(row[onIndex])), format_text)
            #brf_sheet.write(row_num, 0, row[onIndex])
            brf_sheet.write_datetime(row_num, 1, billing_date_from, format_date)
            brf_sheet.write_datetime(row_num, 2, billing_date_tom, format_date)
            brf_sheet.write(row_num, 3, "ELM")
            row_num=row_num+1

    if (pricepageid != ""):
        download_csv_data(sheetid, pricepageid, "price.csv")

    my = "%s%s" % (year, month)
    # Open the price data
    with open("price.csv", "r") as priceConfig:
        csvPrice = csv.reader(priceConfig)
        header = next(csvPrice)
        datumIdx = header.index("Datum")
        leveransIdx = header.index("Leverans")
        elnatIdx = header.index("Elnat")
        energisIdx = header.index("Energiskatt")
        totaltIdx = header.index("Totalt")
        totaltmIdx = header.index("Totalt_ink_moms")


        priceDateList = []
        priceTotalList = []
        for row in csvPrice:
            # get cost for elnät and energiskatt
            if (my == row[datumIdx]):
                elnat = row[elnatIdx]
                elskatt = row[energisIdx]
            #print("my=%s elnät: %s, elskatt: %s row=%s" % (my,elnat, elskatt,row[datumIdx]))

            datep = row[datumIdx]
            totalp = (float)(row[totaltIdx]) / 100.0
            priceDateList.append(datep)
            priceTotalList.append(totalp)
            # print("%s %s" % (datep, totalp))

    print("elnät: %s, elskatt: %s" % (elnat, elskatt))

    now = datetime.today()
    days_in_month = monthrange(int(year), int(month))[1]
    print("days_in_month: %d" % days_in_month)



    client = InfluxDBClient(host, port, USER, PASSWORD, DBNAME)
    client.switch_database(DBNAME)
    client2 = InfluxDBClient(host, port, USER, PASSWORD, DBNAME2)
    client2.switch_database(DBNAME2)

    query = 'SELECT max("energy")-min("energy") FROM "evmeters" WHERE time >= 1633039200000ms and time <= 1635721199999ms GROUP BY id'

    result = client.query(query, database=DBNAME)
    # print("Result: {0}".format(result))

    #last_day = str(last_day_of_month(datetime.date(int(year), int(month), 1)))

    #last_day_string = "%s%s" % (last_day, 'T23:59:00Z')

    s_query = "SELECT max(\"energy\")-min(\"energy\") FROM \"evmeters\" WHERE time >= '%s-%s-01' and time <= '%s' GROUP BY id " % (year, month, last_day_string)
    #print(s_query)
    result = client.query(s_query, database=DBNAME)
    # print("Result: {0}".format(result))


    xls_book_array = []
    xls_book_array_idx = []
    # per id loop
    j=0
    for idi in ppidList:
        print("idi: %s" % idi)
        xls_book_array_idx.append(idi)
        evse_billing_report_file_xslx = "evse_%s-%s-%s.xlsx" % (idi, year, month)
        evse_book = xlsxwriter.Workbook(evse_billing_report_file_xslx, {'strings_to_numbers': True})
        ows = evse_book.add_worksheet('Oversikt')
        for dim in range(1, days_in_month+1):
            ws_date = "%s-%s-%02d" % (year, month, dim)
            formula = "=SUM('%s'!D2:D25)" % (ws_date)
            ows.write_formula(dim, 1,formula)
            ows.write(dim,0,ws_date)
            formula = "=SUM('%s'!F2:F25)" % (ws_date)
            ows.write_formula(dim, 2, formula)
            evse_book.add_worksheet(ws_date)
            evse_ws=evse_book.get_worksheet_by_name(ws_date)
            evse_ws.write(0,0, 'Datum')
            evse_ws.write(0,1, 'Timme')
            evse_ws.write(0,2, 'kWh pris')
            evse_ws.write(0,3, 'kWh')
            evse_ws.write(0,4, 'Öre')
            evse_ws.write(0,5, 'Ink skatt')
        ows.write(0,0, 'Datum')
        ows.write(0,1, 'kWh')
        ows.write(0,2, 'Kostnad (kr)')
        ows.write(33,0, 'Total')
#        ows.write(0,4, 'Öre')
        formula = "=SUM(B2:B%d)" % (days_in_month+1)
        ows.write_formula(33, 1,formula)
        formula = "=SUM(C2:C%d)" % (days_in_month+1)
        ows.write_formula(33, 2,formula)

        ows.write(36,0, 'Elnät')
        ows.write(36,1, elnat)
        ows.write(37,0, 'Elskatt')
        ows.write(37,1, elskatt)
        ows.write(38,0, 'Utrustning')
        ows.write(38,1, 70)



        chart1 = evse_book.add_chart({'type': 'column'})
        # Configure the first series.
        chart1.add_series({
            'name':       'kWh',
            'categories': '=$A$2:$A$32',
            'values':     '=$C$2:$C$32',
        })

        # Add a chart title and some axis labels.
        chart1.set_title ({'name': 'Kostnad'})
        chart1.set_x_axis({'name': 'Datum'})
        chart1.set_y_axis({'name': 'Kr'})

        # Set an Excel chart style.
        chart1.set_style(11)

        # Insert the chart into the worksheet (with an offset).
        ows.insert_chart('D2', chart1, {'x_offset': 25, 'y_offset': 10})



        xls_book_array.append(evse_book)

    i=0
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
            ##print(s_query)
            result = client.query(s_query, database=DBNAME)

            # get price for specific hour
            s_query2 = "SELECT * from pricewatch WHERE time = '%s'" % (date_start)
            # q_res2 = result.get_points()
            # hour_price =
            result2 = client.query(s_query2, database=DBNAME2)
            q_res2 = result2.get_points()
            for item in q_res2:
                kWh_price = float(item['price'])
            #for point in result2.get_points():
            ##print(kWh_price)

            k=0
            for idi in ppidList:
                tagfilter = {}
                tagfilter['id'] = idi
                q_res = result.get_points(tags=tagfilter)
                ws_date = "%s-%s-%02d" % (year, month, day_idx)
                evse_ws = xls_book_array[k].get_worksheet_by_name(ws_date)
                l=0
                #print("k = %d" % k)
                for item in q_res:
                    kWh = item['max_min']
                    evse_ws.write(hour_idx+1,0, ws_date)
                    evse_ws.write(hour_idx+1,1, hour_idx)
                    evse_ws.write(hour_idx+1,2, kWh_price)
                    evse_ws.write(hour_idx+1,3, kWh)
                    costList[k] = costList[k] + ((kWh_price+float(elnat)+float(elskatt)+56.0)*kWh)
                    #print("cost[%d][%d] = %f" %(day_idx, k, costList[k]))
                    formula = "=D%d*C%d" % (hour_idx+2,hour_idx+2)
                    evse_ws.write_formula(hour_idx+1, 4,formula)
                    formula = "=((D%d*(C%d+'Oversikt'!B37+'Oversikt'!B38))*1.25+D%d*'Oversikt'!B39)/100" % (hour_idx+2,hour_idx+2,hour_idx+2)
                    evse_ws.write_formula(hour_idx+1, 5,formula)

                    ##print("%d-%d [%s]: %f T=%.02f" % (day_idx, hour_idx, idi, kWh, kWh*kWh_price))

                    #brf.write("7729-%05d;%s;%s;ELM;%s;%s kWh tariff 22-06 %s p-plats %s\n" % (int(onList[i]),billing_date_str_from,billing_date_str_tom,lformat('%.2f', tariff*kWh), lformat('%.2f', kWh), months[int(month)], idi))
                    l=l+1
                k=k+1
                i=i+1


    # per id loop
    i=0
    for idi in ppidList:
        print("idi: %s cost: %f (%f)" % (idi, costList[i]/100.0, (costList[i]/100.0)*1.25))
        brf_sheet.write(i+1, 4, "%.2f" % (costList[i]/100.0))
        xls_book_array[i].close()
        i = i + 1

    brf_evse_book.close()
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
    parser.add_argument('--pricepageid', type=str, required=False, default="",
                        help='Google Docs sheetid to use')
    parser.add_argument('--pageid', type=str, required=False, default="",
                        help='Google Docs sheet pageid to use')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(host=args.host, port=args.port, year=args.year, month=args.month, tariff=args.tariff, sheetid=args.sheetid, pageid=args.pageid, pricepageid=args.pricepageid)
