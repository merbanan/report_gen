import argparse

import math
#from datetime import datetime
import datetime
import calendar
import time
import json
import xlsxwriter

from influxdb import InfluxDBClient

USER = 'Energy'
PASSWORD = 'Energy'
DBNAME = 'Energy'


months = ['', 'januari', 'februari', 'mars', 'april', 'maj', 'juni', 'juli',
          'augusti', 'september', 'oktober', 'november', 'december' ]

def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)  # this will never fail
    return next_month - datetime.timedelta(days=next_month.day)

def result_to_kWh(result, value):
    kWh = result.get_points()
    for item in kWh:
        return item[value]

def main(host, port, year, lgh_nr):

    today = datetime.datetime.now()
    month = today.strftime("%m")
    year = today.strftime("%Y")
    print("Current Month with Decimal Number :", month, year);
    id = lgh_nr;

    last_day = last_day_of_month(datetime.date(int(year), int(month), 1))

    last_day_string = "%s%s" % (last_day, 'T23:59:00Z')

    print("Last day :", last_day, last_day_string);


    client = InfluxDBClient(host, port, USER, PASSWORD, DBNAME)
    client.switch_database(DBNAME)

    query = 'SELECT last("kWh")-first("kWh") FROM "Energy" WHERE (id = 40) AND time >= 1570572000000ms and time <= 1570658399999ms'

    result = client.query(query, database=DBNAME)
    #print("Result: {0}".format(result))

    billing_report_file_xslx = "%s-%s.xlsx" % (year, month)
    brf_book = xlsxwriter.Workbook(billing_report_file_xslx, {'strings_to_numbers': True})
    brf_main = brf_book.add_worksheet('Översikt')

    for mos in range(1, int(month)+1):
        brf_sheet = brf_book.add_worksheet(months[mos])
        format_text = brf_book.add_format()
        format_text.set_num_format('@') # @ - This is text format in excel
        format_date = brf_book.add_format({'num_format': 'yyyy-mm-dd'})
        brf_sheet.write(0,0, 'Datum')
        brf_sheet.write(0,1, 'kWh')

        input_dt = datetime.datetime(int(year), int(mos), 1)
        #print("The original date is:", input_dt.date())
        res = calendar.monthrange(input_dt.year, input_dt.month)
        ldom = res[1]
        #print(f"Last date of month is: {input_dt.year}-{input_dt.month}-{ldom}")

        kWh_list = []
        #brf_sheet.write("7729-%05d," % id)
        for day in range(1, ldom):
            to_time = "%s-%02d-%02d%s" % (year, mos,day, 'T23:59:00Z')
            s_query = "SELECT last(\"kWh\")-first(\"kWh\") FROM \"Energy\" WHERE (id = %d) AND time >= '%s-%02d-%02d' AND time <= '%s'" % (id, year, mos, day, to_time)
            #print("", s_query)
            result = client.query(s_query, database=DBNAME)
            kWh = result_to_kWh(result,'last_first')
            kWh_list += [kWh]
            brf_sheet.write(day, 0,"%s-%02d-%02d" % (year, mos,day))
            brf_sheet.write(day, 1,"%s" % kWh)

        brf_sheet.write_formula(35, 1,'=SUM(B2:B32)')

    brf_main.write(0,0, 'Månad')
    brf_main.write(0,1, 'kWh per månad')

    for mos in range(1, int(month)+1):
        brf_main.write(mos +1 ,0, "%s" % (months[mos]))
        brf_main.write_formula(mos +1 ,1, "='%s'!B36" % (months[mos]))

    brf_main.write(14 ,0, "Totalt" )
    brf_main.write_formula(14, 1,"=SUM(B1:B12)")



    chart1 = brf_book.add_chart({'type': 'column'})
    # Configure the first series.
    chart1.add_series({
        'name':       'kWh',
        'categories': '=$A$3:$A$14',
        'values':     '=$B$3:$B$14',
    })

    # Add a chart title and some axis labels.
    chart1.set_title ({'name': 'Förbrukning'})
    chart1.set_x_axis({'name': 'Månad'})
    chart1.set_y_axis({'name': 'Energi (kWh)'})

    # Set an Excel chart style.
    chart1.set_style(11)

    # Insert the chart into the worksheet (with an offset).
    brf_main.insert_chart('D2', chart1, {'x_offset': 25, 'y_offset': 10})



    brf_book.close()
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
                        default='2022',
                        help='year of the queried period')
    parser.add_argument('--lgh_nr', type=int, required=True,
                        help='number')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(host=args.host, port=args.port, year=args.year, lgh_nr=args.lgh_nr)
