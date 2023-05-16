import requests
import argparse
import json
from datetime import date, timedelta
from influxdb import InfluxDBClient

USER = ''
PASSWORD = ''
DBNAME = 'pricewatch'


def main(host, port, start_date, end_date):
    #start_date = date(year, month, day)
    #end_date = date(year, month, day)
    #delta = timedelta(days=1)
    #while start_date <= end_date:
        #print(start_date.strftime("%Y-%m-%d"))
        #start_date += delta

    client = InfluxDBClient(host, port, USER, PASSWORD, DBNAME)
    client.switch_database(DBNAME)

    # The API endpoint
    #url = "https://www.vattenfall.se/api/price/spot/pricearea/2023-01-01/2023-05-16/SN3"
    url = "https://www.vattenfall.se/api/price/spot/pricearea/%s/%s/SN3" % (start_date, end_date)
    #print(url)

    # A GET request to the API
    response = requests.get(url)

    # Print the response
    response_json = response.json()
    #print(response_json)

    # insert day data into influx
    for jl in response_json:
        row = []
        #data = json.loads(json_list)
        #print("%s: %s" % (jl['TimeStamp'], jl['Value']))
        price = "%s" % (jl['Value'])
        ts = jl['TimeStamp']
        json_body = [{
            "time": ts,
            "measurement": "pricewatch",
            "fields": {
                "price": price
            }
        }]
        #print(json_body)
        ret = client.write_points(json_body)
        #print(ret)

def parse_args():
    """Parse the args."""
    parser = argparse.ArgumentParser(
        description='Nordpool market price extractor')
    parser.add_argument('--host', type=str, required=False,
                        default='192.168.1.6',
                        help='hostname influxdb http API')
    parser.add_argument('--port', type=int, required=False, default=8086,
                        help='port influxdb http API')
    parser.add_argument('--start_date', type=str, required=False,
                        default=date.today(),
                        help='start date of the queried period')
    parser.add_argument('--end_date', type=str, required=False,
                        default=date.today(),
                        help='end date of the queried period')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(host=args.host, port=args.port, start_date=args.start_date,end_date=args.end_date)

