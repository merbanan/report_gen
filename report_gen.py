import argparse

import math
import datetime
import time
import json

from influxdb import InfluxDBClient

USER = 'Energy'
PASSWORD = 'Energy'
DBNAME = 'Energy'

id_list = [ 1 ,
2   ,
5   ,
6   ,
11  ,
12  ,
15  ,
16  ,
17  ,
18  ,
19  ,
20  ,
21  ,
22  ,
24  ,
25  ,
26  ,
27  ,
28  ,
29  ,
30  ,
31  ,
32  ,
33  ,
34  ,
35  ,
36  ,
37  ,
38  ,
39  ,
40  ,
41  ,
42  ,
43  ,
44  ,
45  ,
46  ,
47  ,
48  ,
49  ,
50  ,
51  ,
52  ,
53  ,
54  ,
55  ,
56  ,
57  ,
58  ,
59  ,
60  ,
61  ,
62  ,
63  ,
64  ,
65  ,
66  ,
67  ,
68  ,
69  ,
70  ,
71  ,
72  ,
73  ,
74  ,
75  ,
76  ,
77  ,
78  ,
79  ,
80  ,
81  ,
82  ,
83  ,
84  ,
85  ,
86  ,
87  ,
88  ,
89  ,
90  ,
91  ,
92  ,
93  ,
94  ,
95  ,
96  ,
97  ,
98  ,
99  ,
100 ,
101 ,
102 ,
103 ,
104 ,
105 ,
106 ,
107 ,
108 ,
109 ,
110 ,
111 ,
112 ,
113 ,
114 ,
115 ,
116 ,
117 ,
118 ,
119 ,
120 ,
121 ,
122 ,
123 ,
124 ,
125 ,
126 ,
127 ,
128 ,
129 ,
130 ,
131 ,
132 ,
133 ,
134 ,
135 ,
136 ,
137 ,
138 ,
139 ,
140 ,
141 ,
142 ,
143 ,
144 ,
145 ,
146 ,
147 ,
148 ,
149 ,
150 ,
151 ,
152 ,
153 ,
154 ,
155 ,
156 ,
157 ,
158 ,
159 ,
160 ,
161 ,
162 ,
163 ,
164 ,
165 ,
166 ,
167 ,
168 ,
169 ,
170 ,
171 ,
172 ,
173 ,
174 ,
175 ,
176 ,
177 ,
178 ,
179 ,
180 ,
181 ,
182 ,
183 ,
184 ,
185 ,
186 ,
187 ,
188 ,
189 ,
191 ,
192 ,
193 ,
194 ,
195 ,
196 ,
198 ,
199 ,
201 ,
202 ,
204 ,
205 ,
206 ,
207 ,
208 ,
209 ,
210 ,
211 ,
212 ,
213 ,
214 ,
215 ,
216 ,
217 ,
218 ,
219 ,
220 ,
221 ,
222 ,
223 ,
224 ,
225 ,
226 ,
227 ,
228 ,
229 ,
230 ,
231 ,
232 ,
233 ,
250 ,
251 ,
252 ,
253 ,
254 ,
255 ,
256 ]

def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)  # this will never fail
    return next_month - datetime.timedelta(days=next_month.day)

def result_to_kWh(result, value):
    kWh = result.get_points()
    for item in kWh:
        return item[value]

def main(host='192.168.1.6', port=8086, year='2019', month='08'):
    now = datetime.datetime.today()

    client = InfluxDBClient(host, port, USER, PASSWORD, DBNAME)
    client.switch_database(DBNAME)

    query = 'SELECT last("kWh")-first("kWh") FROM "Energy" WHERE (id = 40) AND time >= 1570572000000ms and time <= 1570658399999ms'

    result = client.query(query, database=DBNAME)
    #print("Result: {0}".format(result))

    last_day = "%s%s" % (last_day_of_month(datetime.date(int(year), int(month), 1)), 'T23:59:00Z')
    #print last_day

    for i in id_list:
        s_query = "SELECT last(\"kWh\")-first(\"kWh\") FROM \"Energy\" WHERE (id = %d) AND time >= '%s-%s-01' AND time <= '%s'" % (i, year, month, last_day)
        #print(s_query)
        result = client.query(s_query, database=DBNAME)
        kWh = result_to_kWh(result,'last_first')
        print("%s%s, 7729-%s, %s" % (year,month,i,kWh))



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
    parser.add_argument('--month', type=str, required=False,
                        default='08',
                        help='month of the queried period')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(host=args.host, port=args.port, year=args.year, month=args.month)
