#!/usr/bin/env python
import csv
import pickle
import re
import StringIO

from bs4 import BeautifulSoup
from matplotlib import pyplot
import redis
import requests

URL = "http://stats.areppim.com/listes/list_billionairesx{}xwor.htm"

def gather_billionaire_data():
    htmls = []
    session = requests.Session()
    years = ["{0:02d}".format(i) for i in range(15)] + ["96", "97", "98", "99"]
    for year in years:
        request = session.get(URL.format(year))
        htmls.append(request.text)
        request.close()
    session.close()
    return htmls


def parse_html(htmls):
    parsed = dict()
    for html in htmls:
        soup = BeautifulSoup(html)
        rows = soup.find_all("tr")
        year = re.search(r"(\d{4})", rows[0].text).groups()[0]
        parsed[year] = list()
        indices = [index.text for index in rows[1].find_all("h4")]
        for row in rows:
            vals = [val.text for val in row.find_all("td")]
            if re.search("Billionaire [Ll]ist \d{4}", vals[0]) or "Rank" in vals:
                continue
            props = dict()
            for idx, val in enumerate(vals):
                props[indices[idx]] = val
            parsed[year].append(props)
    return parsed


def parse_sp500_data(sp500_csv_reader):
    sp500_data = []
    for data in sp500_csv_reader:
        if "Date" in data:  # Is header
            continue
        sp500_data.append((data[0], data[-1]))
    sp500_data.reverse()
    return sp500_data


def visualize_data(billionaire_data, sp500_data):
    us_billionaires = {}
    for year in billionaire_data:
        us_billionaires[year] = []
        for billionaire in billionaire_data[year]:
            if re.search("(USA|United States)", billionaire["Citizenship"]):
                us_billionaires[year].append(billionaire)
    billionaire_tuple = [(year, len(values)) for year, values in us_billionaires.items()]
    billionaire_tuple.sort(key=lambda tuple_: tuple_[0])
    years = map(lambda x: x[0], billionaire_tuple)
    number_billionaires = map(lambda x: x[1], billionaire_tuple)
    figure, ax1 = pyplot.subplots()
    figure.set_size_inches(25, 10)
    ax1.plot(years, number_billionaires, color="red", lw=5)
    ax1.set_xlabel("Year")
    ax1.set_ylabel("Number US billionaires")
    sp500_x = map(lambda x: x[0], sp500_data)
    sp500_y = map(lambda x: x[1], sp500_data)
    ax2 = ax1.twiny().twinx()
    ax2.plot(sp500_y, lw=10)
    ax2.set_xticks(range(1, len(sp500_x) + 1))
    ax2.set_xlim(right=len(sp500_x) - 1)
    ax2.set_xticklabels([], visible=False)
    ax2.set_xlabel("Date (Months)")
    ax2.set_ylabel("SPY value")
    pyplot.show()


def main():
    redis_client = redis.StrictRedis()
    billionaire_key = "billionaires_list"
    stored_list = redis_client.get(billionaire_key)
    if not redis_client.get(billionaire_key):
        htmls = gather_billionaire_data()
        dumped_data = pickle.dumps(htmls)
        redis_client.set(billionaire_key, dumped_data)
    else:
        htmls = pickle.loads(stored_list)
    billionaire_data = parse_html(htmls)
    sp500_key = "billionaires_sp500_series"
    sp500_raw_redis = redis_client.get(sp500_key)
    if not sp500_raw_redis:
        req = requests.get("http://ichart.yahoo.com/table.csv?s=SPY&a=2&b=1&c=1996&d=2&e=1&f=2014&g=m")
        csv_stringio = StringIO.StringIO(req.text)
        req.close()
        redis_client.set(sp500_key, pickle.dumps(csv_stringio))
    else:
        csv_stringio = pickle.loads(sp500_raw_redis)
    sp500_csv_reader = csv.reader(csv_stringio)
    sp500_data = parse_sp500_data(sp500_csv_reader)
    visualize_data(billionaire_data, sp500_data)


if __name__ == "__main__":
    main()
