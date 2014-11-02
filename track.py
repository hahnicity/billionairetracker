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
    billionaire_tuple = [(year, len(values)) for year, values in billionaire_data.items()]
    billionaire_tuple.sort(key=lambda tuple_: tuple_[0])
    years = map(lambda x: x[0], billionaire_tuple)
    number_billionaires = map(lambda x: x[1], billionaire_tuple)
    figure, ax1 = pyplot.subplots()
    import pdb; pdb.set_trace()
    ax1.plot(years, number_billionaires)
    sp500_x = map(lambda x: x[0], sp500_data)
    sp500_y = map(lambda x: x[1], sp500_data)
    ax1.plot(sp500_y)
    ax1.xticks(range(len(sp500_x)), sp500_x)

    pyplot.show()


def main():
    redis_client = redis.StrictRedis()
    billionaire_key = "billionaires_list"
    stored_list = redis_client.get(billionaire_key)
    if not redis_client.get(billionaire_key):
        htmls = gather_billionaire_data()
        billionaire_data = parse_html(htmls)
        dumped_data = pickle.dumps(billionaire_data)
        redis_client.set(billionaire_key, dumped_data)
    else:
        billionaire_data = pickle.loads(stored_list)
    sp500_key = "billionaires_sp500_series"
    csv_stringio = pickle.loads(redis_client.get(sp500_key))
    if not csv_stringio:
        req = requests.get("http://ichart.yahoo.com/table.csv?s=SPY&a=0&b=1&c=1996&d=9&e=1&f=2014&g=m")
        csv_stringio = StringIO.StringIO(req.text)
        req.close()
        redis_client.set(sp500_key, pickle.dumps(csv_stringio))
    sp500_csv_reader = csv.reader(csv_stringio)
    sp500_data = parse_sp500_data(sp500_csv_reader)
    visualize_data(billionaire_data, sp500_data)


if __name__ == "__main__":
    main()
