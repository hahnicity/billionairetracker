#!/usr/bin/env python
import pickle
import re

from bs4 import BeautifulSoup
import redis
import requests

URL = "http://stats.areppim.com/listes/list_billionairesx{}xwor.htm"

def gather_data():
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


def main():
    redis_client = redis.StrictRedis()
    redis_key = "billionaires_list"
    stored_list = redis_client.get(redis_key)
    if not redis_client.get(redis_key):
        htmls = gather_data()
        parsed_data = parse_html(htmls)
        dumped_data = pickle.dumps(parsed_data)
        redis_client.set(redis_key, dumped_data)
    else:
        parsed_data = pickle.loads(stored_list)


if __name__ == "__main__":
    main()
