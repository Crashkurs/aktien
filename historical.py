import json
import os.path
from multiprocessing.dummy import Pool

import requests
import tqdm

headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0",
    "Origin": "https://www.nasdaq.com",
    "Referer": "https://www.nasdaq.com/",
    "TE": "Trailers"
}


def check_file_or_call(file, callable):
    if os.path.exists(file):
        with open(file, "r") as f:
            content = json.loads(f.read())
    else:
        content = callable()
        with open(file, "w+") as f:
            f.write(json.dumps(content, indent=4))
    return content


def get_stocks():
    query = {
        "limit": 100,
        "offset": 0,
        "tableonly": True
    }
    url = "https://api.nasdaq.com/api/screener/stocks"
    result = []
    response = requests.get(url, params=query, headers=headers).json()
    entries = response["data"]["table"]["rows"]
    while entries and len(entries) > 0:
        result += entries
        query["offset"] += query["limit"]
        print(query)
        response = requests.get(url, params=query, headers=headers).json()
        entries = response["data"]["table"]["rows"]
    return result


def get_data(symbol, asset="stocks"):
    head = headers.copy()
    head["Referer"] = "https://www.nasdaq.com/market-activity/stocks/{}/historical".format(symbol)
    url = "https://api.nasdaq.com/api/quote/{}/historical?assetclass={}&fromdate={}&limit=9999&todate={}"
    from_date = "2011-01-08"
    to_date = "2022-01-08"
    stock = symbol
    try:
        response = requests.get(url.format(stock, asset, from_date, to_date), headers=head)
        text = response.json()
    except Exception as e:
        print(e, response.text)
        return {}
    return text


def load(args):
    stock, bar = args
    cleaned = stock['symbol'].replace("/", "_").replace("\\", "__")
    bar.update()
    return check_file_or_call(f"data/{cleaned}.json", lambda: get_data(stock['symbol']))


def get_all_data(stock_desc):
    os.makedirs("data", exist_ok=True)
    bar = tqdm.tqdm(stock_desc, desc="Stocks")
    args = [(stock, bar) for stock in stock_desc]
    with Pool(processes=16) as pool:
        result = pool.map(load, args)
    return result


stocks = check_file_or_call("stocks.json", get_stocks)
data = check_file_or_call("data.json", lambda: get_all_data(stocks))
