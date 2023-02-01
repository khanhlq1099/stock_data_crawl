from datetime import date

import requests
import json

from stock.config import SSI_API_CONFIG
from stock.module.ssi.constants import MARKET_API
from stock.module.ssi.model import SsiApiException


def get_access_token():
    api_url = MARKET_API.ROOT_PATH + MARKET_API.ACCESS_TOKEN_API
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {SSI_API_CONFIG.PRIVATE_KEY}"
    }
    body = {
        "consumerID": SSI_API_CONFIG.CONSUMER_ID,
        "consumerSecret": SSI_API_CONFIG.CONSUMER_SECRET
    }
    data = json.dumps(body)

    response = requests.post(api_url, headers=headers, params=None, data=data)
    response = json.loads(response.content)

    if (response and response["message"] and response["message"] == 'Success' and response["data"] and response["data"]["accessToken"]):
        return response["data"]["accessToken"]
    else:
        raise SsiApiException(error_code="ssi.market.access_token", message="Access Token Error")

def get_securities(market: str = None, page_index: int = 1, page_size: int = MARKET_API.DEFAULT_PAGE_SIZE_1000):
    access_token = get_access_token()
    # print(access_token)

    api_url = MARKET_API.ROOT_PATH + MARKET_API.SECURITIES_API
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    params = {
        "market": market,  # HOSE, HNX, UPCOM, DER
        "pageIndex": page_index,
        "pageSize": page_size
    }

    response = requests.get(api_url, headers=headers, params=params, data=None)
    response = json.loads(response.content)

    if (response["message"] and response["message"] == 'Success'):
        return {**response, **{"success": True}}
    else:
        return {**response, **{"success": False}}

def get_daily_stock_price(symbol: str, from_date: date, to_date: date, market: str = None, page_index: int = 1, page_size: int = MARKET_API.DEFAULT_PAGE_SIZE_100):
    access_token = get_access_token()
    # print(access_token)
    # print(from_date.strftime("%d/%m/%Y"))
    # print(to_date.strftime("%d/%m/%Y"))

    api_url = MARKET_API.ROOT_PATH + MARKET_API.DAILY_STOCK_PRICE_API
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    params = {
        "symbol": symbol,
        "fromDate": from_date.strftime("%d/%m/%Y"),
        "toDate": to_date.strftime("%d/%m/%Y"),
        "pageIndex": page_index,
        "pageSize": page_size,
        "market": market
    }

    # print(params)

    response = requests.get(api_url, headers=headers, params=params, data=None)
    response = json.loads(response.content)

    # print(response)
    if (response["message"] and response["message"] == 'Success'):
        return {**response, **{"success": True}}
    else:
        return {**response, **{"success": False}}

def authorization_headers(access_token: str):
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}"
    }