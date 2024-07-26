import requests
import pandas as pd
import numpy as np


def load_df_compound():
    market_tx = """
  query MyQuery($hour: Int, $id: String) {
    marketHourlySnapshots(where: {hours_gte: $hour, id_gte: $id}, first: 1000) {
      rates {
        rate
        side
        type
      }
      totalBorrowBalanceUSD
      totalDepositBalanceUSD
      market {
        name
      }
      hours
      id
    }
  }"""

    blue_launch_timestamp = 1704927599
    url = "https://api.thegraph.com/subgraphs/name/messari/compound-v3-ethereum"
    hour = blue_launch_timestamp // 3600
    last_id = ""
    market_data = []

    while True:
        res = requests.post(url, json={"query": market_tx, "variables": {
                            "hour": hour, "id": last_id}})
        print(res.status_code)
        markets = res.json()["data"]["marketHourlySnapshots"]

        for market in markets:
            rates = market["rates"]
            if rates != []:
                market_name = market["market"]["name"].split(' - ')[0]
                asset = market_name.split()[-1].strip()
                supplyApy, borrowApy = None, None
                for rate in rates:
                    if rate["side"] == "LENDER" and rate["type"] == "VARIABLE":
                        supplyApy = rate["rate"]
                    elif rate["side"] == "BORROWER" and rate["type"] == "VARIABLE":
                        borrowApy = rate["rate"]
                market_data.append((
                    asset,
                    market["hours"],
                    supplyApy,
                    borrowApy,
                    market["totalDepositBalanceUSD"],
                    market["totalBorrowBalanceUSD"]
                ))

        if len(markets) < 1000:
            break
        last_id = markets[-1]["id"]

    df_compound = pd.DataFrame(market_data, columns=[
                               "loan_asset", "hours", "supplyApy", "borrowApy", "totalSupplyUSD", "totalBorrowUSD"])
    df_compound['date'] = pd.to_datetime(df_compound['hours']*3600, unit='s')
    df_compound.sort_values("date", inplace=True)
    columns_to_convert = ['supplyApy', 'borrowApy',
                          'totalSupplyUSD', 'totalBorrowUSD']
    df_compound[columns_to_convert] = df_compound[columns_to_convert].astype(
        float)
    df_compound["borrowApy"] = df_compound["borrowApy"]/100.
    df_compound["supplyApy"] = df_compound["supplyApy"]/100.
    df_compound['utilization'] = df_compound['totalBorrowUSD'] / \
        df_compound['totalSupplyUSD']

    df_compound = df_compound[['date', 'loan_asset', 'supplyApy',
                               'borrowApy', 'utilization', 'totalSupplyUSD', 'totalBorrowUSD']]

    return df_compound


if __name__ == '__main__':

    df = load_df_compound()
    print(df.head())
