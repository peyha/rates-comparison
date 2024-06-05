import requests
import pandas as pd

def load_df_aave(relevant_markets):
  market_tx = """
  query MyQuery($hour: Int, $id: String, $marketNames: [String!]) {
    marketHourlySnapshots(where: {hours_gte: $hour, id_gte: $id, market_: {name_in: $marketNames}}, first: 1000) {
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

  url = "https://api.thegraph.com/subgraphs/name/messari/aave-v3-ethereum"
  blue_launch_timestamp = 1704927599
  hour = blue_launch_timestamp // 3600
  last_id = ""
  market_data = []

  while True:
      variables = {"hour": hour, "id": last_id, "marketNames": relevant_markets}
      res = requests.post(url, json={"query": market_tx, "variables": variables})
      markets = res.json()["data"]["marketHourlySnapshots"]

      for market in markets:
          rates = market["rates"]
          symbol = market["market"]["name"].split()[-1]
          supply_rate, borrow_rate = None, None
          for rate in rates:
              if rate["side"] == "LENDER" and rate["type"] == "VARIABLE":
                  supply_rate = rate["rate"]
              elif rate["side"] == "BORROWER" and rate["type"] == "VARIABLE":
                  borrow_rate = rate["rate"]
          market_data.append((
              symbol,
              market["hours"],
              supply_rate,
              borrow_rate,
              market["totalDepositBalanceUSD"],
              market["totalBorrowBalanceUSD"]
          ))

      if len(markets) < 1000:
          break
      last_id = markets[-1]["id"]

  df_aave = pd.DataFrame(market_data, columns=["loan_asset", "hours", "supplyApy", "borrowApy", "totalSupplyUSD", "totalBorrowUSD"])
  df_aave['date'] = pd.to_datetime(df_aave['hours']*3600, unit='s')
  df_aave.sort_values("date", inplace=True)
  columns_to_convert = ['supplyApy', 'borrowApy', 'totalSupplyUSD', 'totalBorrowUSD']
  df_aave[columns_to_convert] = df_aave[columns_to_convert].astype(float)
  df_aave['supplyApy'] = df_aave['supplyApy'] / 100
  df_aave['borrowApy'] = df_aave['borrowApy'] / 100
  df_aave['utilization'] = df_aave['totalBorrowUSD'] / df_aave['totalSupplyUSD']

  df_aave = df_aave[['date', 'loan_asset', 'supplyApy', 'borrowApy', 'utilization', 'totalSupplyUSD', 'totalBorrowUSD']]

  return df_aave