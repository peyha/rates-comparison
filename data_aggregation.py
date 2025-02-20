import pandas as pd
import numpy as np
import os
import time
from blue_data import load_df_blue
from aave_data import load_df_aave
from compound_data import load_df_compound


def load_df_all_protocols():

    print("Fetching blue data...")
    df_blue = load_df_blue()
    print("Blue data fetched!")

    print("Now fetching Compound data...")
    df_compound = load_df_compound()
    print("Compound data fetched!")

    loan_assets = df_blue['loan_asset'].unique()
    relevant_markets = [f"Aave Ethereum {asset}" for asset in loan_assets]
    # relevant_markets = ["Aave Ethereum DAI", "Aave Ethereum USDC", "Aave Ethereum WETH", "Aave Ethereum USDT", "Aave Ethereum USDA", "Aave Ethereum PYUSD", "Aave Ethereum crvUSD", "Aave Ethereum WBTC"]

    print("Now fetching Aave data...")
    df_aave = load_df_aave(relevant_markets)
    print("Aave data fetched!")

    columns = ['date', 'protocol', 'market', 'loan_asset', 'supplyApy', 'borrowApy',
               'rate_at_target', 'utilization', 'totalSupplyUSD', 'totalBorrowUSD']
    df_aave['rate_at_target'] = np.nan
    df_compound['rate_at_target'] = np.nan
    df_aave['protocol'] = 'Aave'
    df_aave['market'] = df_aave['loan_asset'] + ' - Aave'
    df_compound['protocol'] = 'Compound'
    df_compound['market'] = df_compound['loan_asset'] + ' - Compound'
    df_blue['protocol'] = 'Blue'
    df_all = pd.concat(
        [df_aave[columns], df_compound[columns], df_blue[columns]])
    print(df_all['protocol'].unique())
    # df_all = df_all[df_all.loan_asset.isin(df_blue['loan_asset'].unique())]

    # # Filter based on the launch date and the minimum date for each loan asset
    # min_dates = df_all[df_all['protocol'] == 'Blue'].groupby('loan_asset')['date'].min().reset_index()
    # min_dates.columns = ['loan_asset', 'min_date']
    # df_all = pd.merge(df_all, min_dates, on='loan_asset', how='left')
    # df_all = df_all[(df_all['date'] >= df_all['min_date'])]
    # df_all = df_all.drop(columns=['min_date'])

    # Add utilization_target information
    utilization_targets = {
        'Aave': {
            'USDC': 0.92,
            'USDT': 0.92,
            'WETH': 0.90,
            'DAI': 0.92,
            'PYUSD': 0.80
        },
        'Compound': {
            'WETH': 0.85,
            'USDC': 0.90
        },
        'Blue': 0.90
    }

    def get_utilization_target(row):
        if row['protocol'] == 'Blue':
            return utilization_targets['Blue']
        else:
            return utilization_targets.get(row['protocol'], {}).get(row['loan_asset'], None)

    df_all['utilization_target'] = df_all.apply(get_utilization_target, axis=1)

    df_all = df_all.sort_values(by=['market', 'date'])
    df_all['borrowApy_daily'] = df_all.groupby('market')['borrowApy'].transform(
        lambda x: x.rolling(24, center=True).mean())
    df_all['borrowApy_weekly'] = df_all.groupby('market')['borrowApy'].transform(
        lambda x: x.rolling(7*24, center=True).mean())
    df_all['utilization_daily'] = df_all.groupby('market')['utilization'].transform(
        lambda x: x.rolling(24, center=True).mean())
    df_all['utilization_weekly'] = df_all.groupby('market')['utilization'].transform(
        lambda x: x.rolling(7*24, center=True).mean())
    df_all['supplyApy_daily'] = df_all.groupby('market')['supplyApy'].transform(
        lambda x: x.rolling(24, center=True).mean())
    df_all['supplyApy_weekly'] = df_all.groupby('market')['supplyApy'].transform(
        lambda x: x.rolling(7*24, center=True).mean())

    # df_all = df_all.dropna(
    #    subset=[col for col in df_all.columns if col != 'rate_at_target'])

    return df_all


if __name__ == '__main__':
    if os.path.exists("last_update.txt"):
        with open("last_update.txt", 'r') as f:
            last_update = float(f.read().strip())
    else:
        last_update = 0

    current_time = time.time()
    if current_time - last_update > 172_800:  # 48 hours in seconds
        print('updating data... This can take a few minutes')
        df_all = load_df_all_protocols()
        print("Data process completed!")

        df_all.to_csv('df_all.csv', index=False)

        with open("last_update.txt", 'w') as f:
            f.write(str(current_time))
