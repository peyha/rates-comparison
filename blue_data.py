import requests
import pandas as pd
import numpy as np
import time

def load_df_blue():
        current_timestamp = int(time.time())
        query = f"""
        query MyQuery{{
                markets(
                        first: 100
                        skip: 0
                        orderBy: SupplyAssetsUsd
                        where: {{ supplyAssetsUsd_gte: 100000 }}
                ){{
                        items{{
                                uniqueKey
                                lltv
                                loanAsset {{symbol }}
                                collateralAsset {{symbol }}
                                supplyingVaults {{ name }}
                                historicalState {{
                                        borrowAssetsUsd(options: {{
                                                startTimestamp: 1
                                                endTimestamp: {current_timestamp}
                                                interval: MINUTE
                                        }}){{
                                                x
                                                y
                                        }}
                                        supplyAssetsUsd(options: {{
                                                startTimestamp: 1
                                                endTimestamp: {current_timestamp}
                                                interval: MINUTE
                                        }}){{
                                                x
                                                y
                                        }}
                                        collateralAssetsUsd(options: {{
                                                startTimestamp: 1
                                                endTimestamp: {current_timestamp}
                                                interval: MINUTE
                                        }}){{
                                                x
                                                y
                                        }}
                                        utilization(options: {{
                                                startTimestamp: 1
                                                endTimestamp: {current_timestamp}
                                                interval: MINUTE
                                        }}){{
                                                x
                                                y
                                        }}
                                        rateAtUTarget(options: {{
                                                startTimestamp: 1
                                                endTimestamp: {current_timestamp}
                                                interval: MINUTE
                                        }}){{
                                                x
                                                y
                                        }}
                                        supplyApy(options: {{
                                                startTimestamp: 1
                                                endTimestamp: {current_timestamp}
                                                interval: MINUTE
                                        }}){{
                                                x
                                                y
                                        }}
                                        netSupplyApy(options: {{
                                                startTimestamp: 1
                                                endTimestamp: {current_timestamp}
                                                interval: MINUTE
                                        }}){{
                                                x
                                                y
                                        }}
                                        borrowApy(options: {{
                                                startTimestamp: 1
                                                endTimestamp: {current_timestamp}
                                                interval: MINUTE
                                        }}){{
                                                x
                                                y
                                        }}
                                }}
                        }}
                }}
        }}
        """

        url = 'https://blue-api.morpho.org/graphql'

        response = requests.post(url, json={"query": query})
        data = response.json()['data']['markets']['items']

        rows = []

        for market in data:
                marketKey = market['uniqueKey']
                lltv = float(market['lltv'])/10e17 if market['lltv'] else np.nan
                loanAsset_symbol = market['loanAsset']['symbol']
                collateralAsset_symbol = market['collateralAsset']['symbol'] if market['collateralAsset'] else np.nan
                market_name = f'{collateralAsset_symbol}/{loanAsset_symbol} ({lltv})' if collateralAsset_symbol else f'{loanAsset_symbol} idle'
                supplyingVaults = ', '.join([vault['name'] for vault in market['supplyingVaults']])

                historicalState = market['historicalState']
                timestamps = historicalState['borrowAssetsUsd']
        
                try:
                        for idx in range(len(timestamps)):
                                row = {
                                        'date': pd.to_datetime(timestamps[idx]['x'], unit='s') if timestamps else np.nan,
                                        'market': market_name,
                                        'market_id': marketKey,
                                        'lltv': lltv,
                                        'loan_asset': loanAsset_symbol,
                                        'collateral_asset': collateralAsset_symbol,
                                        'supplyingVaults': supplyingVaults,
                                        'totalBorrowUSD': historicalState['borrowAssetsUsd'][idx]['y'] if historicalState['borrowAssetsUsd'] else np.nan,
                                        'totalSupplyUSD': historicalState['supplyAssetsUsd'][idx]['y'] if historicalState['supplyAssetsUsd'] else np.nan,
                                        'collateralAssetsUsd': historicalState['collateralAssetsUsd'][idx]['y'] if historicalState['collateralAssetsUsd'] else np.nan,
                                        'utilization': historicalState['utilization'][idx]['y'] if historicalState['utilization'] else np.nan,
                                        'rate_at_target': historicalState['rateAtUTarget'][idx]['y'] if historicalState['rateAtUTarget'] else np.nan,
                                        'supplyApy': historicalState['supplyApy'][idx]['y'] if historicalState['supplyApy'] else np.nan,
                                        'netSupplyApy': historicalState['netSupplyApy'][idx]['y'] if historicalState['netSupplyApy'] else np.nan,
                                        'borrowApy': historicalState['borrowApy'][idx]['y'] if historicalState['borrowApy'] else np.nan
                                }
                                rows.append(row)

                        pass
                except Exception as e:
                        print("An error occurred:", str(e))

        df_blue = pd.DataFrame(rows)
        
        ## Dealing with different markets with the same name
        df_blue.loc[df_blue['market_id'] == '0xc54d7acf14de29e0e5527cabd7a576506870346a78a11a6762e2cca66322ec41', 'market'] = 'WETH / wstETH (94.5%) MP'
        df_blue.loc[df_blue['market_id'] == '0xd0e50cdac92fe2172043f5e0c36532c6369d24947e40968f34a5e8819ca9ec5d', 'market'] = 'WETH / wstETH (94.5%) ER'

        df_blue['market'] = df_blue.apply(
                lambda row: row['market'] + '_' + row['market_id'][:5] if len(df_blue[df_blue['market'] == row['market']]['market_id'].unique()) > 1 else row['market'], axis=1
        )

        return df_blue