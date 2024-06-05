import numpy as np
import pandas as pd

def IAE(U, u_target):
    error = np.sum(np.abs(U - u_target)) / len(U)
    return error

def ISE(U, u_target):
    error = np.sum((U - u_target)**2) / len(U)
    return error

def liquidity(U, u_treshold=0.99):
    n = len(U)
    count = 0
    for i in range(n):
        if U[i] > u_treshold:
            count += 1
    return count / n

def ISE_positive(U, u_target):
    mask = U > u_target
    error = np.sum(((U[mask] - u_target)/(1-u_target))**2) / len(U)
    return error

def IAE_negative(U, u_target):
    mask = U < u_target
    error = np.sum(np.abs(U[mask] - u_target)/u_target) / len(U)
    return error

def volatility(dataframe, column):
    return dataframe[column].pct_change().std()*((252*24*6)**0.5)

def inside_spread(r, U, r_B, r_D):
    mask = (r < r_B) & (r*U > r_D)
    return np.count_nonzero(mask) / len(r)

def average_utilization(U):
    return np.mean(U)

def average_rate(dataframe):
    return np.mean(dataframe['borrowApy'])

def weighted_average_rate(dataframe):
    """ would be useful if the dataframe wasn't regularly sampled """
    df = dataframe.copy()
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(by='date')
    df['duration'] = df['date'].diff().dt.total_seconds() / (60 * 60 * 24)  # Duration in days
    weighted_avg_borrow_rate = np.average(df['borrowApy'], weights=df['duration'].shift(-1).fillna(0))
    return weighted_avg_borrow_rate

def compute_metrics(df):
    metrics_columns = ['avg utilization', 'IAE', 'ISE', 'Liquidity', 'ISE_positive', 'IAE_negative', 'volatility', 'avg borrow rate']

    results_df = pd.DataFrame(columns=['market', 'loan_asset', 'utilization_target'] + metrics_columns)
    markets = df['market'].unique()

    for market in markets:
        market_data = df[df['market'] == market]
        U = market_data['utilization'].values
        u_target = market_data['utilization_target'].values[0]

        try:
            metrics = {
                'utilization_target': u_target,
                'market': market_data['market'].iloc[0],
                'loan_asset': market_data['loan_asset'].iloc[0],
                'IAE': round(IAE(U, u_target), 3),
                'ISE': round(ISE(U, u_target), 3),
                'Liquidity': round(liquidity(U), 3),
                'ISE_positive': round(ISE_positive(U, u_target), 3),
                'IAE_negative': round(IAE_negative(U, u_target), 3),
                'volatility': round(volatility(market_data, 'borrowApy'), 3),
                'avg borrow rate': round(average_rate(market_data), 3),
                'avg utilization': round(average_utilization(U), 3)
            }
        except Exception as e:
            print("An error occurred:", e)
            print(market)
            print(market_data)
            print(market_data['loan_asset'])
        
        results_df = pd.concat([results_df, pd.DataFrame(metrics, index=[0])], ignore_index=True)

    return results_df