import streamlit as st
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import time
from metrics import *
from data_aggregation import load_df_all_protocols

if os.path.exists("last_update.txt"):
    with open("last_update.txt", 'r') as f:
        last_update = float(f.read().strip())
else:
    last_update = 0

current_time = time.time()
if current_time - last_update > 86400:  # 24 hours in seconds
    print('updating data... This can take a few minutes')
    df_all = load_df_all_protocols()
    print("Data process completed!")

    df_all.to_csv('df_all.csv', index=False)

    with open("last_update.txt", 'w') as f:
        f.write(str(current_time))

else:
    df_all = pd.read_csv('df_all.csv')

results = compute_metrics(df_all).sort_values('market')

# Define the layout and interactivity
st.title('Loan Asset Data Visualization')

# Dropdown for loan asset selection
loan_asset = st.selectbox(
    'Select a loan asset',
    df_all['loan_asset'].unique()
)

# Dropdown for rate type selection
rate_type = st.selectbox(
    'Select a rate type',
    ['borrowApy', 'daily_rolling_avg', 'weekly_rolling_avg']
)

# Multiselect for market selection
markets = df_all[df_all['loan_asset'] == loan_asset]['market'].unique()
selected_markets = st.multiselect(
    'Select markets',
    markets,
    default=markets
)

# Tab selection
tab = st.selectbox(
    'Select a view',
    ['Graphs', 'Metrics Table', 'Correlation Heatmap']
)

# Function to update graphs
def update_graphs(selected_loan_asset, selected_markets, borrow_rate_type):
    traces_utilization = []
    traces_borrow_rate = []
    traces_rate_at_target = []

    if selected_loan_asset:
        filtered_df = df_all[df_all['loan_asset'] == selected_loan_asset]

        if selected_markets:
            filtered_df = filtered_df[filtered_df['market'].isin(selected_markets)]

        unique_markets = filtered_df['market'].unique()

        color_map = {market: px.colors.qualitative.Plotly[i % len(px.colors.qualitative.Plotly)]
                     for i, market in enumerate(unique_markets)}

        for market in unique_markets:
            market_data = filtered_df[filtered_df['market'] == market]
            color = color_map[market]
            traces_utilization.append(go.Scatter(x=market_data['date'], y=market_data['utilization'], mode='lines', name=f'{market}', line=dict(color=color)))
            if borrow_rate_type:
                traces_borrow_rate.append(go.Scatter(x=market_data['date'], y=market_data[borrow_rate_type], mode='lines', name=f'{market}', line=dict(color=color)))
            if market_data['protocol'].iloc[0] == 'Blue':
                traces_rate_at_target.append(go.Scatter(x=market_data['date'], y=market_data['rate_at_target'], mode='lines', name=f'{market}', line=dict(color=color)))

    utilization_fig = go.Figure(data=traces_utilization)
    utilization_fig.update_layout(title=f'Evolution of Utilization for {selected_loan_asset}' if selected_loan_asset else 'Evolution of Utilization')

    borrow_rate_fig = go.Figure(data=traces_borrow_rate)
    borrow_rate_fig.update_layout(title=f'Evolution of Borrow Rate for {selected_loan_asset}' if selected_loan_asset else 'Evolution of Borrow Rate')

    rate_at_target_fig = go.Figure(data=traces_rate_at_target)
    rate_at_target_fig.update_layout(title=f'Evolution of Rate at Target for {selected_loan_asset}' if selected_loan_asset else 'Evolution of Rate at Target')

    return borrow_rate_fig, utilization_fig, rate_at_target_fig

# Function to update table
def update_table(selected_loan_asset, selected_markets):
    if selected_loan_asset:
        filtered_df = results[results['loan_asset'] == selected_loan_asset]
        if selected_markets:
            filtered_df = filtered_df[filtered_df['market'].isin(selected_markets)]
    else:
        filtered_df = results
    return filtered_df.drop('loan_asset', axis=1).to_dict('records')

# Function to update heatmap
def update_heatmap(selected_loan_asset, selected_rate_type, selected_markets):
    if selected_loan_asset and selected_rate_type and selected_markets:
        filtered_df = df_all[(df_all['loan_asset'] == selected_loan_asset) & (df_all['market'].isin(selected_markets))]
        pivot_df = filtered_df.pivot(index='date', columns='market', values=selected_rate_type)
        pivot_df = pivot_df.fillna(method='ffill')

        def pairwise_corr(df):
            corr_matrix = pd.DataFrame(index=df.columns, columns=df.columns)
            for col1 in df.columns:
                for col2 in df.columns:
                    valid_idx = df[[col1, col2]].dropna().index
                    if len(valid_idx) > 1:  # Ensure there are at least 2 valid points
                        corr_matrix.at[col1, col2] = df.loc[valid_idx, col1].corr(df.loc[valid_idx, col2])
                    else:
                        corr_matrix.at[col1, col2] = np.nan
            return corr_matrix

        correlation_matrix = pairwise_corr(pivot_df)
        fig = px.imshow(
            correlation_matrix,
            text_auto=True,
            aspect="auto",
            color_continuous_scale=px.colors.sequential.RdBu[::-1],
            zmin=-1,
            zmax=1
        )
        fig.update_layout(title=f'{selected_rate_type.replace("_", " ").title()} Correlation Heatmap for {selected_loan_asset}')
    else:
        fig = go.Figure()
        fig.update_layout(title='Select a loan asset, rate type, and markets to see the correlation heatmap')

    return fig

# Render content based on the selected tab
if tab == 'Graphs':
    if loan_asset and selected_markets and rate_type:
        borrow_rate_fig, utilization_fig, rate_at_target_fig = update_graphs(loan_asset, selected_markets, rate_type)
        st.plotly_chart(borrow_rate_fig)
        st.plotly_chart(utilization_fig)
        st.plotly_chart(rate_at_target_fig)
    else:
        st.write('Please select a loan asset, rate type, and markets.')
elif tab == 'Metrics Table':
    if loan_asset and selected_markets:
        table_data = update_table(loan_asset, selected_markets)
        st.dataframe(pd.DataFrame(table_data))
    else:
        st.write('Please select a loan asset and markets.')
elif tab == 'Correlation Heatmap':
    if loan_asset and rate_type and selected_markets:
        heatmap_fig = update_heatmap(loan_asset, rate_type, selected_markets)
        st.plotly_chart(heatmap_fig)
    else:
        st.write('Please select a loan asset, rate type, and markets.')

if __name__ == '__main__':
    st.write("Run the app using `streamlit run your_script_name.py`")