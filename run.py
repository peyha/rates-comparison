import streamlit as st
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from metrics import *
from scipy.stats import pearsonr

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
    'Select a rolling window',
    ['hourly rolling avg', 'daily rolling avg', 'weekly rolling avg']
)

dict_borrow_rate_type = {
    'hourly rolling avg': 'borrowApy',
    'daily rolling avg': 'borrowApy_daily',
    'weekly rolling avg': 'borrowApy_weekly'
}
dic_utilization_type = {
    'hourly rolling avg': 'utilization',
    'daily rolling avg': 'utilization_daily',
    'weekly rolling avg': 'utilization_weekly'
}

# Input for minimum total supply USD
min_totalSupplyUSD = st.slider(
    'Minimum Total Supply USD', min_value=0, max_value=100_000_000, value=0, step=1_000_000)
# Filter markets by minimum total supply USD
filtered_markets_df = df_all[(df_all['loan_asset'] == loan_asset) & (
    df_all['totalSupplyUSD'] > min_totalSupplyUSD)]
markets = filtered_markets_df['market'].unique()

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


def update_graphs(selected_loan_asset, selected_markets, rolling_window):
    traces_utilization = []
    traces_supply_rate = []
    traces_borrow_rate = []
    traces_rate_at_target = []

    if selected_loan_asset:
        filtered_df = df_all[df_all['loan_asset'] == selected_loan_asset]

        if selected_markets:
            filtered_df = filtered_df[filtered_df['market'].isin(
                selected_markets)]

        unique_markets = filtered_df['market'].unique()

        color_map = {market: px.colors.qualitative.Plotly[i % len(px.colors.qualitative.Plotly)]
                     for i, market in enumerate(unique_markets)}

        for market in unique_markets:
            market_data = filtered_df[filtered_df['market'] == market]
            color = color_map[market]
            if rolling_window:
                traces_supply_rate.append(go.Scatter(
                    x=market_data['date'], y=market_data['supplyApy'], mode='lines', name=f'{market}', line=dict(color=color)))
                traces_borrow_rate.append(go.Scatter(
                    x=market_data['date'], y=market_data[dict_borrow_rate_type[rolling_window]], mode='lines', name=f'{market}', line=dict(color=color)))
                traces_utilization.append(go.Scatter(
                    x=market_data['date'], y=market_data[dic_utilization_type[rolling_window]], mode='lines', name=f'{market}', line=dict(color=color)))

            if market_data['protocol'].iloc[0] == 'Blue':
                traces_rate_at_target.append(go.Scatter(
                    x=market_data['date'], y=market_data['rate_at_target'], mode='lines', name=f'{market}', line=dict(color=color)))

    utilization_fig = go.Figure(data=traces_utilization)
    utilization_fig.update_layout(
        title=f'Evolution of Utilization for {selected_loan_asset}' if selected_loan_asset else 'Evolution of Utilization')

    borrow_rate_fig = go.Figure(data=traces_borrow_rate)
    borrow_rate_fig.update_layout(
        title=f'Evolution of Borrow Rate for {selected_loan_asset}' if selected_loan_asset else 'Evolution of Borrow Rate')

    supply_rate_fig = go.Figure(data=traces_supply_rate)
    supply_rate_fig.update_layout(
        title=f'Evolution of Supply Rate for {selected_loan_asset}' if selected_loan_asset else 'Evolution of Supply Rate')

    rate_at_target_fig = go.Figure(data=traces_rate_at_target)
    rate_at_target_fig.update_layout(
        title=f'Evolution of Rate at Target for {selected_loan_asset}' if selected_loan_asset else 'Evolution of Rate at Target')

    return borrow_rate_fig, supply_rate_fig, utilization_fig, rate_at_target_fig

# Function to update table


def update_table(selected_loan_asset, selected_markets):
    if selected_loan_asset:
        filtered_df = results[results['loan_asset'] == selected_loan_asset]
        if selected_markets:
            filtered_df = filtered_df[filtered_df['market'].isin(
                selected_markets)]
    else:
        filtered_df = results
    return filtered_df.drop('loan_asset', axis=1).to_dict('records')

# Function to update heatmap


def pairwise_corr_with_pvalues(df):
    corr_matrix = pd.DataFrame(index=df.columns, columns=df.columns)
    pvalue_matrix = pd.DataFrame(index=df.columns, columns=df.columns)

    for col1 in df.columns:
        for col2 in df.columns:
            valid_idx = df[[col1, col2]].dropna().index
            if len(valid_idx) > 1:  # Ensure there are at least 2 valid points
                corr, pvalue = pearsonr(
                    df.loc[valid_idx, col1], df.loc[valid_idx, col2])
                corr_matrix.at[col1, col2] = corr
                pvalue_matrix.at[col1, col2] = pvalue
            else:
                corr_matrix.at[col1, col2] = np.nan
                pvalue_matrix.at[col1, col2] = np.nan

    return corr_matrix, pvalue_matrix


def update_heatmap(selected_loan_asset, rolling_window, selected_markets):
    if selected_loan_asset and rolling_window and selected_markets:
        filtered_df = df_all[(df_all['loan_asset'] == selected_loan_asset) & (
            df_all['market'].isin(selected_markets))]
        aggregated_df = filtered_df.groupby(['date', 'market'])[
            dict_borrow_rate_type[rolling_window]].mean().reset_index()
        pivot_df = aggregated_df.pivot(
            index='date', columns='market', values=dict_borrow_rate_type[rolling_window])
        pivot_df = pivot_df.fillna(method='ffill')

        correlation_matrix, pvalue_matrix = pairwise_corr_with_pvalues(
            pivot_df)

        # Create a combined matrix of correlation and p-values as strings
        combined_matrix = correlation_matrix.astype(str)

        fig = px.imshow(
            correlation_matrix.astype(float),
            text_auto=True,
            aspect="auto",
            color_continuous_scale=px.colors.sequential.RdBu[::-1],
            zmin=-1,
            zmax=1
        )

        # Update annotations with combined values
        fig.update_traces(
            text=combined_matrix.values,
            texttemplate="%{text}",
            hovertemplate="%{text}<extra></extra>"
        )

        fig.update_layout(
            title=f'{rolling_window.replace("_", " ").title()} Correlation Heatmap for {selected_loan_asset}')
    else:
        fig = go.Figure()
        fig.update_layout(
            title='Select a loan asset, rate type, and markets to see the correlation heatmap')

    return fig


# Render content based on the selected tab
if tab == 'Graphs':
    if loan_asset and selected_markets and rate_type:
        borrow_rate_fig, supply_rate_fig, utilization_fig, rate_at_target_fig = update_graphs(
            loan_asset, selected_markets, rate_type)
        st.plotly_chart(borrow_rate_fig)
        st.plotly_chart(supply_rate_fig)
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
