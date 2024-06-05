import pandas as pd
import numpy as np
import os
import time
import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output, State
import plotly.express as px
import plotly.graph_objects as go
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

app = dash.Dash(__name__, suppress_callback_exceptions=True)

app.layout = html.Div([
    html.Div([
        dcc.Dropdown(
            id='loan-asset-selector',
            options=[{'label': asset, 'value': asset} for asset in df_all['loan_asset'].unique()],
            placeholder="Select a loan asset",
        ),
        dcc.Dropdown(
            id='rate-type-selector',
            options=[
                {'label': 'Borrow Rate', 'value': 'borrow_rate'},
                {'label': 'Hourly Rolling Avg', 'value': 'hourly_rolling_avg'},
                {'label': 'Daily Rolling Avg', 'value': 'daily_rolling_avg'},
                {'label': 'Weekly Rolling Avg', 'value': 'weekly_rolling_avg'}
            ],
            placeholder="Select a rate type",
        ),
        html.Button('Select Markets', id='toggle-button', n_clicks=0, style={'background-color': 'white', 'color': 'black'}),
        html.Div(id='market-checkbox-container', style={'display': 'none'}),
    ]),
    dcc.Tabs(id='tabs', value='tab-1', children=[
        dcc.Tab(label='Graphs', value='tab-1'),
        dcc.Tab(label='Metrics Table', value='tab-2'),
        dcc.Tab(label='Correlation Heatmap', value='tab-3'),
    ]),
    html.Div(id='tabs-content')
])

@app.callback(
    Output('market-checkbox-container', 'style'),
    Input('toggle-button', 'n_clicks'),
    State('market-checkbox-container', 'style')
)
def toggle_market_checkboxes(n_clicks, current_style):
    if n_clicks % 2 == 1:
        return {'display': 'block'}
    return {'display': 'none'}

@app.callback(
    Output('market-checkbox-container', 'children'),
    Input('loan-asset-selector', 'value')
)
def update_market_checkboxes(selected_loan_asset):
    if selected_loan_asset:
        markets = df_all[df_all['loan_asset'] == selected_loan_asset]['market'].unique()
        checkboxes = dcc.Checklist(
            id='market-checkboxes',
            options=[{'label': market, 'value': market} for market in markets],
            value=markets,  # Initially select all markets
            inline=False,  # Display one row at a time
            labelStyle={'color': 'black', 'backgroundColor': 'white'}
        )
        return checkboxes
    return []

@app.callback(
    Output('tabs-content', 'children'),
    [Input('tabs', 'value'),
     Input('loan-asset-selector', 'value'),
     Input('rate-type-selector', 'value'),
     Input('market-checkboxes', 'value')]
)
def render_content(tab, selected_loan_asset, selected_rate_type, selected_markets):
    if tab == 'tab-1':
        if selected_loan_asset and selected_markets and selected_rate_type:
            borrow_rate_fig, utilization_fig, rate_at_target_fig = update_graphs(selected_loan_asset, selected_markets, selected_rate_type)
            return html.Div([
                dcc.Graph(figure=borrow_rate_fig),
                dcc.Graph(figure=utilization_fig),
                dcc.Graph(figure=rate_at_target_fig)
            ])
        else:
            return 'Please select a loan asset, rate type, and markets.'
    elif tab == 'tab-2':
        if selected_loan_asset and selected_markets:
            table_data = update_table(selected_loan_asset, selected_markets)
            return dash_table.DataTable(
                id='metrics-table',
                columns=[{'name': col, 'id': col} for col in results.drop('loan_asset', axis=1).columns],
                data=table_data,
                sort_action='native',
                filter_action='native',
                page_size=20,
            )
        else:
            return 'Please select a loan asset and markets.'
    elif tab == 'tab-3':
        if selected_loan_asset and selected_rate_type and selected_markets:
            heatmap_fig = update_heatmap(selected_loan_asset, selected_rate_type, selected_markets)
            return dcc.Graph(figure=heatmap_fig)
        else:
            return 'Please select a loan asset, rate type, and markets.'

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

def update_table(selected_loan_asset, selected_markets):
    if selected_loan_asset:
        filtered_df = results[results['loan_asset'] == selected_loan_asset]
        if selected_markets:
            filtered_df = filtered_df[filtered_df['market'].isin(selected_markets)]
    else:
        filtered_df = results
    return filtered_df.drop('loan_asset', axis=1).to_dict('records')

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

if __name__ == '__main__':
    app.run_server(debug=True)
