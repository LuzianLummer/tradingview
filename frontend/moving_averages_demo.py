"""
Demo showing how to use moving averages in Streamlit.
This file demonstrates both approaches: pre-calculated and on-the-fly.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from tradingview.transform.moving_averages import MovingAverageCalculator, calculate_moving_averages_for_symbol


def display_chart_with_precalculated_ma(symbol: str):
    """Display chart using pre-calculated moving averages from database."""
    st.subheader("Pre-calculated Moving Averages (Fast)")
    
    # Get data with pre-calculated MAs
    calculator = MovingAverageCalculator()
    df = calculator.fetch_data_for_symbol(symbol)
    
    if df.empty:
        st.error(f"No data found for {symbol}")
        return
    
    # Create chart
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, 
                       vertical_spacing=0.03, subplot_titles=(f'{symbol} Price', 'Volume'),
                       row_width=[0.7, 0.3])

    # Candlestick chart
    fig.add_trace(go.Candlestick(x=df['timestamp'],
                                open=df['open'],
                                high=df['high'],
                                low=df['low'],
                                close=df['close'],
                                name='OHLC'), row=1, col=1)

    # Add pre-calculated moving averages
    colors = ['red', 'blue', 'green', 'orange', 'purple', 'brown']
    periods = [5, 10, 20, 50, 100, 200]
    
    for i, period in enumerate(periods):
        if f'sma_{period}' in df.columns and not df[f'sma_{period}'].isna().all():
            fig.add_trace(go.Scatter(x=df['timestamp'], 
                                   y=df[f'sma_{period}'],
                                   mode='lines',
                                   name=f'SMA {period}',
                                   line=dict(color=colors[i % len(colors)])), row=1, col=1)
        
        if f'ema_{period}' in df.columns and not df[f'ema_{period}'].isna().all():
            fig.add_trace(go.Scatter(x=df['timestamp'], 
                                   y=df[f'ema_{period}'],
                                   mode='lines',
                                   name=f'EMA {period}',
                                   line=dict(color=colors[i % len(colors)], dash='dash')), row=1, col=1)

    # Volume
    fig.add_trace(go.Bar(x=df['timestamp'], y=df['volume'], name='Volume'), row=2, col=1)

    fig.update_layout(
        title=f'{symbol} with Moving Averages',
        yaxis_title='Price',
        yaxis2_title='Volume',
        xaxis_rangeslider_visible=False
    )

    st.plotly_chart(fig, use_container_width=True)


def display_chart_with_onthefly_ma(symbol: str, custom_periods: list):
    """Display chart with moving averages calculated on-the-fly."""
    st.subheader("On-the-fly Moving Averages (Flexible)")
    
    # Get data and calculate MAs on-the-fly
    df = calculate_moving_averages_for_symbol(symbol, custom_periods)
    
    if df.empty:
        st.error(f"No data found for {symbol}")
        return
    
    # Create chart
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, 
                       vertical_spacing=0.03, subplot_titles=(f'{symbol} Price', 'Volume'),
                       row_width=[0.7, 0.3])

    # Candlestick chart
    fig.add_trace(go.Candlestick(x=df['timestamp'],
                                open=df['open'],
                                high=df['high'],
                                low=df['low'],
                                close=df['close'],
                                name='OHLC'), row=1, col=1)

    # Add on-the-fly calculated moving averages
    colors = ['red', 'blue', 'green', 'orange', 'purple', 'brown']
    
    for i, period in enumerate(custom_periods):
        if f'SMA_{period}' in df.columns:
            fig.add_trace(go.Scatter(x=df['timestamp'], 
                                   y=df[f'SMA_{period}'],
                                   mode='lines',
                                   name=f'SMA {period}',
                                   line=dict(color=colors[i % len(colors)])), row=1, col=1)
        
        if f'EMA_{period}' in df.columns:
            fig.add_trace(go.Scatter(x=df['timestamp'], 
                                   y=df[f'EMA_{period}'],
                                   mode='lines',
                                   name=f'EMA {period}',
                                   line=dict(color=colors[i % len(colors)], dash='dash')), row=1, col=1)

    # Volume
    fig.add_trace(go.Bar(x=df['timestamp'], y=df['volume'], name='Volume'), row=2, col=1)

    fig.update_layout(
        title=f'{symbol} with Custom Moving Averages',
        yaxis_title='Price',
        yaxis2_title='Volume',
        xaxis_rangeslider_visible=False
    )

    st.plotly_chart(fig, use_container_width=True)


def main():
    st.title("Moving Averages Demo")
    
    # Symbol selection
    symbol = st.text_input("Enter Symbol:", value="AAPL")
    
    if st.button("Load Data"):
        if symbol:
            # Pre-calculated approach
            display_chart_with_precalculated_ma(symbol)
            
            # On-the-fly approach
            custom_periods = st.multiselect(
                "Select custom periods for on-the-fly calculation:",
                options=[3, 7, 14, 21, 30, 60, 90],
                default=[7, 21, 60]
            )
            
            if custom_periods:
                display_chart_with_onthefly_ma(symbol, custom_periods)
            
            # Performance comparison
            st.subheader("Performance Comparison")
            col1, col2 = st.columns(2)
            
            with col1:
                st.info("**Pre-calculated Approach:**\n- Fast loading\n- Limited to standard periods\n- Requires database updates")
            
            with col2:
                st.info("**On-the-fly Approach:**\n- Flexible periods\n- Slower for large datasets\n- No storage overhead")


if __name__ == "__main__":
    main()
