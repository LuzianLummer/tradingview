import streamlit as st
import pandas as pd
from datetime import date, timedelta
from tradingview.transform.transformer import DataTransformer
import plotly.express as px
from tradingview.common.utilfunctions import read_tickers_from_file

st.set_page_config(page_title="Trading Dashboard", layout="wide")

st.title("Trading Dashboard (Prototype)")
st.caption("Minimal Streamlit app wired for containerization. Data integration to follow.")

@st.cache_resource
def get_transformer():
    return DataTransformer()

with st.sidebar:
    st.header("Filter")
    tickers = read_tickers_from_file()
    today = date.today()
    if tickers:
        default_index = tickers.index("AAPL") if "AAPL" in tickers else 0
        symbol = st.selectbox("Symbol", options=tickers, index=default_index)
    else:
        symbol = st.text_input("Symbol", "AAPL")
    start_d = st.date_input("Start", value=None, min_value=date(1980,1,1), max_value=today)
    end_d = st.date_input("End", value=None, max_value=today)
    run = st.button("Load")

if run and symbol:
    dt = get_transformer()
    records = dt.fetch_data_from_db(symbol, pd.to_datetime(start_d), pd.to_datetime(end_d))
    if not records:
        st.warning("No data for selected range.")
    else:
        df = pd.DataFrame([
            {
                "timestamp": r.timestamp,
                "open": r.open,
                "high": r.high,
                "low": r.low,
                "close": r.close,
                "volume": r.volume,
                "market_state": r.market_state,
            }
            for r in records
        ])
        st.subheader(f"{symbol} Close price")
        fig = px.line(df, x="timestamp", y="close", color="market_state", title=f"{symbol} Close Price")
        fig.update_yaxes(range=[0, None])
        fig.update_layout(uirevision=True)
        with st.expander("Show raw data"):
            st.dataframe(df, use_container_width=True)
        st.plotly_chart(fig, use_container_width=True)

