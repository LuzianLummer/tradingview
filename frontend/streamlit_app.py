import streamlit as st
import pandas as pd
from datetime import date, timedelta, datetime
from tradingview.transform.transformer import DataTransformer
import plotly.express as px
from tradingview.common.utilfunctions import read_tickers_from_file
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    
    # Fix date range - end date should not be in the future
    start_d = st.date_input("Start", value=date(2023, 1, 1), min_value=date(1980,1,1), max_value=today)
    end_d = st.date_input("End", value=today, max_value=today)
    
    # Add debug info
    st.subheader("Debug Info")
    st.write(f"Available tickers: {len(tickers) if tickers else 0}")
    st.write(f"Selected symbol: {symbol}")
    st.write(f"Date range: {start_d} to {end_d}")
    
    run = st.button("Load")

if run and symbol:
    try:
        dt = get_transformer()
        
        # Convert dates to datetime
        start_datetime = datetime.combine(start_d, datetime.min.time())
        end_datetime = datetime.combine(end_d, datetime.max.time())
        
        st.info(f"Fetching data for {symbol} from {start_datetime} to {end_datetime}")
        
        records = dt.fetch_data_from_db(symbol, start_datetime, end_datetime)
        
        st.write(f"Found {len(records)} records")
        
        if not records:
            st.warning("No data for selected range.")
            st.info("Try running the data ingestion DAGs first:")
            st.code("docker-compose exec airflow-webserver airflow dags trigger daily_market_insert")
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
            
    except Exception as e:
        st.error(f"Error fetching data: {str(e)}")
        st.exception(e)

