import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import ssl
import json
import http.client
import certifi
import time
import pytz
import pickle
import smtplib
from st_aggrid import AgGrid
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
from pandasql import sqldf
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# **** Create alert if data-manager is not running ****
    # Give option to open data-manager url and allow it to run in background

# CURRENT DATA
    # current_df = AB-Power-data-manager.read_current_data()

# HISTORICAL DATA
@st.experimental_memo(suppress_st_warning=True)
def query_historical_data():
    query = '''
    SELECT
        DATETIME(
            EXTRACT (YEAR FROM timeStamp), 
            EXTRACT (MONTH FROM timeStamp),
            EXTRACT (DAY FROM timeStamp),
            EXTRACT (HOUR FROM timeStamp), 0, 0) AS timeStamp,
        fuelType,
        EXTRACT (YEAR FROM timeStamp) AS year,
        EXTRACT (MONTH FROM timeStamp) AS month,
        EXTRACT (DAY FROM timeStamp) AS day,
        EXTRACT (HOUR FROM timeStamp) AS hour,
        AVG(value) AS value
    FROM nrgdata.historical_data
    WHERE timeStamp BETWEEN
        DATE_SUB(TIMESTAMP(current_date(),'America/Edmonton'), INTERVAL 7 DAY) AND 
        TIMESTAMP(current_date(),'America/Edmonton')
    GROUP BY fuelType, year, month, day, hour, timeStamp
    ORDER BY fuelType, year, month, day, hour, timeStamp
    '''
    history_df = bigquery.Client(credentials=credentials).query(query).to_dataframe()
    history_df['timeStamp'] = history_df['timeStamp'].dt.tz_localize('utc', ambiguous=True, nonexistent='shift_forward')
    history_df['timeStamp'] = history_df['timeStamp'].dt.tz_convert(tz)
    return history_df

def read_historical_data():
    # Check when historical data was last added to BigQuery
    if 'last_history_update' not in st.session_state:
        query = 'SELECT MAX(timeStamp) FROM nrgdata.historical_data'
        last_history_update = bigquery.Client(credentials=credentials).query(query).to_dataframe().iloc[0][0]
        last_history_update = last_history_update.tz_convert(tz)
        st.session_state['last_history_update'] = last_history_update
    # Insert data to BQ from when it was last updated to yesterday
    if st.session_state['last_history_update'].date() < (datetime.now(tz).date()-timedelta(days=1)):
        query_historical_data.clear()
        history_df = query_historical_data()
        st.session_state['last_history_update'] = max(history_df['timeStamp'])
    else:
        history_df = query_historical_data()
    return history_df

# Set global parameters
tz = pytz.timezone('America/Edmonton')
credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])

placeholder = st.empty()
for seconds in range(10):
    history_df = read_historical_data()
    history_df

    # with placeholder.container():

    time.sleep(1)
# DAILY OUTAGE DATA
    # daily_outage_df = Pull today's version of current daily outages from BQ
    # old_daily_outage_df = Pull a week ago's version of current daily outages from BQ

# MONTHLY OUTAGE DATA
    # monthly_outages = Pull today's version of monthly outages from BQ

# KPIs

# CURRENT SUPPLY CHART

# 7-DAY DAILY OUTAGES CHART

# 90-DAY DAILY OUTAGES CHART

# 2-YEAR MONTHLY OUTAGES CHART

# DAILY OUTAGE DIFFS CHART

# MONTHLY OUTAGE DIFFS CHART