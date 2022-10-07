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

def get_token():
    username = st.secrets["nrg_username"]
    password = st.secrets["nrg_password"]
    server = 'api.nrgstream.com'
    tokenPath = '/api/security/token'
    tokenPayload = f'grant_type=password&username={username}&password={password}'
    headers = {"Content-type": "application/x-www-form-urlencoded"}
    # Connect to API server to get a token
    context = ssl.create_default_context(cafile=certifi.where())
    conn = http.client.HTTPSConnection(server,context=context)
    conn.request('POST', tokenPath, tokenPayload, headers)
    res = conn.getresponse()
    res_code = res.status
    # Check if the response is good
    if res_code == 200:
        res_data = res.read()
        # Decode the token into an object
        jsonData = json.loads(res_data.decode('utf-8'))
        accessToken = jsonData['access_token']
        # Put accessToken into session_state in case token is not successfully released
        st.session_state['accessToken'] = accessToken
    # If accessToken wasn't successfully released then pull from session_state
    elif res_code == 400:
        res.read()
        release_token(st.session_state['accessToken'])
        get_token()
    else:
        res_data = res.read()
    conn.close()
    return accessToken

def release_token(accessToken):
    path = '/api/ReleaseToken'
    server = 'api.nrgstream.com'
    headers = {'Authorization': f'Bearer {accessToken}'}
    context = ssl.create_default_context(cafile=certifi.where())
    conn = http.client.HTTPSConnection(server,context=context)
    conn.request('DELETE', path, None, headers)

@st.experimental_memo(suppress_st_warning=True)
def get_streamInfo(streamId):
    streamInfo = pd.read_csv('stream_codes.csv')
    streamInfo = streamInfo[streamInfo['streamId']==streamId]
    return streamInfo

def pull_NRG_data(fromDate, toDate, streamId, accessToken):
    server = 'api.nrgstream.com'
    context = ssl.create_default_context(cafile=certifi.where())
    conn = http.client.HTTPSConnection(server, context=context)
    path = f'/api/StreamData/{streamId}?fromDate={fromDate}&toDate={toDate}'
    headers = {'Accept': 'Application/json', 'Authorization': f'Bearer {accessToken}'}
    conn.request('GET', path, None, headers)
    response = conn.getresponse()
    if response.status != 200:
        conn.close()
        #time.sleep(2)
        pull_NRG_data(fromDate, toDate, streamId, accessToken)
    try:
        jsonData = json.loads(response.read().decode('utf-8'))
    except json.JSONDecodeError:
        st.experimental_rerun()
    conn.close()
    df = pd.json_normalize(jsonData, record_path='data')
    # Rename df cols
    df.rename(columns={0:'timeStamp', 1:'value'}, inplace=True)
    # Add streamInfo cols to df
    streamInfo = get_streamInfo(streamId)
    fuelType = streamInfo.iloc[0,3]
    df = df.assign(fuelType=fuelType)
    # Changing 'value' col to numeric and filling in NA's with previous value in col
    df.replace(to_replace={'value':''}, value=0, inplace=True)
    df['value'] = pd.to_numeric(df['value'])
    df.fillna(method='ffill', inplace=True)
    df['timeStamp'] = pd.to_datetime(df['timeStamp']).dt.tz_localize(tz, ambiguous=True, nonexistent='shift_forward')
    return df

def get_data(streamIds, start_date, end_date):
    df = pd.DataFrame([])
    for streamId in streamIds:
        accessToken = get_token()
        APIdata = pull_NRG_data(start_date.strftime('%m/%d/%Y'), end_date.strftime('%m/%d/%Y'), streamId, accessToken)
        release_token(accessToken)
        df = pd.concat([df, APIdata], axis=0)
    return df

# HISTORICAL
def update_historical_data(credentials):
    # Check when historical data was last added to BigQuery
    if 'last_history_update' not in st.session_state:
        query = 'SELECT MAX(timeStamp) FROM nrgdata.historical_data'
        last_history_update = bigquery.Client(credentials=credentials).query(query).to_dataframe().iloc[0][0]
        last_history_update = last_history_update.tz_convert(tz)
        st.session_state['last_history_update'] = last_history_update
    # Insert data to BQ from when it was last updated to yesterday
    if st.session_state['last_history_update'].date() < (datetime.now(tz).date()-timedelta(days=1)):
        streamIds = [86, 322684, 322677, 87, 85, 23695, 322665, 23694, 120, 124947, 122, 1]
        history_df = get_data(streamIds, last_history_update.date(), datetime.now(tz).date())
        bigquery.Client(credentials=credentials).load_table_from_dataframe(history_df, 'nrgdata.historical_data')
        st.session_state['last_history_update'] = max(history_df['timeStamp'])

# Pull data from BQ
@st.experimental_memo(suppress_st_warning=True)
def read_historical_data(credentials):
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
    history_df['timeStamp'] = history_df['timeStamp'].dt.tz_localize('utc',ambiguous=True, nonexistent='shift_forward')
    history_df['timeStamp'] = history_df['timeStamp'].dt.tz_convert(tz)
    return history_df

# OUTAGES
    # ****add the date when the outages were pulled to outage database****
    # @st.experimental_memo(suppress_st_warning=True, ttl=300)
    # def update_daily_outages():
        # daily_outage_old_df = pull latest (SELECT MAX(date_added)) outages that are currently stored in BQ
        # daily_outage_new_df = pull current outage data from NRG
        # Update and merge new_daily_outage_df to BQ every 5 min
        # return daily_outage_old_df, daily_outage_new_df
    
    # def daily_outage_diffs(daily_outage_old_df, daily_outage_new_df):
        # daily_outage_diff_df = check if there is a diff between daily_outage_old_df & daily_outage_new_df
        # Send alert charts for each stream that has a diff
            # render alert chart (as diff charts pic: https://altair-viz.github.io/user_guide/saving_charts.html)
            # send charts to users
        # Remove outages in BQ older than a week ago

# CURRENT
    # Refresh every 10 sec:
    # def update_current_data():
        # pull current_df from NRG
        # put current_df into session_state
            # st.session_state['current_df'] = current_df
        # clear read_current_data() memoization to refresh new current_df and make available to the apps
            # read_current_data.clear()

    # @st.experimental_memo()
    # def read_current_data():
        # Apps will always read from memo, and memo is only updated when new data is pulled 
        # read current_df from session_state
            # current_df = st.session_state['current_df']
        # return current_df

# MAIN APP CODE
# **** make text messages of when each element was last run ****
tz = pytz.timezone('America/Edmonton')
# Google BigQuery auth
credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])

placeholder = st.empty()
for seconds in range(100):
    with st.spinner('Updating historical data...'):
        update_historical_data(credentials)
    #with st.spinner('Updating current data...'):
        #pass
    #with st.spinner('Updating daily outages...'):
        #pass
    #with st.spinner('Updating monthly outages...'):
        #pass
    time.sleep(1)

