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
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from google.cloud import bigquery
from google.oauth2 import service_account
from pandasql import sqldf
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
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
    if res_code != 200:
        res_code
        res.read()
        if 'accessToken' in st.session_state:
            st.error('token in SS')
            release_token(st.session_state['accessToken'])
            time.sleep(1)
            st.experimental_rerun()
            get_token()
        else:
            st.error('token NOT in SS')
            time.sleep(1)
            #st.experimental_rerun()
            get_token()
    else:
        res_data = res.read()
        # Decode the token into an object
        jsonData = json.loads(res_data.decode('utf-8'))
        accessToken = jsonData['access_token']
        with open('./accessToken.pickle', 'wb') as token:
            pickle.dump(accessToken, token, protocol=pickle.HIGHEST_PROTOCOL)
        # Put accessToken into session_state in case token is not successfully released
        st.session_state['accessToken'] = accessToken
    # If accessToken wasn't successfully released then pull from session_state
    conn.close()
    return st.session_state['accessToken']

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
    start_date, end_date
    df = pd.DataFrame([])
    for streamId in streamIds:
        accessToken = get_token()
        APIdata = pull_NRG_data(start_date.strftime('%m/%d/%Y')-timedelta(days=1), end_date.strftime('%m/%d/%Y'), streamId, accessToken)
        release_token(accessToken)
        df = pd.concat([df, APIdata], axis=0)
    return df

# HISTORICAL
def update_historical_data():
    bq_cred = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
    # Check when historical data was last added to BigQuery
    if 'last_history_update' not in st.session_state:
        query = 'SELECT MAX(timeStamp) FROM nrgdata.historical_data'
        last_history_update = bigquery.Client(credentials=bq_cred).query(query).to_dataframe().iloc[0][0]
        last_history_update = last_history_update.tz_convert(tz)
        st.session_state['last_history_update'] = last_history_update
    # Insert data to BQ from when it was last updated to yesterday
    if st.session_state['last_history_update'].date() < (datetime.now(tz).date()-timedelta(days=1)):
        streamIds = [86, 322684, 322677, 87, 85, 23695, 322665, 23694, 120, 124947, 122, 1]
        last_history_update
        history_df = get_data(streamIds, last_history_update.date(), datetime.now(tz).date())
        bigquery.Client(credentials=bq_cred).load_table_from_dataframe(history_df, 'nrgdata.historical_data')
        st.session_state['last_history_update'] = max(history_df['timeStamp'])

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

@st.experimental_singleton()
def firestore_db_instance():
    st.warning('inside db')
    fb_cred = credentials.Certificate(st.secrets["gcp_service_account"])
    app = firebase_admin.initialize_app(fb_cred)
    db = firestore.client()
    return db

@st.experimental_memo(suppress_st_warning=True, ttl=15)
def update_current_data(_currentData_ref):
    # Pull current day's data from NRG
    streamIds = [86, 322684, 322677, 87, 85, 23695, 322665, 23694, 120, 124947, 122, 1]
    current_df = get_data(streamIds, datetime.now(tz).date(), datetime.now(tz).date()+timedelta(days=1))
    last_update = datetime.now(tz)
    # Update current_df data in Firestore DB
    currentData_ref.set(current_df.to_dict('list'), merge=True)
    return current_df, last_update

# MAIN APP CODE
st.title('Alberta Power Data Manager')

# Set timezone
tz = pytz.timezone('America/Edmonton')
# Google BigQuery auth
db = firestore_db_instance()
currentData_ref = db.collection(u'appData').document('currentData')
placeholder = st.empty()
for seconds in range(300000):
    with placeholder.container():
        # **** data will relaod every second but only rerun when the ttl is up ****
        st.header('HISTORY')
        with st.spinner('Updating historical data...'):
            update_historical_data()
        st.success(f"Historical data has been updated to: {st.session_state['last_history_update'].strftime('%a, %b %d @ %X')}")
        st.write('---')
        st.header('CURRENT DATA')
        with st.spinner('Updating current data...'):
            current_df, last_update = update_current_data(currentData_ref)
        st.success(f"Current data last updated: {last_update.strftime('%a, %b %d @ %X')}")
        st.write('---')
        st.header('DAILY OUTAGES')
        #with st.spinner('Updating daily outages...'):
            #pass
        st.write('---')
        st.header('MONTHLY OUTAGES')
        #with st.spinner('Updating monthly outages...'):
            #pass
        st.write('---')
    #time.sleep(1)
st.experimental_rerun()


