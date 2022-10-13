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
from pandasql import sqldf
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

@st.experimental_singleton(suppress_st_warning=True)
def firestore_db_instance():
    firebase_admin.initialize_app(credential=credentials.Certificate(st.secrets["gcp_service_account"]))
    try:
        firebase_admin.initialize_app(credential=credentials.Certificate(st.secrets["gcp_service_account"]))
    except:
        firebase_admin.get_app()
    return firestore.client()

def get_token():
    try:
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
            #default_pickle['accessToken'] = accessToken
            with open('./accessToken.pickle', 'wb') as token:
                pickle.dump(accessToken, token, protocol=pickle.HIGHEST_PROTOCOL)
        elif res_code == 400:
            res.read()
            with open('./accessToken.pickle', 'rb') as token:
                accessToken = pickle.load(token)
            release_token(accessToken)
            get_token()
        else:
            res_data = res.read()
        conn.close()
    except:
        pass
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
def update_historical_data():
    historicalData_ref = db.collection(u'appData').document('historicalData')
    if 'last_history_update' not in st.session_state:
        st.write('not in ss')
        df = pd.DataFrame.from_dict(historicalData_ref.get().to_dict())
        df['timeStamp'] = df['timeStamp'].dt.tz_convert('America/Edmonton')
        st.session_state['last_history_update'] = min(df['timeStamp'])
    if st.session_state['last_history_update'].date() < (datetime.now(tz).date()-timedelta(days=7)):
        st.warning('HISTORY BEING UPDATED')
        streamIds = [86, 322684, 322677, 87, 85, 23695, 322665, 23694, 120, 124947, 122, 1]
        startDate = datetime.now(tz).date()-timedelta(days=7)
        endDate = datetime.now(tz).date()
        history_df = get_data(streamIds, startDate, endDate)
        historicalData_ref.set(history_df.to_dict('list'))
        st.session_state['last_history_update'] = min(history_df['timeStamp'])

@st.experimental_memo(suppress_st_warning=True, ttl=15)
def update_current_data():
    # Pull current day's data from NRG
    streamIds = [86, 322684, 322677, 87, 85, 23695, 322665, 23694, 120, 124947, 122, 1]
    current_df = get_data(streamIds, datetime.now(tz).date(), datetime.now(tz).date()+timedelta(days=1))
    last_update = datetime.now(tz)
    # Update current_df data in Firestore DB
    currentData_ref = db.collection(u'appData').document('currentData')
    currentData_ref.set(current_df.to_dict('list'))
    return current_df, last_update

    # streamIds = [86, 322684, 322677, 87, 85, 23695, 322665, 23694, 120, 124947, 122, 1]
    # startDate = datetime.now(tz).date()-timedelta(days=7)
    # endDate = datetime.now(tz).date()
    # history_df = get_data(streamIds, startDate, endDate)
    # st.write(min(history_df['timeStamp']))
    # historicalData_ref.set(history_df.to_dict('list'))
    # st.stop()

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

# MAIN APP CODE
st.title('Alberta Power Data Manager')

# Set timezone
tz = pytz.timezone('America/Edmonton')
# Google BigQuery auth
db = firestore_db_instance()

placeholder = st.empty()
for seconds in range(300000):
    with placeholder.container():
        # **** data will relaod every second but only rerun when the ttl is up ****
        st.header('HISTORY')
        with st.spinner('Updating historical data...'):
            update_historical_data()
        st.success(f"Historical data has been updated from {st.session_state['last_history_update'].strftime('%a, %b %d')} to {(datetime.today()-timedelta(days=1)).strftime('%a, %b %d')}.")
        st.write('---')
        st.header('CURRENT DATA')
        with st.spinner('Updating current data...'):
            current_df, last_update = update_current_data()
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