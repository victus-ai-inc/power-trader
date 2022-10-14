import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import ssl
import json
import http.client
import certifi
import pytz
import pickle
import smtplib
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
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
            res_code
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
    streamInfo = pd.read_csv('stream_codes.csv', usecols=['streamId','fuelType'], dtype={'streamId':'Int64','fuelType':'category'})
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
        pull_NRG_data(fromDate, toDate, streamId, accessToken)
    try:
        jsonData = json.loads(response.read().decode('utf-8'))
    except json.JSONDecodeError:
        st.experimental_rerun()
    conn.close()
    df = pd.json_normalize(jsonData, record_path='data')
    # Rename df cols
    df.rename(columns={0:'timeStamp', 1:'value'}, inplace=True)
    df['timeStamp'] = pd.to_datetime(df['timeStamp']).dt.tz_localize(tz, ambiguous=True, nonexistent='shift_forward')
    # Add streamInfo cols to df
    streamInfo = get_streamInfo(streamId)
    fuelType = streamInfo.iloc[0,1]
    df = df.assign(fuelType=fuelType)
    df.replace(to_replace={'value':''}, value=0, inplace=True)
    df['value'] = df['value'].astype('float')
    df['fuelType'] = df['fuelType'].astype('category')   
    df.fillna(method='ffill', inplace=True)
    return df

def get_data(streamIds, start_date, end_date):
    #df = pd.DataFrame(columns=['timeStamp','value','fuelType'], dtype=[None])
    df = pd.DataFrame({col:pd.Series(dtype=typ) for col, typ in {'timeStamp':'datetime64[ns, America/Edmonton]','value':'float','fuelType':'category'}.items()})
    for streamId in streamIds:
        accessToken = get_token()
        APIdata = pull_NRG_data(start_date.strftime('%m/%d/%Y'), end_date.strftime('%m/%d/%Y'), streamId, accessToken)
        release_token(accessToken)
        df = pd.concat([df, APIdata], axis=0)
    df['fuelType'] = df['fuelType'].astype('category')
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

@st.experimental_memo(suppress_st_warning=True, ttl=300)
def monthly_outages():
    streamIds = [44648, 118361, 322689, 118362, 147262, 322675, 322682, 44651]
    years = [datetime.now(tz).year, datetime.now(tz).year+1, datetime.now(tz).year+2]
    monthly_outages = pd.DataFrame([])
    for year in years:
        df = get_data(streamIds, date(year,1,1), date(year+1,1,1))
        monthly_outages = pd.concat([monthly_outages, df], axis=0)
    monthly_outages = monthly_outages[monthly_outages['timeStamp'].dt.date>(datetime.now(tz).date())]
    monthlyOutages_ref = db.collection(u'appData').document('monthlyOutages')
    monthlyOutages_ref.set(monthly_outages.to_dict('list'))

@st.experimental_memo(suppress_st_warning=True, ttl=180)
def daily_outages():
    streamIds = [124]
    intertie_outages = get_data(streamIds, datetime.now(tz).date(), datetime.now(tz).date() + relativedelta(months=12, day=1, days=-1))
    intertie_outages['value'] = max(intertie_outages['value']) - intertie_outages['value']
    streamIds = [102225, 293354]
    wind_solar = get_data(streamIds, datetime.now(tz).date(), datetime.now(tz).date() + relativedelta(days=7))
    streamIds = [118366, 118363, 322685, 118365, 118364, 322667, 322678, 147263]
    stream_outages = get_data(streamIds, datetime.now(tz).date(), datetime.now(tz).date() + relativedelta(months=4, day=1))
    stream_outages = stream_outages.pivot(index='timeStamp',columns='fuelType',values='value').asfreq(freq='H', method='ffill').reset_index()
    stream_outages = stream_outages.melt(id_vars='timeStamp',value_vars=['Biomass & Other','Coal','Dual Fuel','Energy Storage','Hydro','Natural Gas','Solar','Wind'])
    daily_outages = pd.concat([intertie_outages,stream_outages,wind_solar])
    daily_outages['fuelType'] = daily_outages['fuelType'].astype('category')
    dailyOutages_ref = db.collection(u'appData').document('dailyOutages')
    dailyOutages_ref.set(daily_outages.to_dict('list'))

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
        st.header('DAILY OUTAGES')
        with st.spinner('Updating daily outages...'):
            daily_outages()
        st.success(f"Daily data updated")
        
        st.write('---')
        st.header('MONTHLY OUTAGES')
        with st.spinner('Updating monthly outages...'):
            monthly_outages()
        st.success(f"Monthly data updated")
        
        st.write('---')
        st.header('CURRENT DATA')
        with st.spinner('Updating current data...'):
            update_current_data()
        st.success(f"Current data updated")
st.experimental_rerun()