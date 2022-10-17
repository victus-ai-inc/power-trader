import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.cbook as cbook
import ssl
import json
import http.client
import certifi
import pytz
import pickle
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from google.oauth2 import service_account
from google.cloud import bigquery
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage

@st.experimental_singleton(suppress_st_warning=True)
def firestore_db_instance():
    firebase_admin.initialize_app(credential=credentials.Certificate(st.secrets["gcp_service_account"]))
    try:
        firebase_admin.initialize_app(credential=credentials.Certificate(st.secrets["gcp_service_account"]))
    except:
        firebase_admin.get_app()
    return firestore.client()

def read_firestore(db, document):
    firestore_ref = db.collection(u'appData').document(document)
    df = pd.DataFrame.from_dict(firestore_ref.get().to_dict())
    df['fuelType'] = df['fuelType'].astype('category')
    df['timeStamp'] = df['timeStamp'].dt.tz_convert('America/Edmonton')
    return df

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

def update_historical_data():
    historicalData_ref = db.collection(u'appData').document('historicalData')
    if 'last_history_update' not in st.session_state:
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
    streamIds = [86, 322684, 322677, 87, 85, 23695, 322665, 23694, 120, 124947, 122, 1]
    current_df = get_data(streamIds, datetime.now(tz).date(), datetime.now(tz).date()+timedelta(days=1))
    currentData_ref = db.collection(u'appData').document('currentData')
    currentData_ref.set(current_df.to_dict('list'))

def diff_calc(outageTable, old_df, new_df):
    diff_df = pd.merge(old_df, new_df, on=['timeStamp','fuelType'], suffixes=('_new','_old'))
    diff_df['diff_value'] = diff_df['value_old'] - diff_df['value_new']
    diff_df = diff_df[['timeStamp','fuelType','diff_value']]
    if outageTable == 'dailyOutages':
        diff_df = diff_df[diff_df['timeStamp'].dt.date < datetime.now(tz).date() + timedelta(days=90)]
    elif outageTable == 'monthlyOutages':
        diff_df = diff_df[diff_df['timeStamp'].dt.date > datetime.now(tz).date() + relativedelta(months=3, day=1, days=-1)]
    diff_df = diff_df[diff_df['diff_value'] > 100]
    return diff_df

def alertChart(diff_df):
    #title = diff_df['fuelType'][0]

    df = diff_df[['timeStamp','diff_value']]
    df
    df = df.set_index('timeStamp')
    df
    x = df.index.values
    y = df['diff_value'].values
    y
    upper = np.ma.masked_where(y <= 0, y)
    lower = np.ma.masked_where(y > 0, y)

    fig, ax = plt.subplots()
    ax.bar(x,y,width=1./24)
    st.pyplot(fig)
    st.stop()
    return picture

def text_alert(picture):
    email = st.secrets['email_address']
    pas = st.secrets['email_password']
    sms_gateways = st.secrets['phone_numbers'].values()
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(email, pas)
    for gateway in sms_gateways:
        msg = MIMEMultipart()
        msg['To'] = gateway
        body = picture
        msg.attach(MIMEText(body, 'plain'))
        server.sendmail(email, gateway, msg.as_string())

@st.experimental_memo(suppress_st_warning=True, ttl=180)
def update_daily_outages():
    # Pull last update from FS
    oldOutages = read_firestore(db,'dailyOutages')
    # Import new data
    streamIds = [124]
    intertie_outages = get_data(streamIds, datetime.now(tz).date(), datetime.now(tz).date() + relativedelta(months=12, day=1, days=-1))
    intertie_outages['value'] = max(intertie_outages['value']) - intertie_outages['value']
    streamIds = [118366, 118363, 322685, 118365, 118364, 322667, 322678, 147263]
    stream_outages = get_data(streamIds, datetime.now(tz).date(), datetime.now(tz).date() + relativedelta(months=4, day=1))
    stream_outages = stream_outages.pivot(index='timeStamp',columns='fuelType',values='value').asfreq(freq='H', method='ffill').reset_index()
    stream_outages = stream_outages.melt(id_vars='timeStamp',value_vars=['Biomass & Other','Coal','Dual Fuel','Energy Storage','Hydro','Natural Gas'])
    newOutages = pd.concat([intertie_outages,stream_outages])
    newOutages['fuelType'] = newOutages['fuelType'].astype('category')
    dailyOutages_ref = db.collection(u'appData').document('dailyOutages')
    dailyOutages_ref.set(newOutages.to_dict('list'))
    # Calc diffs
    diff_df = diff_calc('dailyOutages', oldOutages, newOutages)

@st.experimental_memo(suppress_st_warning=True, ttl=300)
def update_monthly_outages():
    # # Pull last update from FS
    # oldOutages = read_firestore(db,'monthlyOutages')
    # # Import new data
    # streamIds = [44648, 118361, 322689, 118362, 147262, 322675, 322682, 44651]
    # years = [datetime.now(tz).year, datetime.now(tz).year+1, datetime.now(tz).year+2]
    # newOutages = pd.DataFrame([])
    # for year in years:
    #     df = get_data(streamIds, date(year,1,1), date(year+1,1,1))
    #     newOutages = pd.concat([newOutages, df], axis=0)
    # newOutages = newOutages[newOutages['timeStamp'].dt.date>(datetime.now(tz).date())]
    # monthlyOutages_ref = db.collection(u'appData').document('monthlyOutages')
    # monthlyOutages_ref.set(newOutages.to_dict('list'))
    # diff_df = diff_calc('monthlyOutages', oldOutages, newOutages)
    diff_df = pd.DataFrame({'timeStamp':[datetime.now(tz)+relativedelta(day=1,hour=10,minute=0,second=0,microsecond=0),
                            datetime.now(tz)+relativedelta(day=1,hour=11,minute=0,second=0,microsecond=0),
                            datetime.now(tz)+relativedelta(day=5,hour=12,minute=30,second=0,microsecond=0)],
                            'fuelType':['Solar','Wind','Solar'],'diff_value':[1000,500,-1000]})
    diff_df['fuelType'] = diff_df['fuelType'].astype('category')
    diff_df
    text_alert(alertChart(diff_df))
    st.stop()
    # **** MERGE NEW OUTAGE DATA INTO BQ ****

# OUTAGES
    # Schedule delete of dates that are older than the 5 latest dates in BQ


# OUTAGE DIFFS
# Pull current day outages from FS
# Create diff_df
    # If there are diffs then update BQ and send alert

# WIND SOLAR FORECAST TO FS
#streamIds = [102225, 293354]
#wind_solar = get_data(streamIds, datetime.now(tz).date(), datetime.now(tz).date() + relativedelta(days=7))

# MAIN APP CODE
st.title('Alberta Power Data Manager')

# Set timezone
tz = pytz.timezone('America/Edmonton')
# Google BigQuery auth
db = firestore_db_instance()

st.header('HISTORY')
with st.spinner('Updating historical data...'):
    update_historical_data()
st.success(f"Historical data has been updated from {st.session_state['last_history_update'].strftime('%a, %b %d')} to {(datetime.today()-timedelta(days=1)).strftime('%a, %b %d')}.")

placeholder = st.empty()
for seconds in range(300000):
    with placeholder.container():
        # **** data will relaod every second but only rerun when the ttl is up ****
        # st.write('---')
        # st.header('DAILY OUTAGES')
        # with st.spinner('Updating daily outages...'):
        #     update_daily_outages()
        # st.success(f"Daily data updated")
        
        st.write('---')
        st.header('MONTHLY OUTAGES')
        with st.spinner('Updating monthly outages...'):
            update_monthly_outages()
        st.success(f"Monthly data updated")
        
        # st.write('---')
        # st.header('CURRENT DATA')
        # with st.spinner('Updating current data...'):
        #     update_current_data()
        # st.success(f"Current data updated")
st.experimental_rerun()