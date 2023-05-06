import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import ssl
import json
import http.client
import certifi
import pytz
import pickle
import time
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from google.oauth2 import service_account
from google.cloud import bigquery
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage

@st.cache_resource()
def firestore_db_instance():
    firebase_admin.initialize_app(credential=credentials.Certificate(dict(st.secrets["gcp_service_account"])))
    firebase_admin.get_app()
    return firestore.client()

def read_firestore(db, document):
    firestore_ref = db.collection(u'appData').document(document)
    df = pd.DataFrame.from_dict(firestore_ref.get().to_dict())
    df['fuelType'] = df['fuelType'].astype('category')
    df['timeStamp'] = df['timeStamp'].dt.tz_convert('America/Edmonton')
    return df

@st.cache_data()
def get_streamInfo(streamId):
    streamInfo = pd.read_csv('stream_codes.csv', usecols=['streamId','fuelType'], dtype={'streamId':'Int64','fuelType':'category'})
    streamInfo = streamInfo[streamInfo['streamId']==streamId]
    return streamInfo

def getData(streamIds, fromDate, toDate):

    def getToken(conn):
        NRGusername = st.secrets["nrg_username"]
        NRGpassword = st.secrets["nrg_password"]
        getTokenPayload = f'grant_type=password&username={NRGusername}&password={NRGpassword}'
        getTokenHeaders = {"Content-type": "application/x-www-form-urlencoded"}
        conn.request('POST', '/api/security/token', getTokenPayload, getTokenHeaders)
        getTokenResponse = conn.getresponse()
        if getTokenResponse.status != 200:
            releaseToken(conn)
            st.experimental_rerun()
        else:
            getTokenData = json.loads(getTokenResponse.read().decode('utf-8'))
            accessToken = getTokenData['access_token']
            with open('./accessToken.pickle', 'wb') as token:
                pickle.dump(accessToken, token, protocol=pickle.HIGHEST_PROTOCOL)
            return accessToken
    
    def getNRGdata(conn, accessToken, streamId, fromDate, toDate):
        NRGheaders = {'Accept': 'Application/json', 'Authorization': f'Bearer {accessToken}'}
        NRGpath = f'/api/StreamData/{streamId}?fromDate={fromDate}&toDate={toDate}'
        conn.request('GET', NRGpath, None, NRGheaders)
        NRGresponse = conn.getresponse()
        if NRGresponse.status != 200:
            releaseToken(conn)
            st.experimental_rerun()
        else:
            #st.write(f'steamID:{streamId} data res:{NRGresponse.status}')
            NRGdata = json.loads(NRGresponse.read().decode('utf-8'))
            return NRGdata
    
    def releaseToken(conn):
        with open('./accessToken.pickle', 'rb') as token:
            accessToken = pickle.load(token)
        releaseTokenHeaders = {'Authorization': f'Bearer {accessToken}'}
        conn.request('DELETE', '/api/ReleaseToken', None, releaseTokenHeaders)
        conn.close()

    df = pd.DataFrame({col:pd.Series(dtype=typ) for col, typ in {'timeStamp':'datetime64[ns, America/Edmonton]','value':'float','fuelType':'category'}.items()})
    
    server = 'api.nrgstream.com'
    context = ssl.create_default_context(cafile=certifi.where())
    for streamId in streamIds:
        conn = http.client.HTTPSConnection(server, context=context)
        accessToken = getToken(conn)
        NRGdata = getNRGdata(conn, accessToken, streamId, fromDate, toDate)
        releaseToken(conn)
        streamInfo = get_streamInfo(streamId)
        fuelType = streamInfo.iloc[0,1]
        new_df = pd.json_normalize(NRGdata, record_path='data')
        new_df = new_df.assign(fuelType=fuelType)
        new_df.rename(columns={0:'timeStamp', 1:'value'}, inplace=True)
        new_df
        new_df['timeStamp'] = pd.to_datetime(new_df['timeStamp'],errors='ignore', infer_datetime_format=True).dt.tz_localize(tz, ambiguous=True, nonexistent='shift_forward')
        new_df.replace(to_replace={'value':''}, value=0, inplace=True)
        df = pd.concat([df, new_df], axis=0, ignore_index=True)
        time.sleep(0.1)
    df['value'] = df['value'].astype('float')
    df['fuelType'] = df['fuelType'].astype('category')  
    df.fillna(method='ffill', inplace=True)
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
        history_df = getData(streamIds, startDate, endDate)
        historicalData_ref.set(history_df.to_dict('list'))
        st.session_state['last_history_update'] = min(history_df['timeStamp'])

@st.cache_data(ttl=20)
def update_current_data():
    streamIds = [86, 322684, 322677, 87, 85, 23695, 322665, 23694, 120, 124947, 122, 1]
    #current_df = get_data(streamIds, datetime.now(tz).date(), datetime.now(tz).date()+timedelta(days=1))
    current_df = getData(streamIds, datetime.now(tz).date(), datetime.now(tz).date()+timedelta(days=1))
    currentData_ref = db.collection(u'appData').document('currentData')
    currentData_ref.set(current_df.to_dict('list'))
    st.success(f"Current data updated: {datetime.now(tz).strftime('%b %d @ %H:%M:%S')}")

def alerts(outageTable, diff_df):
    def baseChart(df):
        x = df['timeStamp']
        y = df['pos']
        y2 = df['neg']
        fuelType = df['fuelType'].unique()[0]
        fig, ax = plt.subplots()
        ax.set_axisbelow(True)
        ax.grid(color='gray',alpha=0.2)
        locator = mdates.AutoDateLocator()
        formatter = mdates.ConciseDateFormatter(locator)
        ax.xaxis.set_major_locator(locator)
        ax.xaxis.set_major_formatter(formatter)
        if outageTable == 'dailyOutages':
            ax.bar(x,y, color='green', alpha=0.6, align='center')
            ax.bar(x,y2, color='red', alpha=0.6, align='center')
            ax.xaxis.set_minor_locator(mdates.DayLocator())
            if fuelType == 'Intertie':
                ax.set_title(f'Daily Outage Adjustments (next 12 months)\n{fuelType}')
            else:
                ax.set_title(f'Daily Outage Adjustments (next 3 months)\n{fuelType}')
        elif outageTable == 'monthlyOutages':
            ax.bar(x,y, color='green', width=20, alpha=0.6, align='center')
            ax.bar(x,y2, color='red', width=20, alpha=0.6, align='center')
            ax.set_title(f'Monthly Outage Adjustments (next 24 months)\n{fuelType}')
        ax.set_ylabel('MW')
        ax.legend(labels=['Net Increase','Net Decrease'])
        plt.savefig('outages.png',facecolor='white')

    def text_alert():
        from_email = st.secrets['email_address']
        from_pw = st.secrets['email_password']
        sms_gateways = st.secrets['phone_numbers'].values()
        msg = MIMEMultipart('alternative')
        part = MIMEImage(open('outages.png', 'rb').read())
        msg.attach(part)
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(from_email, from_pw)
        for recipient in sms_gateways:
            server.sendmail(from_email, recipient, msg.as_string())
        server.quit()

    def generateCharts(diff_df):
        for fuelType in diff_df['fuelType'].unique():
            df = diff_df[diff_df['fuelType']==fuelType]
            if df[(df['diff_value']>=100) | (df['diff_value']<=-100)]['diff_value'].astype(bool).sum(axis=0) != 0:
                baseChart(df)
                text_alert()

    generateCharts(diff_df)

def diff_calc(outageTable, old_df, new_df):
    diff_df = pd.merge(new_df, old_df, on=['timeStamp','fuelType'], suffixes=('_new','_old'))
    diff_df.loc[diff_df['value_old']==0,'value_new']=0
    diff_df['diff_value'] = diff_df['value_new'] - diff_df['value_old']
    diff_df = diff_df[['timeStamp','fuelType','diff_value']]
    if diff_df[(diff_df['diff_value']>=100) | (diff_df['diff_value']<=-100)]['diff_value'].astype(bool).sum(axis=0) != 0:
        if outageTable == 'dailyOutages':
            diff_df = diff_df.groupby('fuelType').resample('D',on='timeStamp').mean(numeric_only=True,skipna=True).reset_index()
        diff_df['pos'] = np.where(diff_df['diff_value']>=0,diff_df['diff_value'],0)
        diff_df['neg'] = np.where(diff_df['diff_value']<0,-diff_df['diff_value'],0)
        alerts(outageTable, diff_df)

def update_BigQuery_outages(outageTable, df):
    cred = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
    if 'last_'+outageTable+'_update' not in st.session_state:
        query = f'SELECT MAX(loadDate) FROM outages.{outageTable}'
        last_load_date = bigquery.Client(credentials=cred).query(query).to_dataframe().iloc[0,0]
        st.session_state['last_'+outageTable+'_update'] = last_load_date
    if st.session_state['last_'+outageTable+'_update'] < datetime.now(tz).date():
        st.warning('OUTAGES BEING UPDATED')
        df['loadDate'] = datetime.now(tz).date()
        bigquery.Client(credentials=cred).load_table_from_dataframe(df,f'outages.{outageTable}')
        st.session_state['last_'+outageTable+'_update'] = datetime.now(tz).date()

@st.cache_data(ttl=180)
def update_daily_outages():
    # Pull last update from FS & update BQ if necessary
    oldOutages = read_firestore(db,'dailyOutages')
    update_BigQuery_outages('dailyOutages', oldOutages)
    # Import new data
    streamIds = [124]
    intertie_outages = getData(streamIds, datetime.now(tz).date(), datetime.now(tz).date() + relativedelta(months=12, day=1, days=-1))
    intertie_outages['value'] = max(intertie_outages['value']) - intertie_outages['value']
    streamIds = [118366, 118363, 322685, 118365, 118364, 322667, 322678, 147263]
    stream_outages = getData(streamIds, datetime.now(tz).date(), datetime.now(tz).date() + relativedelta(months=4, day=1))
    stream_outages = stream_outages.pivot(index='timeStamp',columns='fuelType',values='value').asfreq(freq='H', method='ffill').reset_index()
    stream_outages = stream_outages.melt(id_vars='timeStamp',value_vars=['Biomass & Other','Coal','Dual Fuel','Energy Storage','Hydro','Natural Gas'])
    newOutages = pd.concat([intertie_outages,stream_outages],ignore_index=True)
    newOutages.drop_duplicates(['timeStamp','fuelType'],keep='last',inplace=True)
    newOutages['fuelType'] = newOutages['fuelType'].astype('category')
    dailyOutages_ref = db.collection('appData').document('dailyOutages')
    dailyOutages_ref.set(newOutages.to_dict('list'))
    diff_calc('dailyOutages', oldOutages, newOutages)
    st.success(f"Daily data updated: {datetime.now(tz).strftime('%b %d @ %H:%M:%S')}")

@st.cache_data(ttl=300)
def update_monthly_outages():
    # Pull last update from FS
    oldOutages = read_firestore(db,'monthlyOutages')
    update_BigQuery_outages('monthlyOutages', oldOutages)
    # Import new data
    streamIds = [44648, 118361, 322689, 118362, 147262, 322675, 322682, 44651]
    years = [datetime.now(tz).year, datetime.now(tz).year+1, datetime.now(tz).year+2]
    newOutages = pd.DataFrame([])
    for year in years:
        df = getData(streamIds, date(year,1,1), date(year+1,1,1))
        newOutages = pd.concat([newOutages, df], axis=0, ignore_index=True)
    newOutages = newOutages[newOutages['timeStamp'].dt.date>(datetime.now(tz).date())]
    newOutages['fuelType'] = newOutages['fuelType'].astype('category')
    monthlyOutages_ref = db.collection('appData').document('monthlyOutages')
    monthlyOutages_ref.set(newOutages.to_dict('list'))
    diff_calc('monthlyOutages', oldOutages, newOutages)
    st.success(f"Monthly data updated: {datetime.now(tz).strftime('%b %d @ %H:%M:%S')}")

# WIND SOLAR FORECAST TO FS
@st.cache_data(ttl=300)
def update_wind_solar():
    streamIds = [102225, 293354]
    wind_solar = getData(streamIds, datetime.now(tz).date(), datetime.now(tz).date() + relativedelta(days=8))
    windSolar_ref = db.collection('appData').document('windSolar')
    windSolar_ref.set(wind_solar.to_dict('list'))
    st.success(f"Wind & solar data updated: {datetime.now(tz).strftime('%b %d @ %H:%M:%S')}")

# MAIN APP CODE
st.title('Alberta Power Data Manager')
tz = pytz.timezone('America/Edmonton')
db = firestore_db_instance()

st.header('HISTORY')
with st.spinner('Updating historical data...'):
    update_historical_data()
st.success(f"Historical data has been updated from {st.session_state['last_history_update'].strftime('%a, %b %d')} to {(datetime.now(tz).date()-timedelta(days=1)).strftime('%a, %b %d')}.")

placeholder = st.empty()
for seconds in range(43200):
    # If time is midnight then len(getData())=0, wait until first data comes in at 00:05 to proceed
    twelveOhFiveAM = datetime.now(tz)+relativedelta(hour=0,minute=6,second=0,microsecond=0)
    if datetime.now(tz) <= twelveOhFiveAM:
        st.warning(f'Waiting until 12:05 to load first data of the day')
        time.sleep(twelveOhFiveAM-datetime.now(tz))
        
    with placeholder.container():
        st.write('---')
        st.header('DAILY OUTAGES')
        with st.spinner('Updating daily outages...'):
            update_daily_outages()
        
        st.write('---')
        st.header('MONTHLY OUTAGES')
        with st.spinner('Updating monthly outages...'):
            update_monthly_outages()
        
        st.write('---')
        st.header('WIND+SOLAR FORECAST')
        with st.spinner('Updating wing & solar forecasts...'):
            update_wind_solar()

        st.write('---')
        st.header('CURRENT DATA')
        with st.spinner('Updating current data...'):
            update_current_data()
st.experimental_rerun()