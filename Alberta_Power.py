from re import L
from turtle import width
import streamlit as st
import pandas as pd
import altair as alt
import ssl
import json
import http.client
import certifi
import time
from st_aggrid import AgGrid
from datetime import datetime, date, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
from pandasql import sqldf
import random

# Function to hide top and bottom menus on Streamlit app
def hide_menu(bool):
    if bool == True:
        hide_menu_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            .css-1j15ncu {visibility: hidden;}
            .css-14x9thb {visibility: hidden;}
            </style>
            """
        return st.markdown(hide_menu_style, unsafe_allow_html=True)

def warning(type, lst):
    if type == 'warning':
        background_color = 'rgba(214, 39, 40, .1)'
        border_color = 'rgba(214, 39, 40, .2)'
        text_color = 'rgba(214, 39, 40, 0.6)'
    elif type == 'alert':
        background_color = 'rgba(31, 119, 180, .1)'
        border_color = 'rgba(31, 119, 180, .2)'
        text_color = 'rgba(31, 119, 180, 0.6)'
    st.markdown(f'''<p style="border-radius: 6px;
     -webkit-border-radius: 6px;
     background-color: {background_color};
     background-position: 9px 0px;
     background-repeat: no-repeat;
     border: solid 1px {border_color};
     border-radius: 6px;
     line-height: 18px;
     overflow: hidden;
     font-size:24px;
     font-weight: bold;
     color: {text_color};
     text-align: center;
     padding: 15px 10px;">{lst}</p>''', unsafe_allow_html=True)

def getToken():
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
        # Calculate new expiry date
        tokenExpiry = datetime.now() + timedelta(seconds=5)
        #tokenExpiry = datetime.now() + timedelta(seconds=jsonData['expires_in'])
    elif res_code == 400:
        res.read()
        release_token(accessToken)
        getToken()
    else:
        res_data = res.read()
    conn.close()
    return accessToken, tokenExpiry

def release_token(accessToken):
    path = '/api/ReleaseToken'
    server = 'api.nrgstream.com'
    headers = {'Authorization': f'Bearer {accessToken}'}
    context = ssl.create_default_context(cafile=certifi.where())
    conn = http.client.HTTPSConnection(server,context=context)
    conn.request('DELETE', path, None, headers)
    res = conn.getresponse()

@st.experimental_memo
def get_streamInfo(streamId):
    streamInfo = pd.read_csv('stream_codes.csv')
    streamInfo = streamInfo[streamInfo['streamId']==streamId]
    return streamInfo

def http_connect():
    server = 'api.nrgstream.com'
    context = ssl.create_default_context(cafile=certifi.where())
    conn = http.client.HTTPSConnection(server, context=context)
    return conn

def pull_data(fromDate, toDate, streamId, accessToken, tokenExpiry):
    # Setup the path for data request
    conn = http_connect()
    path = f'/api/StreamData/{streamId}?fromDate={fromDate}&toDate={toDate}'
    headers = {'Accept': 'Application/json', 'Authorization': f'Bearer {accessToken}'}
    conn.request('GET', path, None, headers)
    res = conn.getresponse()
    if res.code != 200:
        res.read()
        conn.close()
        release_token(accessToken)
        accessToken, tokenExpiry = getToken()
        pull_data(fromDate, toDate, streamId, accessToken, tokenExpiry)
    # Load json data & create pandas df
    else:
        jsonData = json.loads(res.read().decode('utf-8'))
        df = pd.json_normalize(jsonData, record_path='data')
        # Rename df cols
        df.rename(columns={0:'timeStamp', 1:'value'}, inplace=True)
        # Add streamInfo cols to df
        streamInfo = get_streamInfo(streamId)
        assetCode = streamInfo.iloc[0,1]
        streamName = streamInfo.iloc[0,2]
        fuelType = streamInfo.iloc[0,3]
        subfuelType = streamInfo.iloc[0,4]
        timeInterval = streamInfo.iloc[0,5]
        intervalType = streamInfo.iloc[0,6]
        df = df.assign(streamId=streamId, assetCode=assetCode, streamName=streamName, fuelType=fuelType, \
                        subfuelType=subfuelType, timeInterval=timeInterval, intervalType=intervalType)
        # Changing 'value' col to numeric and filling in NA's with previous value in col
        df.replace(to_replace={'value':''}, value=0, inplace=True)
        df['value'] = pd.to_numeric(df['value'])
        df.fillna(method='ffill', inplace=True)
        conn.close()
    return df

# Pull current day data from NRG
@st.experimental_memo(suppress_st_warning=True, ttl=20)
def current_data():
    streamIds = [86, 322684, 322677, 87, 85, 23695, 322665, 23694, 120, 124947, 122]
    realtime_df = pd.DataFrame([])
    today = datetime.now()
    for streamId in streamIds:
        accessToken, tokenExpiry = getToken()
        APIdata = pull_data(today.strftime('%m/%d/%Y'), today.strftime('%m/%d/%Y'), streamId, accessToken, tokenExpiry)
        APIdata['timeStamp'] = pd.to_datetime(APIdata['timeStamp'])
        realtime_df = pd.concat([realtime_df, APIdata], axis=0)
        release_token(accessToken)
    return realtime_df, today

# Create KPIs
def kpi(left_df, right_df, title):
    # Merging KPIs into one dataframe
    kpi_df = left_df.merge(right_df, how='left', on='fuelType', suffixes=('Previous','Current'))
    # Creating KPI delta calculation
    kpi_df['delta'] = kpi_df['valueCurrent'] - kpi_df['valuePrevious']
    kpi_df['absDelta'] = abs(kpi_df['delta'])
    # Formatting numbers 
    kpi_df.iloc[:,1:] = kpi_df.iloc[:,1:].applymap('{:.0f}'.format)
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11 = st.columns(11)
    with col1:
        st.subheader(title)
    col2.metric(label=kpi_df.iloc[7,0], value=kpi_df.iloc[7,2], delta=kpi_df.iloc[7,3]) # Natural Gas
    col3.metric(label=kpi_df.iloc[5,0], value=kpi_df.iloc[5,2], delta=kpi_df.iloc[5,3]) # Hydro
    col4.metric(label=kpi_df.iloc[4,0], value=kpi_df.iloc[4,2], delta=kpi_df.iloc[4,3]) # Energy Storage
    col5.metric(label=kpi_df.iloc[9,0], value=kpi_df.iloc[9,2], delta=kpi_df.iloc[9,3]) # Solar
    col6.metric(label=kpi_df.iloc[10,0], value=kpi_df.iloc[10,2], delta=kpi_df.iloc[10,3]) # Wind
    col7.metric(label=kpi_df.iloc[3,0], value=kpi_df.iloc[3,2], delta=kpi_df.iloc[3,3]) # Dual Fuel
    col8.metric(label=kpi_df.iloc[2,0], value=kpi_df.iloc[2,2], delta=kpi_df.iloc[2,3]) # Coal
    col9.metric(label=kpi_df.iloc[0,0], value=kpi_df.iloc[0,2], delta=kpi_df.iloc[0,3]) # BC
    col10.metric(label=kpi_df.iloc[6,0], value=kpi_df.iloc[6,2], delta=kpi_df.iloc[6,3]) # Montanta
    col11.metric(label=kpi_df.iloc[8,0], value=kpi_df.iloc[8,2], delta=kpi_df.iloc[8,3]) # Sask
    return kpi_df

# Pull historical data from Google BigQuery
@st.experimental_memo
def pull_grouped_hist():
    # Google BigQuery auth
    credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
    # Pull data
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
    FROM nrgdata.hourly_data
    WHERE timeStamp BETWEEN DATE_SUB(current_date(), INTERVAL 7 DAY) AND current_date()
    GROUP BY fuelType, year, month, day, hour, timeStamp
    ORDER BY fuelType, year, month, day, hour, timeStamp
    '''
    history_df = bigquery.Client(credentials=credentials).query(query).to_dataframe()
    return history_df

@st.experimental_memo(suppress_st_warning=True, ttl=600)
def outages():
    streamIds = [44648, 118361, 322689, 118362, 147262, 322675, 322682, 44651]
    years = [datetime.now().year, datetime.now().year+1, datetime.now().year+2]
    outages_df = pd.DataFrame([])
    for streamId in streamIds:
        accessToken, tokenExpiry = getToken() 
        for year in years:    
            APIdata = pull_data(date(year,1,1).strftime('%m/%d/%Y'), date(year+1,1,1).strftime('%m/%d/%Y'), streamId, accessToken, tokenExpiry)
            APIdata['timeStamp'] = pd.to_datetime(APIdata['timeStamp'])
            outages_df = pd.concat([outages_df, APIdata], axis=0)
        release_token(accessToken)
    outages_df.drop(['streamId','assetCode','streamName','subfuelType','timeInterval','intervalType'],axis=1,inplace=True)
    return outages_df

def make_alert_chart(df, fuelType, theme):
    chart = alt.Chart(df).mark_bar(opacity=0.7, color=theme[fuelType]).encode(
            x=alt.X('yearmonth(timeStamp):T', axis=alt.Axis(labelAngle=90), title=''),
            y=alt.Y('value:Q', title='MW'),
        ).properties(height=150)
    return chart

def alert_charts(diff, theme):
    st.subheader('Intertie & Outage Alerts')
    for fuelType in alert_list:
        st.write(fuelType)
        gt0 = make_alert_chart(diff[['timeStamp','fuelType','gt0']].rename(columns={'gt0':'value'}), fuelType, theme)
        lt0 = make_alert_chart(diff[['timeStamp','fuelType','lt0']].rename(columns={'lt0':'value'}), fuelType, theme)
        line = alt.Chart(pd.DataFrame({'y':[0]})).mark_rule().encode(y='y')
        st.altair_chart(gt0+lt0+line, use_container_width=True)

def alerts():
    pass
    
# App config
st.set_page_config(layout='wide', initial_sidebar_state='collapsed', menu_items=None)
theme = {'Biomass & Other':'#1f77b4', 
            'Coal':'#aec7e8',
            'Dual Fuel':'#ff7f0e',
            'Energy Storage':'#ffbb78',
            'Hydro':'#2ca02c',
            'Natural Gas':'#98df8a',
            'Solar':'#d62728',
            'Wind':'#7f7f7f'}
hide_menu(True)

old_outage_df = outages().astype({'timeStamp':'datetime64[ns]','value':'int64','fuelType':'object'})
alert_list = []

placeholder = st.empty()
for seconds in range(100000):
    # Pull live data
    # try:
    #     realtime_df, last_update = current_data()
    # except:
    #     with st.spinner('Gathering Live Data Streams'):
    #         time.sleep(10)
    #     realtime_df, last_update = current_data()
    # Pull outage data
    # try:
    #   outage_df = outages().astype({'timeStamp':'datetime64[ns]','value':'int64','fuelType':'object'})
    # except:
    #   with st.spinner('Gathering Intertie & Outage Data'):
    #         time.sleep(10)
    #   outage_df = outages().astype({'timeStamp':'datetime64[ns]','value':'int64','fuelType':'object'})
    outage_df = pd.read_csv('./offsets_changes.csv').astype({'timeStamp':'datetime64[ns]','value':'int64','fuelType':'object'})       
    diff = pd.merge(outage_df, old_outage_df, on=['timeStamp','fuelType'], suffixes=('_new','_old'))
    diff['diff_value'] = diff['value_old'] - diff['value_new']
    diff['gt0'] = [i if i > 0 else 0 for i in diff['diff_value']]
    diff['lt0'] = [i if i < 0 else 0 for i in diff['diff_value']]

    with placeholder.container():      
    # KPIs
        # current_query = '''
        # SELECT
        #     strftime('%Y-%m-%d %H:00:00', timeStamp) AS timeStamp,
        #     fuelType,
        #     strftime('%Y', timeStamp) AS year,
        #     strftime('%m', timeStamp) AS month,
        #     strftime('%d', timeStamp) AS day,
        #     strftime('%H', timeStamp) AS hour,
        #     AVG(value) AS value
        # FROM realtime_df
        # GROUP BY fuelType, year, month, day, hour
        # ORDER BY fuelType, year, month, day, hour, timeStamp
        # '''
        # current_df = sqldf(current_query, locals()).astype({'fuelType':'object', 'year':'int64','month':'int64', 'day':'int64', 'hour':'int64', 'value':'float64', 'timeStamp':'datetime64[ns]'})
        
        # # Real Time KPIs
        # realtime = realtime_df[['fuelType','value','timeStamp']][realtime_df['timeStamp']==max(realtime_df['timeStamp'])]
        # if len(realtime) < 8:
        #     realtime = realtime_df[['fuelType','value','timeStamp']][realtime_df['timeStamp']==max(realtime_df['timeStamp']-timedelta(0,300,0))]
        # realtime.drop('timeStamp', axis=1, inplace=True)
        # realtime = realtime.astype({'fuelType':'object','value':'float64'})
        # previousHour = current_df[['fuelType','value']][current_df['hour']==datetime.now().hour-1]
        # currentHour = current_df[['fuelType','value']][current_df['hour']==datetime.now().hour]
        
        # kpi_df = kpi(previousHour, realtime, 'Real Time')
        # kpi(previousHour, currentHour, 'Hourly Average')
        # warning_list = list(kpi_df['fuelType'][kpi_df['absDelta'].astype('int64') >= 50])

        # st.write(f"Last update: {last_update.strftime('%a, %b %d @ %X')}")
        # # KPI warning box
        # col1, col2 = st.columns(2)
        # # Real time alerts
        # with col1:
        #     if len(warning_list) > 0:
        #         for _ in range(len(warning_list)):
        #             warning('warning', f'{warning_list[_]}')
        # # Outage & intertie alerts
        # with col2:
        #     if len(alert_list) > 0:
        #         for _ in range(len(alert_list)):
        #             warning('alert', f'{alert_list[_]}')

    # # 14 day hist/real-time/forecast
    #     st.subheader('Real-time Supply')
    #     current_query = '''
    #     SELECT
    #         strftime('%Y-%m-%d %H:00:00', timeStamp) AS timeStamp,
    #         fuelType,
    #         strftime('%Y', timeStamp) AS year,
    #         strftime('%m', timeStamp) AS month,
    #         strftime('%d', timeStamp) AS day,
    #         strftime('%H', timeStamp) AS hour,
    #         AVG(value) AS value
    #     FROM realtime_df
    #     WHERE fuelType NOT IN ('BC','Montana','Saskatchewan')
    #     GROUP BY fuelType, year, month, day, hour
    #     ORDER BY fuelType, year, month, day, hour, timeStamp
    #     '''
    #     current_df= sqldf(current_query, locals()).astype({'fuelType':'object', 'year':'int64','month':'int64', 'day':'int64', 'hour':'int64', 'value':'float64', 'timeStamp':'datetime64[ns]'})
    #     # Pull last 7 days data
    #     history_df = pull_grouped_hist()
    #     # Combine last 7 days & live dataframes
    #     combo_df = pd.concat([history_df,current_df], axis=0)
    #     query = 'SELECT * FROM combo_df ORDER BY fuelType'
    #     combo_df = sqldf(query, globals())
    #     # Base combo_df bar chart
    #     combo_area = alt.Chart(combo_df).mark_area(color='grey', opacity=0.7).encode(
    #         x=alt.X('timeStamp:T', title=''),
    #         y=alt.Y('value:Q', title='Current Supply (MW)'),
    #         color=alt.Color('fuelType:N', scale=alt.Scale(domain=list(theme.keys()),range=list(theme.values())), legend=alt.Legend(orient="top"))
    #     ).properties(height=400)
    #     st.altair_chart(combo_area, use_container_width=True)

    # Outages chart
        st.subheader('Monthly Forecasted Outages')
        # Outages area chart
        outage_area = alt.Chart(outage_df).mark_bar(opacity=0.7).encode(
            x=alt.X('yearmonth(timeStamp):T', title='', axis=alt.Axis(labelAngle=90)),
            y=alt.Y('value:Q', stack='zero', axis=alt.Axis(format=',f'), title='Outages (MW)'),
            color=alt.Color('fuelType:N', scale=alt.Scale(domain=list(theme.keys()),range=list(theme.values())), legend=alt.Legend(orient="top")),
            tooltip=['fuelType','value','timeStamp']
            )
        st.altair_chart(outage_area, use_container_width=True)
        
        if (abs(diff['diff_value']) > 100).any():
            alert_list = list(set(diff['fuelType'][abs(diff['diff_value'])>0]))
            alert_charts(diff, theme)
        
        warning_list = []
        time.sleep(1)