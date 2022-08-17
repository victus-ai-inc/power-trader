import streamlit as st
import pandas as pd
import altair as alt
import ssl
import json
import http.client
import certifi
import time
import alerts
from st_aggrid import AgGrid
from datetime import datetime, date, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
from pandasql import sqldf

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
    streamIds = [86, 322684, 322677, 87, 85, 23695, 322665, 23694]
    current_df = pd.DataFrame([])
    today = datetime.now()
    for id in streamIds:
        accessToken, tokenExpiry = getToken()
        APIdata = pull_data(today.strftime('%m/%d/%Y'), today.strftime('%m/%d/%Y'), id, accessToken, tokenExpiry)
        APIdata['timeStamp'] = pd.to_datetime(APIdata['timeStamp'])
        current_df = pd.concat([current_df, APIdata], axis=0)
        release_token(accessToken)
    current_query = '''
        SELECT
            strftime('%Y-%m-%d %H:00:00', timeStamp) AS timeStamp,
            fuelType,
            strftime('%Y', timeStamp) AS year,
            strftime('%m', timeStamp) AS month,
            strftime('%d', timeStamp) AS day,
            strftime('%H', timeStamp) AS hour,
            AVG(value) AS value
        FROM current_df
        GROUP BY fuelType, year, month, day, hour
        ORDER BY fuelType, year, month, day, hour, timeStamp
        '''
    current_df = sqldf(current_query, locals())
    return current_df.astype({'fuelType':'object', 'year':'int64','month':'int64', 'day':'int64', 'hour':'int64', 'value':'float64', 'timeStamp':'datetime64[ns]'}), today

# Create KPIs
def kpi(current_df):
    kpi_query = '''
        SELECT
            AVG(value) AS value,
            fuelType,
            year, month, day, hour
        FROM current_df
        GROUP BY fuelType, year, month, day, hour
        ORDER BY fuelType, year, month, day, hour
    '''
    kpi_df = sqldf(kpi_query, globals())
    # Pull current and last hour KPIs
    current_hour = kpi_df[['fuelType','value']][kpi_df['hour']==datetime.now().hour]
    previous_hour = kpi_df[['fuelType','value']][kpi_df['hour']==datetime.now().hour-1]
    # Merging current and last hour KPIs into one dataframe
    kpi_df = previous_hour.merge(current_hour, how='left', on='fuelType', suffixes=('Previous','Current'))
    # Creating KPI delta calculation
    kpi_df['delta'] = kpi_df['valueCurrent'] - kpi_df['valuePrevious']
    # Creating list of warnings
    kpi_df['absDelta'] = abs(kpi_df['delta'])
    warning_list = list(kpi_df['fuelType'][kpi_df['absDelta'] > 50])
    # Formatting numbers 
    kpi_df.iloc[:,1:] = kpi_df.iloc[:,1:].applymap('{:.0f}'.format)
    return kpi_df, warning_list

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

# App config
st.set_page_config(layout='wide', initial_sidebar_state='collapsed', menu_items=None)
st.title('Alberta Power Supply/Demand')
hide_menu(True)

placeholder = st.empty()
for seconds in range(100000):
    # Pull live data
    try:
        current_df, last_update = current_data()
    except:
        with st.spinner('Gathering Live Data Streams'):
            time.sleep(10)
        current_df, last_update = current_data()
    with placeholder.container():
    # KPIs
        # Create dataframe for KPIs from current_df
        st.subheader('Current Supply - Hourly Average (MW)')
        kpi_df, warning_list = kpi(current_df)
        # Displaying KPIs
        col1, col2, col3, col4, col5, col6, col7, col8 = st.columns(8)
        col1.metric(label=kpi_df.iloc[0,0], value=kpi_df.iloc[0,2], delta=kpi_df.iloc[0,3]) # Biomass & Other
        col2.metric(label=kpi_df.iloc[1,0], value=kpi_df.iloc[1,2], delta=kpi_df.iloc[1,3]) # Coal
        col3.metric(label=kpi_df.iloc[2,0], value=kpi_df.iloc[2,2], delta=kpi_df.iloc[2,3]) # Dual Fuel
        col4.metric(label=kpi_df.iloc[3,0], value=kpi_df.iloc[3,2], delta=kpi_df.iloc[3,3]) # Energy Storage
        col5.metric(label=kpi_df.iloc[4,0], value=kpi_df.iloc[4,2], delta=kpi_df.iloc[4,3]) # Hydro
        col6.metric(label=kpi_df.iloc[5,0], value=kpi_df.iloc[5,2], delta=kpi_df.iloc[5,3]) # Natural Gas
        col7.metric(label=kpi_df.iloc[6,0], value=kpi_df.iloc[6,2], delta=kpi_df.iloc[6,3]) # Solar
        col8.metric(label=kpi_df.iloc[7,0], value=kpi_df.iloc[7,2], delta=kpi_df.iloc[7,3]) # Wind
        st.write(f"Last update: {last_update.strftime('%a, %b %d @ %X')}")
        # KPI warning box
        if len(warning_list) > 0:
            l = len(warning_list)
            for _ in range(l):
                st.error(f'{warning_list[_]} has a differential greater than 50 MW over the previous hour.')

# 14 day hist/real-time/forecast
        st.subheader('Real-time Supply')
        # Pull last 7 days data
        history_df = pull_grouped_hist()
        # Combine last 7 days & live dataframes
        combo_df = pd.concat([history_df,current_df], axis=0)
        query = 'SELECT * FROM combo_df ORDER BY fuelType'
        combo_df = sqldf(query, globals())
        # Base combo_df bar chart
        combo_area = alt.Chart(combo_df).mark_area(color='grey', opacity=0.7).encode(
            x=alt.X('timeStamp:T', title=''),
            y=alt.Y('value:Q', title='Current Supply (MW)'),
            color=alt.Color('fuelType:N', scale=alt.Scale(scheme='category20'), legend=alt.Legend(orient="top")),
            tooltip=['fuelType:N','timeStamp:T','hour:O', 'value:Q']
        ).properties(height=400)
        st.altair_chart(combo_area, use_container_width=True)