from email import header
import streamlit as st
import pandas as pd
import altair as alt
import ssl
import json
import http.client
import certifi
import time
import pickle
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
            with open('./access_token.pickle', 'wb') as handle:
                pickle.dump(accessToken, handle, protocol=pickle.HIGHEST_PROTOCOL)
            if 'last_token' not in st.session_state:
                st.session_state['last_token'] = accessToken
            # Calculate new expiry date
            tokenExpiry = datetime.now() + timedelta(seconds=jsonData['expires_in'])
        elif res_code == 400:
            res.read()
            release_token(accessToken)
            getToken()
        else:
            res_data = res.read()
        conn.close()
    except:
        with st.spinner('Attempting to access database...'):
            with open('./access_token.pickle', 'rb') as handle:
                last_token = pickle.load(handle)
            release_token(last_token)
            time.sleep(5)
            st.experimental_rerun()
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

def pull_data(fromDate, toDate, streamId, accessToken, tokenExpiry):
    server = 'api.nrgstream.com'
    context = ssl.create_default_context(cafile=certifi.where())
    conn = http.client.HTTPSConnection(server, context=context)
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

# Pull current day data (5 min intervals) from NRG
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

# Update and pull historical data from Google BigQuery
@st.experimental_memo(suppress_st_warning=True)
def pull_grouped_hist():
    # Google BigQuery auth
    credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
    # Check when data was last added to BigQuery
    query = 'SELECT MAX(timeStamp) FROM nrgdata.hourly_data'
    # Check when BigQuery was last updated
    last_update = bigquery.Client(credentials=credentials).query(query).to_dataframe().iloc[0][0]
    # Add data to BQ from when it was last updated to yesterday
    if last_update < (datetime.now()-timedelta(days=1)):
        pull_grouped_hist.clear()
        streamIds = [86, 322684, 322677, 87, 85, 23695, 322665, 23694, 120, 124947, 122]
        for streamId in streamIds:
            accessToken, tokenExpiry = getToken()
            APIdata = pull_data(last_update.strftime('%m/%d/%Y'), datetime.now().strftime('%m/%d/%Y'), streamId, accessToken, tokenExpiry)
            APIdata['timeStamp'] = pd.to_datetime(APIdata['timeStamp'])
            bigquery.Client(credentials=credentials).load_table_from_dataframe(APIdata, 'nrgdata.hourly_data')
            release_token(accessToken)
        alerts.sms2()
    # Pull data from BQ
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

@st.experimental_memo(suppress_st_warning=True, ttl=300)
def forecast_outages():
    # Pull monthly outages from NRG
    streamIds = [44648, 118361, 322689, 118362, 147262, 322675, 322682, 44651]
    years = [datetime.now().year, datetime.now().year+1, datetime.now().year+2]
    current_outage_df = pd.DataFrame([])
    for streamId in streamIds:
        accessToken, tokenExpiry = getToken()
        for year in years:
            APIdata = pull_data(date(year,1,1).strftime('%m/%d/%Y'), date(year+1,1,1).strftime('%m/%d/%Y'), streamId, accessToken, tokenExpiry)
            APIdata['timeStamp'] = pd.to_datetime(APIdata['timeStamp'])
            current_outage_df = pd.concat([current_outage_df, APIdata], axis=0)
        release_token(accessToken)
        time.sleep(1)
    current_outage_df.drop(['streamId','assetCode','streamName','subfuelType','timeInterval','intervalType'],axis=1,inplace=True)
    return current_outage_df

@st.experimental_memo(suppress_st_warning=True, ttl=300)
def current_outages():
    streamId = 124
    years = [datetime.now().year, datetime.now().year+1]
    current_outages_df = pd.DataFrame([])
    accessToken, tokenExpiry = getToken()
    for year in years:
        APIdata = pull_data(date(year,1,1).strftime('%m/%d/%Y'), date(year+1,1,1).strftime('%m/%d/%Y'), streamId, accessToken, tokenExpiry)
        APIdata['timeStamp'] = pd.to_datetime(APIdata['timeStamp'])
        current_outages_df = pd.concat([current_outages_df, APIdata], axis=0)
    release_token(accessToken)
    current_outages_df.drop(['streamId','assetCode','streamName','subfuelType','timeInterval','intervalType'],axis=1,inplace=True)
    with open('./intertie.pickle', 'wb') as handle:
        pickle.dump(current_outages_df, handle, protocol=pickle.HIGHEST_PROTOCOL)
    return current_outages_df

@st.experimental_memo(suppress_st_warning=True, ttl=10)
def outage_alerts():
    # Unpickle the alerts dict
    with open('./alerts.pickle', 'rb') as handle:
        alert_pickle = pickle.load(handle)
        # alert_pickle dictionay is of the form {'dates':{...}, 'outage_dfs':[...], 'intertie':[...]}
        # alert_pickle['dates'] is a dictionary where the dates of the latest offset and intertie alerts are stored
        # alert_pickle['outage_dfs'] is a list of len=7 which stores a copy of the previous 7 days of outage_dfs, used to compare to the present day outage_df
    # If the outage_df at the top of the alert_pickle['outage_dfs'] list is older than a week then pop this outage_df and add new outage_df to end of list
    if datetime.now().date() > (alert_pickle['outage_dfs'][0][0] + timedelta(days=6)):
        alert_pickle['outage_dfs'].pop(0)
        forecast_outages.clear()
        new_outage_df = forecast_outages().astype({'timeStamp':'datetime64[ns]','value':'int64','fuelType':'object'})
        alert_pickle['outage_dfs'].append((datetime.now().date(), new_outage_df))
        old_outage_df = alert_pickle['outage_dfs'][0][1]
    # If the most current new_outage_df has already been updated in alert_pickle then just pull the oldest df from the top of the list
    else:
        #outage_df = alert_pickle['outage_dfs'][0][1]
        old_outage_df = pd.read_csv('offsets_changes.csv').astype({'timeStamp':'datetime64[ns]','value':'int64','fuelType':'object'})
    # Create outage_diff df that compares the realtime outages_df to the outages_df from 7 days ago
    outage_diff = pd.merge(old_outage_df, current_outage_df, on=['timeStamp','fuelType'], suffixes=('_new','_old'))
        # Calculate any differentials between the current and previous outage_df
    outage_diff['diff_value'] = outage_diff['value_old'] - outage_diff['value_new']
        # Split the calculated differtials into values that are greater than and less than zero (for charting purposes)
    outage_diff['gt0'] = [i if i > 0 else 0 for i in outage_diff['diff_value']]
    outage_diff['lt0'] = [i if i < 0 else 0 for i in outage_diff['diff_value']]
    # Create list of fuelTypes that have differential greater than the cutoff
    alert_list = list(set(outage_diff['fuelType'][abs(outage_diff['diff_value'])>cutoff]))
    # Update alerts_pickle['dates'] dict if last warning listed is greater than 7 days ago
    for i in alert_list:
        if (datetime.now() - timedelta(days=7)) > alert_pickle['dates'][i]:
            alert_pickle['dates'][i] = datetime.now()
            alerts.sms()
    # Repickle after changes have been made to alerts_pickle dict
    with open('./alerts.pickle', 'wb') as handle:
        pickle.dump(alert_pickle, handle, protocol=pickle.HIGHEST_PROTOCOL)
    # Create dictionary of differentials that have been created in the last 7 days
    alert_dict = {k:v for k,v in alert_pickle['dates'].items() if v > (datetime.now()-timedelta(7,0,0))}
    return outage_diff, alert_dict

def make_alert_chart(df, fuelType, theme):
    chart = alt.Chart(df).mark_bar(opacity=0.7, color=theme[fuelType]).encode(
            x=alt.X('yearmonth(timeStamp):T', axis=alt.Axis(labelAngle=90), title=''),
            y=alt.Y('value:Q', title='MW'),
        ).properties(height=150)
    return chart

def alert_charts(outage_diff, theme):
    st.subheader('Intertie & Outage Alerts')
    for fuelType in alert_dict.keys():
        st.write(fuelType)
        gt0 = outage_diff[['timeStamp','fuelType','gt0']][outage_diff['fuelType']==fuelType]
        lt0 = outage_diff[['timeStamp','fuelType','lt0']][outage_diff['fuelType']==fuelType]
        gt0 = make_alert_chart(gt0.rename(columns={'gt0':'value'}), fuelType, theme)
        lt0 = make_alert_chart(lt0.rename(columns={'lt0':'value'}), fuelType, theme)
        line = alt.Chart(pd.DataFrame({'y':[0]})).mark_rule().encode(y='y')
        st.altair_chart(gt0+lt0+line, use_container_width=True)

# App config
st.set_page_config(layout='wide', initial_sidebar_state='collapsed', menu_items=None)
theme = {'Biomass & Other':'#1f77b4', 
            'Coal':'#aec7e8',
            'Dual Fuel':'#ff7f0e',
            'Energy Storage':'#ffbb78',
            'Hydro':'#2ca02c',
            'Natural Gas':'#98df8a',
            'Solar':'#d62728',
            'Wind':'#7f7f7f',
            'BC':'#9467bd',
            'Saskatchewan':'#c5b0d5',
            'Montana':'#e377c2'}
hide_menu(True)
cutoff = 100

placeholder = st.empty()
for seconds in range(30):
    # Pull current day & outage data from NRG
    try:
        realtime_df, last_update = current_data()
        current_outage_df = forecast_outages().astype({'timeStamp':'datetime64[ns]','value':'int64','fuelType':'object'})
        current_outages_df = current_outages()
        outage_diff, alert_dict = outage_alerts()
    except:
        with st.spinner('Gathering Live Data Streams'):
            time.sleep(5)
        realtime_df, last_update = current_data()
        current_outage_df = forecast_outages().astype({'timeStamp':'datetime64[ns]','value':'int64','fuelType':'object'})
        current_outages_df = current_outages()
        outage_diff, alert_dict = outage_alerts()
    
    # Create a container that will be refreshed every 60 seconds
    with placeholder.container():
    # KPIs
        # Create the "current_df" dataframe that averages the 5min data from "realtime_df" into hourly data
        current_query = '''
        SELECT
            strftime('%Y-%m-%d %H:00:00', timeStamp) AS timeStamp,
            fuelType,
            strftime('%Y', timeStamp) AS year,
            strftime('%m', timeStamp) AS month,
            strftime('%d', timeStamp) AS day,
            strftime('%H', timeStamp) AS hour,
            AVG(value) AS value
        FROM realtime_df
        GROUP BY fuelType, year, month, day, hour
        ORDER BY fuelType, year, month, day, hour, timeStamp
        '''
        current_df = sqldf(current_query, locals()).astype({'fuelType':'object', 'year':'int64','month':'int64', 'day':'int64', 'hour':'int64', 'value':'float64', 'timeStamp':'datetime64[ns]'})
        # Create a "realtime" df (of len=11) which only lists the most recent supply value (in 5min intervals) each fuel type and intertie stream
        realtime = realtime_df[['fuelType','value','timeStamp']][realtime_df['timeStamp']==max(realtime_df['timeStamp'])]
        # Check if all of the live data in the realtime df has been correctly loaded, if not then load the data from 5 mins earlier
        ### IF CHECKING len(realtime<11) DOESN'T WORK, THEN USE TRY-EXCEPT METHOD
        # try:
        if len(realtime) < 11:
            realtime = realtime_df[['fuelType','value','timeStamp']][realtime_df['timeStamp']==max(realtime_df['timeStamp']-timedelta(minutes=5))]   
        # except:
        #     with st.spinner('Failed to gather live data. Waiting to reload...'):
        #         time.sleep(10)
        #     st.experimental_rerun()
        ###
        # Drop the timeStamp col and ensure datatypes are displayed correctly for the realtime df 
        realtime.drop('timeStamp', axis=1, inplace=True)
        realtime = realtime.astype({'fuelType':'object','value':'float64'})
        # Create dfs for the current and previous hourly data (of len=11) from current_df
        previousHour = current_df[['fuelType','value']][current_df['hour']==datetime.now().hour-1]
        currentHour = current_df[['fuelType','value']][current_df['hour']==datetime.now().hour-0]
        # Display KPI that compares the previous hour average to the most recent supply value
        kpi_df = kpi(previousHour, realtime, 'Real Time')
        # Display KPI that compares the previous hour's average supply to the current houly average supply
        kpi(previousHour, currentHour, 'Hourly Average')
        # Create a list of live streams that have a differential > cuttoff 
        warning_list = list(kpi_df['fuelType'][kpi_df['absDelta'].astype('int64') >= cutoff])
        # Display the last time the realtime data was loaded
        st.write(f"Last update: {last_update.strftime('%a, %b %d @ %X')}")
    # KPI warning & alert boxs
        col1, col2 = st.columns(2)
        # Display all of the items listed in warning_list
        with col1:
            if len(warning_list) > 0:
                for _ in range(len(warning_list)):
                    warning('warning', f'{warning_list[_]}')
        # Outage & intertie alerts       
        with col2:
            if len(alert_dict) > 0:
                for (k,v) in alert_dict.items():
                    warning('alert', f"{k} {v.strftime('(%b %-d, %Y)')}")

    # 14 day hist/real-time/forecast
        st.subheader('Last Week\'s Supply')
        # Pull last 7 days data
        history_df = pull_grouped_hist()
        # Combine last 7 days & live dataframes
        combo_df = pd.concat([history_df,current_df], axis=0)
        query = "SELECT * FROM combo_df ORDER BY fuelType"
        combo_df = sqldf(query, globals())
        # Base combo_df bar chart
        combo_area = alt.Chart(combo_df).mark_area(opacity=0.7).encode(
            x=alt.X('monthdatehours(timeStamp):T', title='', axis=alt.Axis(labelAngle=90)),
            y=alt.Y('value:Q', title='Current Supply (MW)'),
            color=alt.Color('fuelType:N', scale=alt.Scale(domain=list(theme.keys()),range=list(theme.values())), legend=alt.Legend(orient="top"))
        ).properties(height=400)
        st.altair_chart(combo_area, use_container_width=True)

    # Outages chart
        st.subheader('Monthly Forecasted Outages')
        # Outages area chart
        outage_area = alt.Chart(current_outage_df).mark_bar(opacity=0.7).encode(
            x=alt.X('yearmonth(timeStamp):T', title='', axis=alt.Axis(labelAngle=90)),
            y=alt.Y('value:Q', stack='zero', axis=alt.Axis(format=',f'), title='Outages (MW)'),
            color=alt.Color('fuelType:N', scale=alt.Scale(domain=list(theme.keys()),range=list(theme.values())), legend=alt.Legend(orient="top")),
            tooltip=['fuelType','value','timeStamp']
            )
        # current_outages_df
        # itertie_outage_area = alt.Chart(current_outages_df).mark_line.encode(
        #     x=
        # )
        st.altair_chart(outage_area, use_container_width=True)
        if (len(alert_dict)>0):
            alert_charts(outage_diff, theme)
        st.write(f'App will reload in {30-seconds} seconds')
        time.sleep(1)
st.experimental_rerun()