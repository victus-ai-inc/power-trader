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
import socket
import psutil
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

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
    res = conn.getresponse()

@st.experimental_memo
def get_streamInfo(streamId):
    streamInfo = pd.read_csv('stream_codes.csv')
    streamInfo = streamInfo[streamInfo['streamId']==streamId]
    return streamInfo

def pull_data(fromDate, toDate, streamId, accessToken):
    server = 'api.nrgstream.com'
    context = ssl.create_default_context(cafile=certifi.where())
    conn = http.client.HTTPSConnection(server, context=context)
    path = f'/api/StreamData/{streamId}?fromDate={fromDate}&toDate={toDate}'
    headers = {'Accept': 'Application/json', 'Authorization': f'Bearer {accessToken}'}
    conn.request('GET', path, None, headers)
    response = conn.getresponse()
    if response.status != 200:
        conn.close()
        time.sleep(2)
        pull_data(fromDate, toDate, streamId, accessToken)
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
    df['timeStamp'] = pd.to_datetime(df['timeStamp'])
    df['timeStamp'] = df['timeStamp'].dt.tz_localize('America/Edmonton', ambiguous=True, nonexistent='shift_forward')
    return df

def get_data(streamIds, start_date, end_date):
    df = pd.DataFrame([])
    for streamId in streamIds:
        accessToken = get_token()
        APIdata = pull_data(start_date.strftime('%m/%d/%Y'), end_date.strftime('%m/%d/%Y'), streamId, accessToken)
        release_token(accessToken)
        df = pd.concat([df, APIdata], axis=0)
        df.drop(['streamId','assetCode','streamName','subfuelType','timeInterval','intervalType'], axis=1, inplace=True)
    return df

@st.experimental_memo(suppress_st_warning=True, ttl=10)
def current_data():
    streamIds = [86, 322684, 322677, 87, 85, 23695, 322665, 23694, 120, 124947, 122, 1]
    if datetime.now(tz).hour==0:
        realtime_df = get_data(streamIds, datetime.now(tz).date()-timedelta(days=1), datetime.now(tz).date()+timedelta(days=1))
    else:
        realtime_df = get_data(streamIds, datetime.now(tz).date(), datetime.now(tz).date()+timedelta(days=1))
    last_update = datetime.now(tz)
    return realtime_df, last_update

def kpi(left_df, right_df, title):
    # Merging KPIs into one dataframe
    kpi_df = left_df.merge(right_df, how='left', on='fuelType', suffixes=('Previous','Current'))
    # Creating KPI delta calculation
    kpi_df['delta'] = kpi_df['valueCurrent'] - kpi_df['valuePrevious']
    kpi_df['absDelta'] = abs(kpi_df['delta'])
    # Formatting numbers 
    kpi_df.iloc[:,1:] = kpi_df.iloc[:,1:].applymap('{:.0f}'.format)
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12 = st.columns(12)
    with col1:
        st.subheader(title)
    col2.metric(label=kpi_df.iloc[7,0], value=kpi_df.iloc[7,2], delta=kpi_df.iloc[7,3]) # Natural Gas
    col3.metric(label=kpi_df.iloc[5,0], value=kpi_df.iloc[5,2], delta=kpi_df.iloc[5,3]) # Hydro
    col4.metric(label=kpi_df.iloc[4,0], value=kpi_df.iloc[4,2], delta=kpi_df.iloc[4,3]) # Energy Storage
    col5.metric(label=kpi_df.iloc[10,0], value=kpi_df.iloc[10,2], delta=kpi_df.iloc[10,3]) # Solar
    col6.metric(label=kpi_df.iloc[11,0], value=kpi_df.iloc[11,2], delta=kpi_df.iloc[11,3]) # Wind
    col7.metric(label=kpi_df.iloc[3,0], value=kpi_df.iloc[3,2], delta=kpi_df.iloc[3,3]) # Dual Fuel
    col8.metric(label=kpi_df.iloc[2,0], value=kpi_df.iloc[2,2], delta=kpi_df.iloc[2,3]) # Coal
    col9.metric(label=kpi_df.iloc[0,0], value=kpi_df.iloc[0,2], delta=kpi_df.iloc[0,3]) # BC
    col10.metric(label=kpi_df.iloc[6,0], value=kpi_df.iloc[6,2], delta=kpi_df.iloc[6,3]) # Montanta
    col11.metric(label=kpi_df.iloc[9,0], value=kpi_df.iloc[9,2], delta=kpi_df.iloc[9,3]) # Sask
    col12.metric(label=kpi_df.iloc[8,0], value=kpi_df.iloc[8,2], delta=kpi_df.iloc[8,3]) # Pool Price
    return kpi_df

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

@st.experimental_memo(suppress_st_warning=True, max_entries=1)
def pull_grouped_hist():
    # Google BigQuery auth
    credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
    # Check when data was last added to BigQuery
    query = 'SELECT MAX(timeStamp) FROM nrgdata.hourly_data'
    # Check when BigQuery was last updated
    last_update = bigquery.Client(credentials=credentials).query(query).to_dataframe().iloc[0][0]
    last_update = last_update.tz_convert('America/Edmonton')
    # Add data to BQ from when it was last updated to yesterday
    if last_update.date() < (datetime.now(tz).date()-timedelta(days=1)):
        pull_grouped_hist.clear()
        streamIds = [86, 322684, 322677, 87, 85, 23695, 322665, 23694, 120, 124947, 122, 1]
        history_df = get_data(streamIds, last_update.date(), datetime.now(tz).date())
        bigquery.Client(credentials=credentials).load_table_from_dataframe(history_df, 'nrgdata.hourly_data')
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
    WHERE timeStamp BETWEEN DATE_SUB(TIMESTAMP(current_date(),'America/Edmonton'), INTERVAL 7 DAY) AND TIMESTAMP(current_date(),'America/Edmonton')
    GROUP BY fuelType, year, month, day, hour, timeStamp
    ORDER BY fuelType, year, month, day, hour, timeStamp
    '''
    history_df = bigquery.Client(credentials=credentials).query(query).to_dataframe()
    history_df['timeStamp'] = history_df['timeStamp'].dt.tz_localize('utc',ambiguous=True, nonexistent='shift_forward')
    history_df['timeStamp'] = history_df['timeStamp'].dt.tz_convert('America/Edmonton')   
    return history_df

@st.experimental_memo(suppress_st_warning=True, ttl=180, max_entries=1)
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
    return daily_outages

@st.experimental_memo(suppress_st_warning=True, ttl=300, max_entries=1)
def monthly_outages():
    streamIds = [44648, 118361, 322689, 118362, 147262, 322675, 322682, 44651]
    years = [datetime.now(tz).year, datetime.now(tz).year+1, datetime.now(tz).year+2]
    monthly_outages = pd.DataFrame([])
    for year in years:
        df = get_data(streamIds, date(year,1,1), date(year+1,1,1))
        monthly_outages = pd.concat([monthly_outages, df], axis=0)
    monthly_outages = monthly_outages[monthly_outages['timeStamp'].dt.date>(datetime.now(tz).date())]
    return monthly_outages

def text_alert(alert_df, pickle_key):
    email = st.secrets['email_address']
    pas = st.secrets['email_password']
    sms_gateways = st.secrets['phone_numbers'].values()
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(email, pas)
      
    for i in range(len(alert_df)):
        if pickle_key == 'daily_df':
            timeStamp = alert_df.iloc[i,0].strftime('%b %d, %Y')
            timeUnit = 'Daily'
        elif pickle_key == 'monthly_df':
            timeStamp = alert_df.iloc[i,0].strftime('%b %Y')
            timeUnit = 'Monthly'
        fuelType = alert_df.iloc[i,1]
        value = round(alert_df.iloc[i,2])
        for gateway in sms_gateways:
            msg = MIMEMultipart()
            msg['To'] = gateway
            if value >= 100:
                body = f'\n{timeUnit} {fuelType} outage changed by +{value}MW for {timeStamp}'
            elif value <= 100:
                body = f'\n{timeUnit} {fuelType} outage has decreased by {value}MW for {timeStamp}'
            msg.attach(MIMEText(body, 'plain'))
            server.sendmail(email, gateway, msg.as_string())

def diff_calc(pickle_key, old_df, new_df):
    diff_df = pd.merge(old_df, new_df, on=['timeStamp','fuelType'], suffixes=('_new','_old'))
    diff_df['diff_value'] = diff_df['value_old'] - diff_df['value_new']
    diff_df['date'] = diff_df['timeStamp'].dt.date
    if pickle_key == 'daily_df':
        diff_df = diff_df[diff_df['date'] < datetime.now(tz).date() + timedelta(days=90)]
    elif pickle_key == 'monthly_df':
        diff_df = diff_df[diff_df['date'] > datetime.now(tz).date() + relativedelta(months=3, day=1, days=-1)]
    return diff_df

def gather_outages(pickle_key, outage_func):
    # Pull outages
    with open(f'./{pickle_key}.pickle', 'rb') as outage:
        outage_dfs = pickle.load(outage)
    try:
        outage_df = outage_func
    except:
        outage_df = outage_dfs[4][1]
    # Send alerts if current outages have changed since last time they were loaded
    new_outage_df = outage_df[~outage_df['fuelType'].isin(['3-Day Solar Forecast','7-Day Wind Forecast'])]
    old_outage_df = outage_dfs[4][1]
    old_outage_df = old_outage_df[~old_outage_df['fuelType'].isin(['3-Day Solar Forecast','7-Day Wind Forecast'])]
    alert_df = diff_calc(pickle_key, old_outage_df, new_outage_df)
    alert_df = alert_df[['date','fuelType','diff_value']][abs(alert_df['diff_value'])>=cutoff]
    alert_df = alert_df.groupby(['date','fuelType','diff_value']).max().reset_index()
    #alert_df = pd.DataFrame({'date':[datetime(2022,9,21),datetime(2022,9,21)],'fuelType':['Test','Test2'],'diff_value':[1000,-1000]})
    if len(alert_df) > 0:
        #text_alert(alert_df, pickle_key)
        pass
    # Remove oldest and add newest outage_df from default_pickle file each day
    if datetime.now(tz).date() > (outage_dfs[0][0].date() + timedelta(days=6)):
        if datetime.now(tz).date() != outage_dfs[4][0].date():
            outage_dfs.pop(0)
            outage_dfs.insert(len(outage_dfs), (datetime.now(tz), new_outage_df))
    # Update to most current outage_df in default_pickle file
    outage_dfs[4] = (datetime.now(tz), new_outage_df)
    with open(f'./{pickle_key}.pickle', 'wb') as outage:
        pickle.dump(outage_dfs, outage, protocol=pickle.HIGHEST_PROTOCOL)
    return outage_df

def outage_diffs(pickle_key):
    # Create df and alert list comparing outages a week ago to current outages
    try:
        with open(f'./{pickle_key}.pickle', 'rb') as outage:
            outage_dfs = pickle.load(outage)
    except:
        st.experimental_rerun()
    diff_df = diff_calc(pickle_key, outage_dfs[0][1], outage_dfs[4][1])
    alert_list = list(set(diff_df['fuelType'][abs(diff_df['diff_value'])>=cutoff]))
    diff_df = diff_df[diff_df['fuelType'].isin(alert_list)]
    return diff_df, alert_list

def current_supply_chart():
    st.subheader('Current Supply (Previous 7-days)')
    thm = {k:v for k,v in theme.items() if k not in ['Intertie','3-Day Solar Forecast','7-Day Wind Forecast']}
    history_df = pull_grouped_hist()
    combo_df = pd.concat([history_df,current_df], axis=0)
    combo_df = sqldf("SELECT * FROM combo_df ORDER BY fuelType", locals())
    combo_max = sqldf(
        '''SELECT MAX(value) AS value FROM (
            SELECT timeStamp, SUM(value) AS value 
            FROM combo_df
            WHERE value > 0
            GROUP BY timeStamp)''',locals())
    combo_max = combo_max.iloc[0]['value']
    combo_max = combo_max if combo_max % 1000 == 0 else combo_max + 1000 - combo_max % 1000
    combo_min = sqldf(
        '''SELECT MIN(value) AS value FROM (
            SELECT timeStamp, SUM(value) AS value 
            FROM combo_df
            WHERE value < 0
            GROUP BY timeStamp)''',locals())
    combo_min = combo_min.iloc[0]['value']
    combo_min = combo_min if combo_min % 1000 == 0 else combo_min - combo_min % 1000
    combo_base = alt.Chart(combo_df).encode(
        x=alt.X('timeStamp:T', title='', axis=alt.Axis(labelAngle=270, gridWidth=0)),
        y=alt.Y('value:Q', stack='zero', title='Current Supply (MW)', scale=alt.Scale(domain=[combo_min,combo_max])),
        color=alt.Color('fuelType:N', scale=alt.Scale(domain=list(thm.keys()), range=list(thm.values())), legend=alt.Legend(orient='top')),
        tooltip=['fuelType','value','yearmonthdatehours(timeStamp)']
    ).properties(height=400)
    combo_area = combo_base.mark_area(opacity=0.7).transform_filter(alt.datum.fuelType!='Pool Price')
    combo_line = combo_base.mark_line(interpolate='step-after').encode(
        y=alt.Y('value:Q', title='Price ($)', scale=alt.Scale(domain=[combo_min/10,combo_max/10])),
        color=alt.Color('fuelType:N')).transform_filter(alt.datum.fuelType=='Pool Price')
    return st.altair_chart(alt.layer(combo_area,combo_line).resolve_scale(y='independent'), use_container_width=True)

def next7_outage_chart():
    st.subheader('Daily Outages (Next 7-days)')
    daily_outage_seven = daily_outage[daily_outage['timeStamp'].dt.date <= datetime.now(tz).date() + timedelta(days=6)]
    daily_outage_seven_base = alt.Chart(daily_outage_seven).encode(
        x=alt.X('timeStamp:T', title='', axis=alt.Axis(labelAngle=270)),
        tooltip=['fuelType','value','timeStamp']
        ).properties(height=400)
    thm = {k:v for k,v in theme.items() if k not in ['BC','Saskatchewan','Montana','Pool Price']}
    daily_outage_seven_area = daily_outage_seven_base.mark_area(opacity=0.7).encode(
        y=alt.Y('value:Q', stack='zero', axis=alt.Axis(format=',f'), title='Outages (MW)'),
        color=alt.Color('fuelType:N', scale=alt.Scale(domain=list(thm.keys()), range=list(thm.values())), legend=alt.Legend(orient='top')),
    ).transform_filter({'not':alt.FieldOneOfPredicate(field='fuelType', oneOf=['7-Day Wind Forecast','3-Day Solar Forecast'])})
    daily_outage_seven_line = daily_outage_seven_base.mark_line(opacity=0.7, strokeWidth=3).encode(
        y=alt.Y('value:Q', title='Wind & Solar Forecast (MW)'),
        color='fuelType:N',
    ).transform_filter(alt.FieldOneOfPredicate(field='fuelType', oneOf=['7-Day Wind Forecast','3-Day Solar Forecast']))
    st.altair_chart(alt.layer(daily_outage_seven_area,daily_outage_seven_line).resolve_scale(y='independent'), use_container_width=True)

def next90_outage_chart():
    st.subheader('Daily Outages (Next 90-days)')
    thm = {k:v for k,v in theme.items() if k not in ['BC','Saskatchewan','Montana','Pool Price','7-Day Wind Forecast','3-Day Solar Forecast']}
    daily_outage_area = alt.Chart(daily_outage).mark_area(opacity=0.8).encode(
        x=alt.X('timeStamp:T', title='', axis=alt.Axis(labelAngle=270)),
        y=alt.Y('value:Q', stack='zero', axis=alt.Axis(format=',f'), title='Outages (MW)'),
        color=alt.Color('fuelType:N', scale=alt.Scale(domain=list(thm.keys()), range=list(thm.values())), legend=alt.Legend(orient='top')),
        tooltip=['fuelType','value','timeStamp']
    ).properties(height=400).configure_view(strokeWidth=0).configure_axis(grid=False)
    st.altair_chart(daily_outage_area, use_container_width=True)

def monthly_outages_chart():
    st.subheader('Monthly Outages (Next 2-years)')
    thm = {k:v for k,v in theme.items() if k not in ['BC','Saskatchewan','Montana','Pool Price','Intertie','7-Day Wind Forecast','3-Day Solar Forecast']}
    monthly_outage_area = alt.Chart(monthly_outage).mark_bar(opacity=0.7).encode(
        x=alt.X('yearmonth(timeStamp):O', title='', axis=alt.Axis(labelAngle=270)),
        y=alt.Y('value:Q', stack='zero', axis=alt.Axis(format=',f'), title='Outages (MW)'),
        color=alt.Color('fuelType:N', scale=alt.Scale(domain=list(thm.keys()),range=list(thm.values())), legend=alt.Legend(orient='top')),
        tooltip=['fuelType','value','timeStamp']
        ).properties(height=400).configure_view(strokeWidth=0).configure_axis(grid=False)
    st.altair_chart(monthly_outage_area, use_container_width=True)

def daily_outage_diff_chart():
    if len(daily_alert_list)>0:
        st.subheader('Daily Intertie & Outage Differentials (Past 7 days)')
        daily_outage_heatmap = alt.Chart(daily_diff[['timeStamp','fuelType','diff_value']]).mark_rect(opacity=0.9, stroke='grey', strokeWidth=0).encode(
            x=alt.X('monthdate(timeStamp):O', title=None, axis=alt.Axis(ticks=False, labelAngle=270)),
            y=alt.Y('fuelType:N', title=None, axis=alt.Axis(labelFontSize=15)),
            color=alt.condition(alt.datum.diff_value==0,
                                alt.value('white'),
                                alt.Color('diff_value:Q', scale=alt.Scale(domainMin=-max(abs(daily_diff['diff_value'])),
                                                                        domainMax=max(abs(daily_diff['diff_value'])),
                                                                        scheme='redyellowgreen'))),
            tooltip=['fuelType','diff_value','timeStamp']
        ).properties(height=110 if len(daily_alert_list)==1 else 60 * len(daily_alert_list)).configure_view(strokeWidth=0).configure_axis(grid=False)
        st.altair_chart(daily_outage_heatmap, use_container_width=True)

def monthly_outage_diff_chart():
    if len(monthly_alert_list)>0:
        st.subheader('Monthly Intertie & Outage Differentials (Past 7 days)')
        monthly_outage_heatmap = alt.Chart(monthly_diff[['timeStamp','fuelType','diff_value']]).mark_rect(opacity=0.9, strokeWidth=0).encode(
            x=alt.X('yearmonth(timeStamp):O', title=None, axis=alt.Axis(ticks=False, labelAngle=270)),
            y=alt.Y('fuelType:N', title=None, axis=alt.Axis(labelFontSize=15)),
            color=alt.condition(alt.datum.diff_value==0,
                                alt.value('white'),
                                alt.Color('diff_value:Q', scale=alt.Scale(domainMin=-max(abs(monthly_diff['diff_value'])), 
                                                                        domainMax=-max(abs(monthly_diff['diff_value'])),
                                                                        scheme='redyellowgreen'))),
            tooltip=['fuelType','diff_value','timeStamp']
        ).properties(height=110 if len(monthly_alert_list)==1 else 60 * len(monthly_alert_list)).configure_view(strokeWidth=0).configure_axis(grid=False)
        st.altair_chart(monthly_outage_heatmap, use_container_width=True)

#@st.experimental_memo()
def user_log(user, logon, lastlog):
    pass

def getSystemInfoDict():
    info = dict()
    info['cached'] = str(
            round(psutil.virtual_memory().cached / (1024.0 ** 2)))+" MB"
    info['available-memory'] = f'{round(psutil.virtual_memory().available * 100 / psutil.virtual_memory().total)}%'
    info = json.dumps(info)
    info = json.loads(info)
    return info

@st.experimental_singleton()
def fblogin():
    cred = credentials.Certificate(st.secrets["gcp_service_account"])
    app = firebase_admin.initialize_app(cred)
    db = firestore.client()
    return db


#def getSystemInfoJson():
    #eturn json.loads(getSystemInfoString())



# App config
st.set_page_config(layout='wide', initial_sidebar_state='collapsed', menu_items=None)

# Hide Streamlit menus
hide_menu_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            .css-1j15ncu {visibility: hidden;}
            .css-14x9thb {visibility: hidden;}
            </style>
            """
st.markdown(hide_menu_style, unsafe_allow_html=True)

# Default app theme colors
theme = {'Biomass & Other':'#1f77b4', 'Coal':'#aec7e8', 'Dual Fuel':'#ff7f0e', 'Energy Storage':'#ffbb78', 'Hydro':'#2ca02c', 'Natural Gas':'#98df8a',
            'Solar':'#d62728', 'Wind':'#7f7f7f', 'BC':'#9467bd', 'Saskatchewan':'#c5b0d5', 'Montana':'#e377c2', 'Intertie':'#17becf', 'Pool Price':'#000000',
            '3-Day Solar Forecast':'#d62728','7-Day Wind Forecast':'#3f3f3f'}

# Initialize variables
cutoff = 100
tz = pytz.timezone('America/Edmonton')
db = fblogin()
# Set path to Firestore DB
currentData_ref = db.collection(u'appData').document(u'currentData')

placeholder = st.empty()
for seconds in range(450):
    #lastlog = datetime.now(tz)

    with st.spinner('Gathering Realtime Data...'):
        # Read current_df from Firestore
        realtime_df = pd.DataFrame.from_dict(currentData_ref.get().to_dict())
        realtime_df['timeStamp'] = realtime_df['timeStamp'].dt.tz_convert('America/Edmonton') 
        last_update = datetime.now()
    if seconds%90==0:
        with st.spinner('Gathering Daily Outages...'):
            daily_outage = gather_outages('daily_df', daily_outages())
            daily_outage = daily_outage[daily_outage['timeStamp'].dt.date < datetime.now(tz).date() + timedelta(days=90)]
    if seconds%150==0:
        with st.spinner('Gathering Monthly Outages...'):
            monthly_outage = gather_outages('monthly_df', monthly_outages())

    daily_diff, daily_alert_list = outage_diffs('daily_df')
    monthly_diff, monthly_alert_list = outage_diffs('monthly_df')
    alert_list = set(daily_alert_list + monthly_alert_list)

    try:
        with open('./alert_dates.pickle', 'rb') as alert:
            alerts_dict = pickle.load(alert)
    except:
        time.sleep(2)
        with open('./alert_dates.pickle', 'rb') as alert:
            alerts_dict = pickle.load(alert)
    for fuel_type in alert_list:
        if (datetime.now(tz).date() - timedelta(days=7)) > alerts_dict[fuel_type]:
            alerts_dict[fuel_type] = datetime.now(tz).date()
    with open('./alert_dates.pickle', 'wb') as handle:
        pickle.dump(alerts_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)
    #alert_dict = {k:v for k,v in alerts_dict.items() if v > (datetime.now(tz).date()-timedelta(days=7))}
    alert_dict = {alert_list:alerts_dict[alert_list] for alert_list in alert_list}
    alert_dict = dict(sorted(alert_dict.items(), key=lambda item: item[1]))

    with placeholder.container():
        #st.write(getSystemInfoDict())
        #st.write(lastlog)
    # KPIs
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
        current_df['timeStamp'] = current_df['timeStamp'].dt.tz_localize(tz='America/Edmonton')
        realtime = realtime_df[['fuelType','value','timeStamp']][realtime_df['timeStamp']==max(realtime_df['timeStamp'])]
        new_realtime = realtime_df[['fuelType','value','timeStamp']][realtime_df['timeStamp']==max(realtime_df['timeStamp'])]
        old_realtime = realtime_df[['fuelType','value','timeStamp']][realtime_df['timeStamp']==max(realtime_df['timeStamp']-timedelta(minutes=5))]
        realtime = pd.merge(old_realtime, new_realtime, on='fuelType', how='outer')
        realtime['value'] = np.where(realtime['timeStamp_y']>realtime['timeStamp_x'], realtime['value_y'], realtime['value_x'])
        realtime.drop(['value_x','timeStamp_x','value_y','timeStamp_y'], axis=1, inplace=True)
        realtime = realtime.astype({'fuelType':'object','value':'float64'})
        #previousHour = current_df[['fuelType','value']][current_df['hour']==datetime.now(tz).hour-1]
        #currentHour = current_df[['fuelType','value']][current_df['hour']==datetime.now(tz).hour]
        previousHour = current_df[['fuelType','value']][current_df['hour']==22]
        currentHour = current_df[['fuelType','value']][current_df['hour']==23]
        with st.expander('*Click here to expand/collapse KPIs',expanded=True):
            kpi_df = kpi(previousHour, realtime, 'Real Time')
            kpi(previousHour, currentHour, 'Hourly Average')
            st.write(f"Last update: {last_update.strftime('%a, %b %d @ %X')}")
        # KPI warning & alert boxes
            warning_list = list(kpi_df['fuelType'][kpi_df['absDelta'].astype('int64') >= cutoff])
            col1, col2 = st.columns(2)
            with col1:
                if len(warning_list) > 0:
                    for _ in range(len(warning_list)):
                        warning('warning', f'{warning_list[_]}')
            with col2:
                if len(alert_dict) > 0:
                    for (k,v) in alert_dict.items():
                        warning('alert', f"{k} {v.strftime('(%b %-d, %Y)')}")
        # Charts
        col1, col2 = st.columns(2)
        with col1:
            current_supply_chart()
            next90_outage_chart()
            daily_outage_diff_chart()
        with col2:
            next7_outage_chart()
            monthly_outages_chart()
            monthly_outage_diff_chart()
    time.sleep(1)
st.experimental_rerun()