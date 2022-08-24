#Streamlit app to monitor historical/future power supply/demand in Alberta
import streamlit as st
import pandas as pd
import altair as alt
import ssl
import json
import http.client
import certifi
import time
import pull_nrg_data
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
            </style>
            """
        return st.markdown(hide_menu_style, unsafe_allow_html=True)

# Creating access token that will be shared by all app users
@st.experimental_singleton
def getToken():
    if 'accessToken' not in st.session_state or st.session_state['tokenExpiry'] <= datetime.now():
        accessToken, tokenExpiry = pull_nrg_data.getToken()
        st.session_state['accessToken'] = accessToken
        st.session_state['tokenExpiry'] = tokenExpiry
    else:
        accessToken = st.session_state['accessToken']
        tokenExpiry = st.session_state['tokenExpiry']
    return accessToken, tokenExpiry

def token():
    if tokenExpiry <= datetime.now():
        pull_nrg_data.release_token(accessToken)
        getToken.clear()
    return getToken()

@st.experimental_memo
def get_streamInfo(streamId):
    streamInfo = pd.read_csv('stream_codes.csv')
    streamInfo = streamInfo[streamInfo['streamId']==streamId]
    return streamInfo

#@st.experimental_singleton
def http_connect():
    server = 'api.nrgstream.com'
    context = ssl.create_default_context(cafile=certifi.where())
    conn = http.client.HTTPSConnection(server, context=context)
    return conn

def pull_data(fromDate, toDate, streamId, accessToken, tokenExpiry):
    # Setup the path for data request
    conn = http_connect()
    conn
    path = f'/api/StreamData/{streamId}?fromDate={fromDate}&toDate={toDate}'
    headers = {'Accept': 'Application/json', 'Authorization': f'Bearer {accessToken}'}
    conn.request('GET', path, None, headers)
    res = conn.getresponse()
    if res.code != 200:
        st.write(f'trying, code = {res.code}')
        res.read()
        conn.close()
        time.sleep(5)
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
@st.experimental_memo(suppress_st_warning=True)
def current_data(tokenExpiry):
    streamIds = [86, 322684, 322677, 87, 85, 23695, 322665, 23694]
    current_df = pd.DataFrame([])
    today = datetime.now()
    accessToken, tokenExpiry = token()
    for id in streamIds:
        APIdata = pull_data(today.strftime('%m/%d/%Y'), today.strftime('%m/%d/%Y'), id, accessToken, tokenExpiry)
        APIdata['timeStamp'] = pd.to_datetime(APIdata['timeStamp'])
        current_df = pd.concat([current_df, APIdata], axis=0)
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
    warning_list = list(kpi_df['fuelType'][kpi_df['absDelta'] > cutoff])
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

# Create outages dataframe from NRG data
@st.experimental_memo
def stream_data(streamIds, streamNames, years):
    stream_df = pd.DataFrame([])
    for id in streamIds:
        server = 'api.nrgstream.com'
        year_df = pd.DataFrame([])
        for yr in years:
            accessToken, tokenExpiry = token()
            # Define start & end dates
            startDate = date(yr,1,1).strftime('%m/%d/%Y')
            endDate = date(yr,12,31).strftime('%m/%d/%Y')
            # NRG API connection
            path = f'/api/StreamData/{id}?fromDate={startDate}&toDate={endDate}'
            headers = {'Accept': 'Application/json', 'Authorization': f'Bearer {accessToken}'}
            context = ssl.create_default_context(cafile=certifi.where())
            conn = http.client.HTTPSConnection(server, context=context)
            conn.request('GET', path, None, headers)
            res = conn.getresponse()
            # Load json data from API & create pandas df
            jsonData = json.loads(res.read().decode('utf-8'))
            df = pd.json_normalize(jsonData, record_path='data')
            # Close NRG API connection
            conn.close()
            # Concat years for each stream
            year_df = pd.concat([year_df,df], axis=0)
            # Release NRG API access token
            pull_nrg_data.release_token(accessToken)
        # Rename year_df cols
        year_df.rename(columns={0:'timeStamp', 1:f'{streamNames[id]}'}, inplace=True)
        # Change timeStamp to datetime
        year_df['timeStamp'] = pd.to_datetime(year_df['timeStamp'])
        year_df['timeStamp'] = year_df['timeStamp'].dt.tz_localize(tz='America/Edmonton')
        # Re-index the year_df
        year_df.set_index('timeStamp', inplace=True)
        # Join year_df to outages dataframe
        stream_df = pd.concat([stream_df,year_df], axis=1, join='outer')
    return stream_df

def outages():
    streamIds = [44648, 118361, 322689, 118362, 147262, 322675, 322682, 44651]
    streamNames = {44648:'Coal', 118361:'Natural Gas', 322689:'Dual Fuel', 118362:'Hydro', 147262:'Wind', 322675:'Solar', 322682:'Energy Storage', 44651:'Biomass & Other'}
    years = [datetime.now().year, datetime.now().year+1, datetime.now().year+2]
    outage_df = stream_data(streamIds, streamNames, years)
    return outage_df

# Grid options for AgGrid Demand forecast table
grid_options = {
    "defaultColDef":
    {
        "autoHeight": True,
        "editable": False,
        "sortable": False,
        "filterable": False,
        "suppressMovable": True,
        "resizable":False,
    },
    "RowHoverHighlight":True,
    "columnDefs": [
        {
            "headerName": "Date",
            "field": "Date",
            "type":"dateColumn",
        },
        {
            "headerName": "Demand (MW)",
            "field": "Close",
            "valueFormatter": "Math.floor(data.Close).toString().replace(/(\d)(?=(\d{3})+(?!\d))/g, '$1,')",
        },
        {
            "headerName": "Offset (MW)",
            "field": "Offset",
            "editable": True,
            "singleClickEdit":True,
            "valueFormatter": "Math.floor(data.Offset).toString().replace(/(\d)(?=(\d{3})+(?!\d))/g, '$1,')",
        },
    ],
}

import random
@st.experimental_memo
def testing():
    df1 = outages().astype('int32')
    old_outage_df = outages().astype('int32').reset_index()
    old_outage_df['Hydro'] = [x - random.randint(0,50) if x >=50 else x + random.randint(0,50)for x in old_outage_df['Hydro']]
    old_outage_df['Solar'] = [x - random.randint(0,50) if x >=100 else x + random.randint(0,50) for x in old_outage_df['Solar']]
    old_outage_df['Natural Gas'] = [x - random.randint(0,5) if x >=500 else x + random.randint(0,50) for x in old_outage_df['Natural Gas']]
    old_outage_df['Coal'] = [x - random.randint(0,50) if x >=50 else x + random.randint(0,50) for x in old_outage_df['Coal']]
    old_outage_df.set_index(['timeStamp'], inplace=True)
    df = old_outage_df - df1
    #drop cols that do not have a change >=50
    df = df.loc[:, (abs(df) >= 50).any(axis=0)]
    df.reset_index(inplace=True)
    df.drop(0,inplace=True)
    df = df.melt(id_vars=['timeStamp'])
    df['timeStamp'] = df['timeStamp'].dt.strftime('%Y-%m')
    return df

# # Offset demand table & chart
#     st.subheader('Forecasted & Adjusted Demand')
#     # Creating cols for Streamlit app
#     col1, col2 = st.columns([1,2])
#     # Adding AgGrid table to Col1
#     with col1:
#         offset_df = AgGrid(offset_df[['Date','Open','Close','Offset']], grid_options, fit_columns_on_grid_load=True)['data']
#     # Calculating cummulative offset
#         offset_df = pd.DataFrame(offset_df)
#         offset_df['tot_offset'] = offset_df['Offset'].cumsum()
#         offset_df['Adjusted Demand'] = offset_df['Close'] + offset_df['tot_offset']
#         offset_df = offset_df[1:]
#     # Creating Altair charts
#         # Demand candlestick chart
#         open_close_color = alt.condition("datum.Open <= datum.Close",
#                                  alt.value("#06982d"),
#                                  alt.value("#ae1325"))
#         candlestick = alt.Chart(offset_df).mark_bar().encode(
#             x=alt.X('Date:T',axis=alt.Axis(format='%b %Y'),title=''),
#             y=alt.Y('Open:Q', title='Demand (MW)'),
#             y2=alt.Y2('Close:Q', title=''),
#             color=open_close_color
#         ).interactive( ).properties(height=430)
#         # Adjusted Demand area chart
#         area = alt.Chart(offset_df).mark_area(color='grey', opacity=0.3).encode(
#             x='Date:T',
#             y='Adjusted Demand:Q',
#         ).interactive(bind_y=True)
#         # Selector for sliding vertical line
#         nearest = alt.selection(type='single', nearest=True, on='mouseover', fields=['Date'], empty='none', clear='mouseout')
#         # Creating vertical line to highlight current Date
#         rule = alt.Chart(offset_df).mark_rule(color='gray').encode(
#             x='Date:T',
#             opacity=alt.condition(nearest, alt.value(1), alt.value(0)),
#             tooltip = ['Date:T', 
#                         alt.Tooltip('Open:Q', title='Open (MW)', format=',d'),
#                         alt.Tooltip('Close:Q', title='Close (MW)', format=',d'),
#                         alt.Tooltip('Adjusted Demand:Q', title='Adjusted Close (MW)', format=',d')]
#         ).add_selection(nearest)
#     # Adding layered chart to Col2
#     with col2:
#         st.altair_chart(candlestick + area + rule, use_container_width=True)

# Main code block
if __name__ == '__main__':
# App config
    st.set_page_config(layout='wide', initial_sidebar_state='collapsed', menu_items=None)
    with st.sidebar:
        cutoff = st.select_slider('Warning MW cutoff', [_ * 10 for _ in range(11)], value=100)
    st.title('Alberta Power Supply/Demand')
    hide_menu(True)

# Get & save access token to cache
    accessToken, tokenExpiry = getToken()
    accessToken
    tokenExpiry 
    st.session_state
# Pull 24M Supply/Demand data
    #st.subheader('24M Supply/Demand data (Daily)')
    # streamIds = [278763]
    # streamNames = {278763:'0'}
    # years = [datetime.now().year, datetime.now().year+1, datetime.now().year+2]
    # offset_df = stream_data(streamIds, streamNames, years)
    # offset_df.rename(columns={'0':'Peak Hour', 2:'Expected Supply', 3:'Import Capacity', 4:'Load+Reserve', 5:'Surplus'}, inplace=True)

# Cache outages dataframe in session_state if it doesn't already exist
    # if 'outages' not in st.session_state:
    #     st.session_state['outages'] = outages()
    old_outage_df = outages().astype('int32').reset_index()
    old_outage_df['Hydro'] = [x - random.randint(0,50) if x >=50 else x + random.randint(0,50)for x in old_outage_df['Hydro']]
    old_outage_df['Solar'] = [x - random.randint(0,50) if x >=100 else x + random.randint(0,50) for x in old_outage_df['Solar']]
    old_outage_df['Natural Gas'] = [x - random.randint(0,5) if x >=500 else x + random.randint(0,50) for x in old_outage_df['Natural Gas']]
    old_outage_df['Coal'] = [x - random.randint(0,50) if x >=50 else x + random.randint(0,50) for x in old_outage_df['Coal']]
    old_outage_df.set_index(['timeStamp'], inplace=True)

    placeholder = st.empty()
    for seconds in range(100000):
        # Pull live data
        #st.session_state['test_df']
        current_df, last_update = current_data(tokenExpiry)
        #last_update = last_update
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
                    st.error(f'{warning_list[_]} has a differential greater than {cutoff} MW over the previous hour.')
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
        # Outages chart
            st.subheader('Forecasted Outages (Daily)')
            #Create outages_df
            outage_df = outages().astype('int32')
            #outage_data = outage_df.reset_index(inplace=True)
            outage_data = outages().reset_index()
            outage_data = pd.melt(outage_data, 
                            id_vars=['timeStamp'],
                            value_vars=['Coal', 'Natural Gas', 'Dual Fuel', 'Hydro', 'Wind', 'Solar', 'Energy Storage', 'Biomass & Other'],
                            var_name='Source',
                            value_name='Value')          
            # Outages area chart
            outage_area = alt.Chart(outage_data).mark_area(opacity=0.7).encode(
                x=alt.X('timeStamp:T', title=''),
                y=alt.Y('Value:Q', stack='zero', axis=alt.Axis(format=',f'), title='Outages (MW)'),
                color=alt.Color('Source:N', scale=alt.Scale(scheme='category20'), legend=alt.Legend(orient="top")),
                )
            st.altair_chart(outage_area, use_container_width=True)

            #TESTING!!
            outage_df = outages().astype('int32')
            # Check and send alert if outages have changed by > 50 MW
            if (abs(outage_df-old_outage_df) >= cutoff).any().any():
                st.subheader('Outage Alerts')
                outage_df = old_outage_df
                #alerts.sms()
                df = testing()
                test = alt.Chart(df).mark_bar(cornerRadiusTopLeft=5, 
                                                cornerRadiusTopRight=5,
                                                cornerRadiusBottomLeft=5,
                                                cornerRadiusBottomRight=5,
                                                opacity=0.6
                                                ).encode(
                    x=alt.X('yearmonth(timeStamp):T'),
                    y=alt.Y('value:Q', impute={'value':0},title=''),
                    column='variable:N',
                    color=alt.condition(alt.datum.value < 0, alt.value('red'), alt.value('black')),
                ).properties(height = 100)
                st.altair_chart(test)
            current_data.clear()
            time.sleep(10)