#Streamlit app to monitor historical/future power supply/demand in Alberta
import streamlit as st
import pandas as pd
import altair as alt
import pull_nrg_data
import ssl
import json
import http.client
import certifi
from st_aggrid import AgGrid
from datetime import datetime, date, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
from pandasql import sqldf
import time

def current_data():
    streamIds = [86, 322684, 322677, 87, 85, 23695, 322665, 23694]
    current_df = pd.DataFrame([])
    today = datetime.now()
    for id in streamIds:
        accessToken, tokenExpiry = pull_nrg_data.getToken()
        try:
            APIdata = pull_nrg_data.pull_data(today.strftime('%m/%d/%Y'), today.strftime('%m/%d/%Y'), id, accessToken, tokenExpiry)
            pull_nrg_data.release_token(accessToken)
            APIdata['timeStamp'] = pd.to_datetime(APIdata['timeStamp'])
            current_df = pd.concat([current_df, APIdata], axis=0)
        except:
            pull_nrg_data.release_token(accessToken)
            pass
    query = '''
        SELECT
            fuelType,
            strftime('%Y', timeStamp) AS year,
            strftime('%m', timeStamp) AS month,
            strftime('%d', timeStamp) AS day,
            strftime('%H', timeStamp) AS hour,
            avg(value)
        FROM current_df
        GROUP BY fuelType, year, month, day, hour
        '''
    current_df = sqldf(query, locals())
    return current_df

# Function to hide top and bottom menus on Streamlit app
def hide_menu(bool):
    if bool == True:
        hide_menu_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            </style>
            """
        return st.markdown(hide_menu_style, unsafe_allow_html=True)

# Create outages dataframe from NRG data
@st.experimental_memo
def stream_data(streamIds, streamNames, years):
    stream_df = pd.DataFrame([])
    for id in streamIds:
        server = 'api.nrgstream.com'
        year_df = pd.DataFrame([])
        for yr in years:
            accessToken, tokenExpiry = pull_nrg_data.getToken()
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
        # Re-index the year_df
        year_df.set_index('timeStamp', inplace=True)
        # Join year_df to outages dataframe
        stream_df = pd.concat([stream_df,year_df], axis=1, join='outer')
    return stream_df

# Pull historical data from Google BigQuery
@st.experimental_memo
def pull_grouped_hist():
    # Google BigQuery auth
    credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
    # Pull data
    query = 'SELECT * FROM nrgdata.grouped_hist'
    history_df = bigquery.Client(credentials=credentials).query(query).to_dataframe()
    #history_df['date'] = pd.to_datetime(history_df[['year','month','day']])
    return history_df

# Main code block
if __name__ == '__main__':

# App config
    st.set_page_config(layout='wide', initial_sidebar_state='auto', menu_items=None)
    st.title('Alberta Power Forecaster')
    hide_menu(True)

# Pull 24M Supply/Demand data from AESO
    streamIds = [278763]
    streamNames = {278763:'0'}
    years = [datetime.now().year, datetime.now().year+1, datetime.now().year+2]
    offset_df = stream_data(streamIds, streamNames, years)
    offset_df.rename(columns={'0':'Peak Hour', 2:'Expected Supply', 3:'Import Capacity', 4:'Load+Reserve', 5:'Surplus'}, inplace=True)
    st.write(offset_df)
    st.write(pull_grouped_hist())

# Pull live data
    st.write(current_data())
    for seconds in range(20):
        placeholder = st.empty()
        with placeholder.container():
            st.dataframe(current_data())
            time.sleep(10)

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

# Offset demand table & chart
    st.subheader('Forecasted & Adjusted Demand')
    # Creating cols for Streamlit app
    col1, col2 = st.columns([1,2])
    # Adding AgGrid table to Col1
    with col1:
        offset_df = AgGrid(offset_df[['Date','Open','Close','Offset']], grid_options, fit_columns_on_grid_load=True)['data']
    # Calculating cummulative offset
        offset_df = pd.DataFrame(offset_df)
        offset_df['tot_offset'] = offset_df['Offset'].cumsum()
        offset_df['Adjusted Demand'] = offset_df['Close'] + offset_df['tot_offset']
        offset_df = offset_df[1:]
    # Creating Altair charts
        # Demand candlestick chart
        open_close_color = alt.condition("datum.Open <= datum.Close",
                                 alt.value("#06982d"),
                                 alt.value("#ae1325"))
        candlestick = alt.Chart(offset_df).mark_bar().encode(
            x=alt.X('Date:T',axis=alt.Axis(format='%b %Y'),title=''),
            y=alt.Y('Open:Q', title='Demand (MW)'),
            y2=alt.Y2('Close:Q', title=''),
            color=open_close_color
        ).interactive( ).properties(height=430)
        # Adjusted Demand area chart
        area = alt.Chart(offset_df).mark_area(color='grey', opacity=0.3).encode(
            x='Date:T',
            y='Adjusted Demand:Q',
        ).interactive(bind_y=True)
        # Selector for sliding vertical line
        nearest = alt.selection(type='single', nearest=True, on='mouseover', fields=['Date'], empty='none', clear='mouseout')
        # Creating vertical line to highlight current Date
        rule = alt.Chart(offset_df).mark_rule(color='gray').encode(
            x='Date:T',
            opacity=alt.condition(nearest, alt.value(1), alt.value(0)),
            tooltip = ['Date:T', 
                        alt.Tooltip('Open:Q', title='Open (MW)', format=',d'),
                        alt.Tooltip('Close:Q', title='Close (MW)', format=',d'),
                        alt.Tooltip('Adjusted Demand:Q', title='Adjusted Close (MW)', format=',d')]
        ).add_selection(nearest)
    # Adding layered chart to Col2
    with col2:
        st.altair_chart(candlestick + area + rule, use_container_width=True)

# Outages chart
    st.subheader('Forecasted Outages')
    #Create outages_df
    streamIds = [44648, 118361, 322689, 118362, 147262, 322675, 322682, 44651]
    streamNames = {44648:'Coal', 118361:'Gas', 322689:'Dual Fuel', 118362:'Hydro', 147262:'Wind', 322675:'Solar', 322682:'Energy Storage', 44651:'Biomass & Other'}
    years = [datetime.now().year, datetime.now().year+1, datetime.now().year+2]
    outage_df = stream_data(streamIds, streamNames, years)
    
    # Reset index so dataframe can be plotted with Altair
    outage_df.reset_index(inplace=True)
    outage_df = pd.melt(outage_df, 
                    id_vars=['timeStamp'],
                    value_vars=['Coal', 'Gas', 'Dual Fuel', 'Hydro', 'Wind', 'Solar', 'Energy Storage', 'Biomass & Other'],
                    var_name='Source',
                    value_name='Value')
    # Outages area chart
    selection = alt.selection_interval(encodings=['x'])
    outage_area = alt.Chart(outage_df).mark_area(opacity=0.5).encode(
        x=alt.X('yearmonth(timeStamp):T', title=''),
        y=alt.Y('Value:Q', stack='zero', axis=alt.Axis(format=',f'), title='Outages (MW)'),
        color=alt.Color('Source:N', scale=alt.Scale(scheme='category20'), legend=alt.Legend(orient="top")),
        ).add_selection(selection).properties(width=1300)
    # Outages bar chart
    outage_bar = alt.Chart(outage_df).mark_bar(opacity=0.5).encode(
        x=alt.X('Value:Q', title='Outages (MW)'),
        y=alt.Y('Source:N',title=''),
        color=alt.Color('Source:N', scale=alt.Scale(scheme='category20'))
    ).transform_filter(selection).properties(width=1300)
    st.altair_chart(outage_area & outage_bar, use_container_width=True)

# Pull historical data
    history_df = pull_grouped_hist()
    hist = alt.Chart(history_df).mark_area().encode(
        x='date:T',
        y=alt.Y('Close:Q', stack='zero'),
        color='subfuelType'
    ).interactive()
    st.altair_chart(hist, use_container_width=True)