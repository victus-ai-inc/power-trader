#Streamlit app to monitor historical/future power supply/demand in Alberta
import streamlit as st
import pandas as pd
import altair as alt
import pull_nrg_data
import json
import http.client
import certifi
import ssl
from st_aggrid import AgGrid
from datetime import datetime, date, timedelta

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
def outage_data():
    # Define streams & years
    streamIds = [44648, 118361, 322689, 118362, 147262, 322675, 322682, 44651]
    streamNames = {44648:'Coal', 118361:'Gas', 322689:'Dual Fuel', 118362:'Hydro', 147262:'Wind', 322675:'Solar', 322682:'Energy Storage', 44651:'Biomass & Other'}
    year = [datetime.now().year,datetime.now().year+1,datetime.now().year+2]
    outages = pd.DataFrame([])

    for id in streamIds:
        accessToken, tokenExpiry = pull_nrg_data.getToken()
        server = 'api.nrgstream.com'
        stream_df = pd.DataFrame([])
        # Pull NRG API access token
        if datetime.now() >= tokenExpiry:
                accessToken, tokenExpiry = pull_nrg_data.getToken()
        for yr in year:
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
            stream_df = pd.concat([stream_df,df], axis=0)
        # Rename stream_df cols
        stream_df.rename(columns={0:'timeStamp', 1:f'{streamNames[id]}'}, inplace=True)
        # Change timeStamp to datetime
        stream_df['timeStamp'] = pd.to_datetime(stream_df['timeStamp'])
        # Re-index the stream_df
        stream_df.set_index('timeStamp', inplace=True)
        # Join stream_df to outages dataframe
        outages = pd.concat([outages,stream_df], axis=1, join='outer')
        # Release NRG API access token
        pull_nrg_data.release_token(accessToken)
    # Reset index so dataframe can be plotted with Altair
    outages.reset_index(inplace=True)
    outages = pd.melt(outages, 
                    id_vars=['timeStamp'],
                    value_vars=['Coal', 'Gas', 'Dual Fuel', 'Hydro', 'Wind', 'Solar', 'Energy Storage', 'Biomass & Other'],
                    var_name='Source',
                    value_name='Value')
    return outages

# Main code block
if __name__ == '__main__':
# App config
    st.set_page_config(layout='wide', initial_sidebar_state='auto', menu_items=None)
    st.title('Alberta Power Forecaster')
    hide_menu(True)

# Sidebar config
    # fromDate = st.sidebar.date_input('Start Date', value=datetime.now()-timedelta(1))
    # toDate = st.sidebar.date_input('End Date', min_value=fromDate)+timedelta(1)
    # fromDate = fromDate.strftime('%m/%d/%Y')
    # toDate = toDate.strftime('%m/%d/%Y')

# Pull 24M Supply/Demand data from AESO
    forecast = pd.DataFrame([])
    # Scraping data from AESO website
    for forecast_num in range(1,5):
        df = pd.read_csv(f'http://ets.aeso.ca/Market/Reports/Manual/supply_and_demand/csvData/{forecast_num}-6month.csv', on_bad_lines='skip')
        df = df[~df['Report Date'].str.contains('Disclaimer')]
        forecast = pd.concat([forecast,df],axis=0)
# Creating offset_df
    offset_df = forecast[['Report Date','AIL Load + Operating Reserves (MW)']]
    offset_df['Offset'] = 0
    offset_df.rename(columns={'Report Date':'Date','AIL Load + Operating Reserves (MW)':'Close'},inplace=True)
    offset_df['Open'] = offset_df['Close'].shift(periods=1)
    offset_df['Year'] = pd.DatetimeIndex(offset_df['Date']).year
    offset_df['Month'] = pd.DatetimeIndex(offset_df['Date']).month
    
# Grid options for AgGrid table
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
    
# Outages chart
    st.subheader('Forecasted Outages')
    outage_df = outage_data()
    selection = alt.selection_interval(encodings=['x'])
    outage_area = alt.Chart(outage_df).mark_area(opacity=0.5).encode(
        x=alt.X('yearmonth(timeStamp):T', title=''),
        y=alt.Y('Value:Q', stack='zero', axis=alt.Axis(format='f'), title=''),
        color=alt.Color('Source:N', scale=alt.Scale(scheme='category20'), legend=alt.Legend(orient="top")),
        ).add_selection(selection).properties(width=1600)
    outage_bar = alt.Chart(outage_df).mark_bar(opacity=0.5).encode(
        x='Value:Q',
        y=alt.Y('Source:N',title=''),
        color=alt.Color('Source:N', scale=alt.Scale(scheme='category20'))
    ).transform_filter(selection).properties(width=1600)
    st.altair_chart(outage_area & outage_bar, use_container_width=True)

# Offset demand table & chart
    st.subheader('Adjusted Demand')
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
        area = alt.Chart(offset_df).mark_area(color='green', opacity=0.3).encode(
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
    

