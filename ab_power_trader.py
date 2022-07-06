#Streamlit app to monitor historical/future power supply/demand in Alberta
from turtle import color
import streamlit as st
import pandas as pd
import numpy as np
import json
import altair as alt
from st_aggrid import AgGrid
from datetime import datetime, timedelta
import pull_nrg_data

def get_nrg_creds():
    username = st.secrets["nrg_username"]
    password = st.secrets["nrg_password"]
    return username, password

if __name__ == '__main__':
# App config
    st.set_page_config(layout='wide', initial_sidebar_state='auto', menu_items=None)
    st.title('Alberta Power Forecaster')

# Sidebar config
    # fromDate = st.sidebar.date_input('Start Date', value=datetime.now()-timedelta(1))
    # toDate = st.sidebar.date_input('End Date', min_value=fromDate)+timedelta(1)
    # fromDate = fromDate.strftime('%m/%d/%Y')
    # toDate = toDate.strftime('%m/%d/%Y')

    # Pull NRG data
    forecast = pd.DataFrame([])
    for forecast_num in range(1,5):
        df = pd.read_csv(f'http://ets.aeso.ca/Market/Reports/Manual/supply_and_demand/csvData/{forecast_num}-6month.csv', on_bad_lines='skip')
        df = df[~df['Report Date'].str.contains('Disclaimer')]
        forecast = pd.concat([forecast,df],axis=0)

    offset_df = forecast[['Report Date','AIL Load + Operating Reserves (MW)']]
    
    offset_df['Offset'] = 0
    offset_df.rename(columns={'Report Date':'Date','AIL Load + Operating Reserves (MW)':'Demand'},inplace=True)
     
    grid_options = {
        "columnDefs": [
            {
                "headerName": "Date",
                "field": "Date",
                "editable": False,
            },
            {
                "headerName": "Demand",
                "field": "Demand",
                "editable": False,
            },
            {
                "headerName": "Offset",
                "field": "Offset",
                "editable": True,
            },
        ],
    }

    col1, col2 = st.columns([1,2])

    with col1:
        offset_df = AgGrid(offset_df[['Date','Demand','Offset']], grid_options)['data']
    
    offset_df=pd.DataFrame(offset_df)
    offset_df['tot_offset'] = offset_df['Offset'].cumsum()
    offset_df['Adjusted Demand'] = offset_df['Demand'] + offset_df['tot_offset']
    
    brush = alt.selection_interval(encodings=['x'])
    line = alt.Chart(offset_df).mark_line(color='black').encode(
        x='Date:T',
        y=alt.Y('Demand:Q'),
    ).add_selection(brush)
    area = alt.Chart(offset_df).mark_area(color='green', opacity=0.3).encode(
        x='Date:T',
        y='Adjusted Demand:Q'
    )
    
    with col2:
        st.altair_chart(line + area, use_container_width=True)
    


    


