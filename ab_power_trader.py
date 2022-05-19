#Streamlit app to monitor historical/future power supply/demand in Alberta
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
    st.title('Alberta Power Trader')

# Sidebar config
    fromDate = st.sidebar.date_input('Start Date', value=datetime.now()-timedelta(1))
    toDate = st.sidebar.date_input('End Date', min_value=fromDate)+timedelta(1)
    fromDate = fromDate.strftime('%m/%d/%Y')
    toDate = toDate.strftime('%m/%d/%Y')

    # Stream Ids
        #AB Internal Load Demand (5min) = 225
        #AB Internal Load Demand (1min) = 139308
        #24 month supply demand forecast = 278763
    streamId = [139308]
    # Pull NRG data
    df = pull_nrg_data.pull_data(fromDate, toDate, streamId)
    #meta = pd.json_normalize(df, record_path=['columns'])
    #st.write(meta)
    df = pd.json_normalize(df, record_path=['data'])
    st.write(df)
    
    


