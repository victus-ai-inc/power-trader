#Streamlit app to monitor historical/future power supply/demand in Alberta
import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
from st_aggrid import AgGrid
from datetime import datetime, timedelta, date
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
    fromDate = st.sidebar.date_input('Start Date', value=datetime.now()-timedelta(30))
    toDate = st.sidebar.date_input('End Date', min_value=fromDate)
    fromDate = fromDate.strftime('%m/%d/%Y')
    toDate = toDate.strftime('%m/%d/%Y')

    # Stream Ids
        #AB Internal Load Demand (5min) = 225
        #AB Internal Load Demand (1min) = 139308
        #24 month supply demand forecast = 278763
    streamIds = [225]
    # Pull NRG data
    pull_nrg_data.pull_data(fromDate, toDate, streamIds)

