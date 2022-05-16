#Streamlit app to monitor historical/future power supply/demand in Alberta

import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
from st_aggrid import AgGrid
import datetime
#import NRGstream py script 
#https://stackoverflow.com/questions/1186789/what-is-the-best-way-to-call-a-script-from-another-script

# App config
st.set_page_config(layout='wide', initial_sidebar_state='auto', menu_items=None)
st.title('Alberta Power Trader')

def get_nrg_creds():
    username = st.secrets["nrg_username"]
    password = st.secrets["nrg_password"]
    return username, password

# Sidebar config
start_date = st.sidebar.date_input('Start Date', value=datetime.datetime.now()-datetime.timedelta(30))
end_date = st.sidebar.date_input('End Date', min_value=start_date)

if __name__ == '__main__':
    #call NRGstream py script
    pass