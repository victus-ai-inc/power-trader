#Streamlit app to monitor historical/future power supply/demand in Alberta

import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
from st_aggrid import AgGrid
#import NRGstream script

# Configuring app setup
st.set_page_config(layout='wide', initial_sidebar_state='auto', menu_items=None)
st.title('Alberta Power Trader')

st.sidebar.write('test')

if __name__ == '__main__':
    #call NRGstream def
    pass
