import streamlit as st
from datetime import datetime, timedelta
import pandas as pd

st.experimental_memo()
def dct():
    df = {'email':{'logon':datetime.now()-timedelta(hours=1),'logoff':datetime.now()}}
    return df
a = dct()
a