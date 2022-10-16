import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import time
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import pytz
import ssl
import json
import http.client
import certifi
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from google.oauth2 import service_account
import firebase_admin
from google.cloud import firestore
from firebase_admin import credentials
from firebase_admin import firestore
from google.cloud import bigquery
from pandasql import sqldf

# **** Create alert if data-manager is not running ****
    # Give option to open data-manager url and allow it to run in background
@st.experimental_singleton(suppress_st_warning=True)
def firestore_db_instance():
    try:
        firebase_admin.get_app()
    except:
        firebase_admin.initialize_app(credential=credentials.Certificate(st.secrets["gcp_service_account"]))
    return firestore.client()

def read_firestore(db, document):
    firestore_ref = db.collection(u'appData').document(document)
    df = pd.DataFrame.from_dict(firestore_ref.get().to_dict())
    df['fuelType'] = df['fuelType'].astype('category')
    df['timeStamp'] = df['timeStamp'].dt.tz_convert('America/Edmonton')
    return df

@st.experimental_memo(suppress_st_warning=True)
def oldOutage_df(outageTable):
    cred = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
    query = f'''
    SELECT * FROM `{outageTable}`
    WHERE loadDate = 
        (SELECT MIN(loadDate) FROM 
            (SELECT loadDate FROM `{outageTable}`
            GROUP BY loadDate
            ORDER BY loadDate DESC
            LIMIT 5))
    ORDER BY fuelType, timeStamp
    '''
    df = bigquery.Client(credentials=cred).query(query).to_dataframe()
    df = df[['timeStamp','fuelType','value']]
    df['timeStamp'] = df['timeStamp'].dt.tz_convert(tz)
    df['fuelType'] = df['fuelType'].astype('category')
    return df

# MONTHLY OUTAGE DATA

# Set global parameters
tz = pytz.timezone('America/Edmonton')
db = firestore_db_instance()

history_df = read_firestore(db,'historicalData')
current_df = read_firestore(db,'currentData')
oldDailyOutage_df = oldOutage_df('outages.dailyOutages')
oldMonthlyOutage_df = oldOutage_df('outages.monthlyOutages')
# windSolar_df = ***pull from FS***

placeholder = st.empty()
for seconds in range(10):
    

    # with placeholder.container():

    time.sleep(1)


# KPIs

# CURRENT SUPPLY CHART

# 7-DAY DAILY OUTAGES CHART

# 90-DAY DAILY OUTAGES CHART

# 2-YEAR MONTHLY OUTAGES CHART

# DAILY OUTAGE DIFFS CHART

# MONTHLY OUTAGE DIFFS CHART