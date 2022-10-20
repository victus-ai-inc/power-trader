import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import time
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import pytz
from google.oauth2 import service_account
import firebase_admin
from google.cloud import firestore
from firebase_admin import credentials
from firebase_admin import firestore
from google.cloud import bigquery

# **** Create alert if data-manager is not running ****
    # Give option to open data-manager url and allow it to run in background

def hideMenu():
    hide_menu_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            .css-1j15ncu {visibility: hidden;}
            .css-14x9thb {visibility: hidden;}
            </style>
            """
    st.markdown(hide_menu_style, unsafe_allow_html=True)

@st.experimental_singleton(suppress_st_warning=True)
def firestore_db_instance():
    try:
        firebase_admin.get_app()
    except:
        firebase_admin.initialize_app(credential=credentials.Certificate(st.secrets["gcp_service_account"]))
    return firestore.client()

@st.experimental_memo(suppress_st_warning=True)
def read_firestore_history(_db):
    firestore_ref = _db.collection('appData').document('historicalData')
    df = pd.DataFrame.from_dict(firestore_ref.get().to_dict())
    df['fuelType'] = df['fuelType'].astype('category')
    df['timeStamp'] = df['timeStamp'].dt.tz_convert('America/Edmonton')
    return df

@st.experimental_memo(suppress_st_warning=True, ttl=20)
def read_firestore(_db, document):
    firestore_ref = _db.collection('appData').document(document)
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

def sevenDayCurrentChart(sevenDay_df, theme):
    st.subheader('Current Supply (Previous 7-days)')
    thm = {k:v for k,v in theme.items() if k not in ['Intertie','3-Day Solar Forecast','7-Day Wind Forecast']}
    combo_base = alt.Chart(sevenDay_df).encode(
        x=alt.X(
            'timeStamp:T', 
            title='',
            axis=alt.Axis(labelAngle=270, gridWidth=0)),
        y=alt.Y(
            'value:Q',
            stack='zero',
            title='Current Supply (MW)',
            scale=alt.Scale(domain=[-1000,11000])),
        color=alt.Color(
            'fuelType:N',
            scale=alt.Scale(
                domain=list(thm.keys()),
                range=list(thm.values())),
                legend=alt.Legend(orient='top')),
        tooltip=[
            alt.Tooltip('fuelType',title='Fuel Type'),
            alt.Tooltip('value',title='Value'),
            alt.Tooltip('yearmonthdatehoursminutes(timeStamp)',title='Date/Time')],
    ).properties(height=400)
    
    combo_area = combo_base.mark_area(opacity=0.7).transform_filter(alt.datum.fuelType!='Pool Price')
    
    combo_line = combo_base.mark_line(interpolate='step-after').encode(
        y=alt.Y(
            'value:Q',
            title='Price ($)',
            scale=alt.Scale(domain=[-100,1100])),
        color=alt.Color('fuelType:N')
    ).transform_filter(alt.datum.fuelType=='Pool Price')

    return st.altair_chart(alt.layer(combo_area,combo_line).resolve_scale(y='independent'), use_container_width=True)

def sevenDayOutageChart(sevenDayOutage_df, windSolar_df, theme):
    st.subheader('Daily Outages (Next 7-days)')
    thm = {k:v for k,v in theme.items() if k not in ['BC','Saskatchewan','Montana','Pool Price']}
    sevenDayOutageBase = alt.Chart(sevenDayOutage_df).encode(
        x=alt.X(
            'timeStamp:T',
            title=None,
            axis=alt.Axis(labelAngle=270)),
        tooltip=[
            alt.Tooltip('fuelType',title='Fuel Type'),
            alt.Tooltip('value',title='Value'),
            alt.Tooltip('yearmonthdatehoursminutes(timeStamp)',title='Date/Time')],
    ).properties(height=400)
    
    sevenDayOutagArea = sevenDayOutageBase.mark_area(opacity=0.7).encode(
        y=alt.Y(
            'value:Q',
            stack='zero',
            axis=alt.Axis(format=',f'),
            title='Outages (MW)'),
        color=alt.Color(
            'fuelType:N',
            scale=alt.Scale(
                domain=list(thm.keys()),
                range=list(thm.values())),
            legend=alt.Legend(orient='top')),
    ).transform_filter(
        {'not':alt.FieldOneOfPredicate(
            field='fuelType',
            oneOf=['7-Day Wind Forecast','3-Day Solar Forecast'])}
    )
    
    sevenDayOutageLine = sevenDayOutageBase.mark_line(opacity=0.7, strokeWidth=3).encode(
        y=alt.Y(
            'value:Q',
            title='Wind & Solar Forecast (MW)'),
        color='fuelType:N',
    ).transform_filter(
        alt.FieldOneOfPredicate(
            field='fuelType',
            oneOf=['7-Day Wind Forecast','3-Day Solar Forecast'])
    )
    
    st.altair_chart(alt.layer(sevenDayOutagArea, sevenDayOutageLine).resolve_scale(y='independent'), use_container_width=True)

def ninetyDayOutageChart(currentDailyOutage_df, theme):
    st.subheader('Daily Outages (Next 90-days)')
    thm = {k:v for k,v in theme.items() if k not in ['BC','Saskatchewan','Montana','Pool Price','7-Day Wind Forecast','3-Day Solar Forecast']}
    daily_outage_area = alt.Chart(daily_outage).mark_area(opacity=0.8).encode(
        x=alt.X('timeStamp:T', title='', axis=alt.Axis(labelAngle=270)),
        y=alt.Y('value:Q', stack='zero', axis=alt.Axis(format=',f'), title='Outages (MW)'),
        color=alt.Color('fuelType:N', scale=alt.Scale(domain=list(thm.keys()), range=list(thm.values())), legend=alt.Legend(orient='top')),
        tooltip=['fuelType','value','timeStamp']
    ).properties(height=400).configure_view(strokeWidth=0).configure_axis(grid=False)
    st.altair_chart(daily_outage_area, use_container_width=True)

# Set global parameters
st.set_page_config(layout='wide', initial_sidebar_state='collapsed', menu_items=None)
tz = pytz.timezone('America/Edmonton')
db = firestore_db_instance()
theme = {'Biomass & Other':'#1f77b4', 'Coal':'#aec7e8', 'Dual Fuel':'#ff7f0e', 'Energy Storage':'#ffbb78', 'Hydro':'#2ca02c', 'Natural Gas':'#98df8a',
            'Solar':'#d62728', 'Wind':'#7f7f7f', 'BC':'#9467bd', 'Saskatchewan':'#c5b0d5', 'Montana':'#e377c2', 'Intertie':'#17becf', 'Pool Price':'#000000',
            '3-Day Solar Forecast':'#d62728','7-Day Wind Forecast':'#3f3f3f'}
hideMenu()

# oldDailyOutage_df = oldOutage_df('outages.dailyOutages') # **** MIGHT NEED TO FILTER DATA TO >= TODAY*****
# oldMonthlyOutage_df = oldOutage_df('outages.monthlyOutages') # **** MIGHT NEED TO FILTER DATA TO >= TODAY*****
# 
# currentMonthlyOutage_df = read_firestore(db,'monthlyOutages')


history_df = read_firestore_history(db)

placeholder = st.empty()
for seconds in range(300):
    # Current supply
    current_df = read_firestore(db,'currentData')
    sevenDayCurrent_df = pd.concat([history_df, current_df], axis=0)
    # Daily outages
    currentDailyOutage_df = read_firestore(db,'dailyOutages')
    sevenDayOutage_df = currentDailyOutage_df[currentDailyOutage_df['timeStamp'].dt.date <= datetime.now(tz).date() + timedelta(days=7)]
    windSolar_df = read_firestore(db, 'windSolar')
    sevenDayOutage_df = pd.concat([sevenDayOutage_df, windSolar_df], axis=0)

    with placeholder.container():
        # Charts
        col1, col2 = st.columns(2)
        with col1:
            sevenDayCurrentChart(sevenDayCurrent_df, theme)
        with col2:
            sevenDayOutageChart(sevenDayOutage_df, windSolar_df, theme)
    time.sleep(1)
st.experimental_rerun()


# KPIs

# CURRENT SUPPLY CHART

# 7-DAY DAILY OUTAGES CHART

# 90-DAY DAILY OUTAGES CHART

# 2-YEAR MONTHLY OUTAGES CHART

# DAILY OUTAGE DIFFS CHART

# MONTHLY OUTAGE DIFFS CHART