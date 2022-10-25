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

@st.experimental_memo(suppress_st_warning=True, ttl=7)
def read_firestore(_db, document):
    firestore_ref = _db.collection('appData').document(document)
    df = pd.DataFrame.from_dict(firestore_ref.get().to_dict())
    df['fuelType'] = df['fuelType'].astype('category')
    df['timeStamp'] = df['timeStamp'].dt.tz_convert('America/Edmonton')
    return df

def displayKPI(left_df, right_df, title):
    # Merging KPIs into one dataframe
    kpi_df = left_df.merge(right_df, how='left', on='fuelType', suffixes=('Previous','Current'))
    # Creating KPI delta calculation
    kpi_df['delta'] = kpi_df['valueCurrent'] - kpi_df['valuePrevious']
    kpi_df['absDelta'] = abs(kpi_df['delta'])
    # Formatting numbers 
    kpi_df.iloc[:,1:] = kpi_df.iloc[:,1:].applymap('{:.0f}'.format)
    col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12 = st.columns(12)
    with col1:
        st.subheader(title)
    col2.metric(label=kpi_df.iloc[7,0], value=kpi_df.iloc[7,2], delta=kpi_df.iloc[7,3]) # Natural Gas
    col3.metric(label=kpi_df.iloc[5,0], value=kpi_df.iloc[5,2], delta=kpi_df.iloc[5,3]) # Hydro
    col4.metric(label=kpi_df.iloc[4,0], value=kpi_df.iloc[4,2], delta=kpi_df.iloc[4,3]) # Energy Storage
    col5.metric(label=kpi_df.iloc[10,0], value=kpi_df.iloc[10,2], delta=kpi_df.iloc[10,3]) # Solar
    col6.metric(label=kpi_df.iloc[11,0], value=kpi_df.iloc[11,2], delta=kpi_df.iloc[11,3]) # Wind
    col7.metric(label=kpi_df.iloc[3,0], value=kpi_df.iloc[3,2], delta=kpi_df.iloc[3,3]) # Dual Fuel
    col8.metric(label=kpi_df.iloc[2,0], value=kpi_df.iloc[2,2], delta=kpi_df.iloc[2,3]) # Coal
    col9.metric(label=kpi_df.iloc[0,0], value=kpi_df.iloc[0,2], delta=kpi_df.iloc[0,3]) # BC
    col10.metric(label=kpi_df.iloc[6,0], value=kpi_df.iloc[6,2], delta=kpi_df.iloc[6,3]) # Montanta
    col11.metric(label=kpi_df.iloc[9,0], value=kpi_df.iloc[9,2], delta=kpi_df.iloc[9,3]) # Sask
    col12.metric(label=kpi_df.iloc[8,0], value=kpi_df.iloc[8,2], delta=kpi_df.iloc[8,3]) # Pool Price
    return kpi_df

def kpi(current_df):
    currentHour_df = current_df[['fuelType','value']][current_df['timeStamp']==max(current_df['timeStamp'])]
    currentHourAvg_df = current_df[current_df['timeStamp'].dt.hour==datetime.now(tz).hour].groupby(['fuelType']).mean().reset_index()
    previousHourAvg_df = current_df[current_df['timeStamp'].dt.hour==(datetime.now(tz)-timedelta(hours=1)).hour].groupby(['fuelType']).mean().reset_index()
    displayKPI(previousHourAvg_df, currentHour_df, 'Real Time')
    displayKPI(previousHourAvg_df, currentHourAvg_df, 'Hourly Average')

def warning(type, lst):
    if type == 'warning':
        background_color = 'rgba(214, 39, 40, .1)'
        border_color = 'rgba(214, 39, 40, .2)'
        text_color = 'rgba(214, 39, 40, 0.6)'
    elif type == 'alert':
        background_color = 'rgba(31, 119, 180, .1)'
        border_color = 'rgba(31, 119, 180, .2)'
        text_color = 'rgba(31, 119, 180, 0.6)'
    st.markdown(f'''<p style="border-radius: 6px;
                -webkit-border-radius: 6px;
                background-color: {background_color};
                background-position: 9px 0px;
                background-repeat: no-repeat;
                border: solid 1px {border_color};
                border-radius: 6px;
                line-height: 18px;
                overflow: hidden;
                font-size:24px;
                font-weight: bold;
                color: {text_color};
                text-align: center;
                padding: 15px 10px;">{lst}</p>''', unsafe_allow_html=True)

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

def diff_calc(old_df, new_df):
    diff_df = pd.merge(new_df, old_df, on=['timeStamp','fuelType'], suffixes=('_new','_old'))
    diff_df.loc[diff_df['value_old']==0,'value_new']=0
    diff_df['diff_value'] = diff_df['value_new'] - diff_df['value_old']
    diff_df = diff_df[['timeStamp','fuelType','diff_value']]
    diff_df = diff_df.groupby('fuelType').filter(lambda x: x['diff_value'].mean() != 0)
    return diff_df

def sevenDayCurrentChart(sevenDay_df, theme):
    st.subheader('Current Supply')
    st.markdown('**Previous 7-days**')
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
                legend=alt.Legend(
                    orient='bottom',
                    title='Fuel Type',
                    columns=4)),
        tooltip=[
            alt.Tooltip('fuelType',title='Fuel Type'),
            alt.Tooltip('value',title='Value (MW)'),
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

def sevenDayOutageChart(sevenDayOutage_df, theme):
    st.subheader('Daily Outages')
    st.markdown('**Next 7-days**')
    thm = {k:v for k,v in theme.items() if k not in ['BC','Saskatchewan','Montana','Pool Price']}
    sevenDayOutageBase = alt.Chart(sevenDayOutage_df).encode(
        x=alt.X(
            'timeStamp:T',
            title=None,
            axis=alt.Axis(labelAngle=270)),
        tooltip=[
            alt.Tooltip('fuelType',title='Fuel Type'),
            alt.Tooltip('value',title='Value (MW)'),
            alt.Tooltip('yearmonthdatehoursminutes(timeStamp)',title='Date/Time')],
    ).properties(height=400)
    
    sevenDayOutageArea = sevenDayOutageBase.mark_area(opacity=0.7).encode(
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
            legend=alt.Legend(
                orient='bottom',
                title='Fuel Type',
                columns=4)),
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
    
    st.altair_chart(alt.layer(sevenDayOutageArea, sevenDayOutageLine).resolve_scale(y='independent'), use_container_width=True)

def ninetyDayOutageChart(ninetyDayOutage_df, theme):
    st.subheader('Daily Outages')
    st.markdown('**Next 90-days**')
    thm = {k:v for k,v in theme.items() if k not in ['BC','Saskatchewan','Montana','Pool Price','7-Day Wind Forecast','3-Day Solar Forecast']}
    daily_outage_area = alt.Chart(ninetyDayOutage_df).mark_area(opacity=0.8).encode(
        x=alt.X(
            'timeStamp:T',
            title=None,
            axis=alt.Axis(labelAngle=270)),
        y=alt.Y(
            'value:Q',
            stack='zero',
            axis=alt.Axis(
                format=',f'),
            title='Outages (MW)'),
        color=alt.Color(
            'fuelType:N',
            scale=alt.Scale(
                domain=list(thm.keys()),
                range=list(thm.values())),
            legend=alt.Legend(
                orient='bottom',
                title='Fuel Type',
                columns=4)),
        tooltip=[
            alt.Tooltip('fuelType',title='Fuel Type'),
            alt.Tooltip('value',title='Value (MW)'),
            alt.Tooltip('yearmonthdatehoursminutes(timeStamp)',title='Date/Time')],
    ).properties(height=400).configure_view(strokeWidth=0).configure_axis(grid=False)
    st.altair_chart(daily_outage_area, use_container_width=True)

def monthlyOutagesChart(currentMonthlyOutage_df, theme):
    st.subheader('Monthly Outages')
    st.markdown('**Next 2-years**')
    thm = {k:v for k,v in theme.items() if k not in ['BC','Saskatchewan','Montana','Pool Price','Intertie','7-Day Wind Forecast','3-Day Solar Forecast']}
    monthly_outage_area = alt.Chart(currentMonthlyOutage_df).mark_bar(opacity=0.7).encode(
        x=alt.X(
            'yearmonth(timeStamp):O',
            title=None,
            axis=alt.Axis(labelAngle=270)),
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
            legend=alt.Legend(
                orient='bottom',
                title='Fuel Type',
                columns=4)),
        tooltip=[
            alt.Tooltip('fuelType',title='Fuel Type'),
            alt.Tooltip('value',title='Value (MW)'),
            alt.Tooltip('yearmonth(timeStamp)',title='Date/Time')],
        ).properties(height=400).configure_view(strokeWidth=0).configure_axis(grid=False)
    st.altair_chart(monthly_outage_area, use_container_width=True)

def outageDiffChart(dateFormat, outageDiff_df, outageAlertList):
    if len(outageAlertList)>0:
        outageHeatmapChart = alt.Chart(outageDiff_df
        ).mark_rect(
            opacity=0.9,
            stroke='grey',strokeWidth=0
        ).encode(
            x=alt.X(
                f'{dateFormat}(timeStamp):O',
                title=None,
                axis=alt.Axis(
                    ticks=False,
                    labelAngle=270)),
            y=alt.Y(
                'fuelType:N',
                title=None,
                axis=alt.Axis(labelFontSize=15)),
            color=alt.condition(
                alt.datum.diff_value==0,
                alt.value('white'),
                alt.Color(
                    'diff_value:Q',
                    scale=alt.Scale(
                        domainMin=-max(abs(outageDiff_df['diff_value'])),
                        domainMax=max(abs(outageDiff_df['diff_value'])),
                        scheme='redyellowgreen'),
                    legend=alt.Legend(
                        title='Value (MW)',
                        columns=2))),
            tooltip=[
                alt.Tooltip('fuelType',title='Fuel Type'),
                alt.Tooltip('diff_value',title='Value (MW)'),
                alt.Tooltip(f'{dateFormat}(timeStamp)',title='Date/Time')],
        ).properties(height=105 if len(outageAlertList)==1 else 60 * len(outageAlertList)
        ).configure_view(strokeWidth=0
        ).configure_axis(grid=False)
        st.altair_chart(outageHeatmapChart, use_container_width=True)

# Set global parameters
st.set_page_config(layout='wide', initial_sidebar_state='collapsed', menu_items=None)
tz = pytz.timezone('America/Edmonton')
db = firestore_db_instance()
theme = {'Biomass & Other':'#1f77b4',
        'Coal':'#aec7e8',
        'Dual Fuel':'#ff7f0e',
        'Energy Storage':'#ffbb78',
        'Hydro':'#2ca02c',
        'Natural Gas':'#98df8a',
        'Solar':'#d62728',
        'Wind':'#7f7f7f',
        'BC':'#9467bd',
        'Saskatchewan':'#c5b0d5',
        'Montana':'#e377c2',
        'Intertie':'#17becf',
        'Pool Price':'#000000',
        '3-Day Solar Forecast':'#d62728',
        '7-Day Wind Forecast':'#3f3f3f'}
cutoff = 100
hideMenu()

history_df = read_firestore_history(db)
if max(history_df['timeStamp']) < datetime.now(tz)-relativedelta(days=1,hour=23,minute=55,second=0,microsecond=0):
    st.alert('Updating history')
    read_firestore_history.clear()
    history_df = read_firestore_history(db)

placeholder = st.empty()
for seconds in range(85): # 85 iterations x 7 second wait time/iteration = Reset after 600 seconds

# Current supply
    current_df = read_firestore(db,'currentData')
    sevenDayCurrent_df = pd.concat([history_df, current_df], axis=0)

# Daily outages
    oldDailyOutage_df = oldOutage_df('outages.dailyOutages')
    currentDailyOutage_df = read_firestore(db,'dailyOutages')
    dailyOutageDiff_df = diff_calc(oldDailyOutage_df, currentDailyOutage_df)
    dailyOutageAlertList = dailyOutageDiff_df['fuelType'].unique()

    sevenDayOutage_df = currentDailyOutage_df[currentDailyOutage_df['timeStamp'].dt.date <= datetime.now(tz).date() + timedelta(days=7)]
    windSolar_df = read_firestore(db, 'windSolar')
    sevenDayOutage_df = pd.concat([sevenDayOutage_df, windSolar_df], axis=0)
    
    ninetyDayOutage_df = currentDailyOutage_df[currentDailyOutage_df['timeStamp'].dt.date <= datetime.now(tz).date() + timedelta(days=90)]
# Monthly Outages
    oldMonthlyOutage_df = oldOutage_df('outages.monthlyOutages')
    currentMonthlyOutage_df = read_firestore(db,'monthlyOutages')
    monthlyOutageDiff_df = diff_calc(oldMonthlyOutage_df, currentMonthlyOutage_df)
    monthlyOutageAlertList = monthlyOutageDiff_df['fuelType'].unique()

    with placeholder.container():
        # KPIs
        with st.expander('*Click here to expand/collapse KPIs',expanded=True):
            kpi(current_df)
            st.write(f"Last update: {datetime.now(tz).strftime('%a, %b %d @ %X')}")
        # KPI warning & alert boxes
            col1, col2 = st.columns(2)
            with col1:
                if len(dailyOutageAlertList) > 0:
                    for fuelType in dailyOutageAlertList:
                        warning('warning', fuelType)
            with col2:
                if len(monthlyOutageAlertList) > 0:
                    for fuelType in monthlyOutageAlertList:
                        warning('alert', fuelType)
        # Charts
        col3, col4 = st.columns(2)
        with col3:
            sevenDayCurrentChart(sevenDayCurrent_df, theme)
            ninetyDayOutageChart(ninetyDayOutage_df, theme)
            st.subheader('Daily Intertie & Outage')
            st.markdown('**+/- vs 7 days ago**')
            outageDiffChart('yearmonthdate', dailyOutageDiff_df, dailyOutageAlertList)
        with col4:
            sevenDayOutageChart(sevenDayOutage_df, theme)
            monthlyOutagesChart(currentMonthlyOutage_df, theme)
            st.subheader('Monthly Outage')
            st.markdown('**+/- vs 7 days ago**')
            outageDiffChart('yearmonth', monthlyOutageDiff_df, monthlyOutageAlertList)
    time.sleep(7)
st.experimental_rerun()