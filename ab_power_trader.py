#Streamlit app to monitor historical/future power supply/demand in Alberta
import streamlit as st
import pandas as pd
import altair as alt
from st_aggrid import AgGrid
from datetime import datetime, timedelta

def get_nrg_creds():
    username = st.secrets["nrg_username"]
    password = st.secrets["nrg_password"]
    return username, password

def hide_menu(bool):
    if bool == True:
        hide_menu_style = """
        <style>
        #MainMenu {visibility:hidden;}
        footer {visibility:hidden;}
        </style>
        """
    return st.markdown(hide_menu_style, unsafe_allow_html=True)

if __name__ == '__main__':
# App config
    hide_menu(True)
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
    offset_df['Year'] = pd.DatetimeIndex(offset_df['Date']).year
    offset_df['Month'] = pd.DatetimeIndex(offset_df['Date']).month

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
                "headerName": "Demand",
                "field": "Demand",
            },
            {
                "headerName": "Offset",
                "field": "Offset",
                "editable": True,
                "singleClickEdit":True,
            },
        ],
    }

    col1, col2 = st.columns([1,2])

    with col1:
        offset_df = AgGrid(offset_df[['Date','Demand','Offset']], grid_options, fit_columns_on_grid_load=True)['data']
    
    offset_df=pd.DataFrame(offset_df)
    offset_df['tot_offset'] = offset_df['Offset'].cumsum()
    offset_df['Adjusted Demand'] = offset_df['Demand'] + offset_df['tot_offset']

    line = alt.Chart(offset_df).mark_line(color='black').encode(
        x=alt.X('Date:T',axis=alt.Axis(format='%b %Y'),title=''),
        y=alt.Y('Demand:Q', title='Demand (MW)'),
    ).properties(
        height=430,
    )

    area = alt.Chart(offset_df).mark_area(color='green', opacity=0.3).encode(
        x='Date:T',
        y='Adjusted Demand:Q',
    ).interactive(bind_y=True)

    nearest = alt.selection(type='single', nearest=True, on='mouseover', fields=['Date'], empty='none', clear="mouseout")
    
    rule = alt.Chart(offset_df).mark_rule(color='gray').encode(
        x='Date:T',
        opacity=alt.condition(nearest, alt.value(1), alt.value(0)),
        tooltip = ['Date:T','Demand','Adjusted Demand']
    ).add_selection(nearest)

    with col2:
        st.altair_chart(line + area + rule, use_container_width=True)