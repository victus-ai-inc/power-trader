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

# Function to hide top and bottom menus on Streamlit app
def hide_menu(bool):
    if bool == True:
        hide_menu_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            </style>
            """
        return st.markdown(hide_menu_style, unsafe_allow_html=True)

# Main code block
if __name__ == '__main__':
# App config
    st.set_page_config(layout='wide', initial_sidebar_state='auto', menu_items=None)
    st.title('Alberta Power Forecaster')
    hide_menu(True)

# Sidebar config
    # fromDate = st.sidebar.date_input('Start Date', value=datetime.now()-timedelta(1))
    # toDate = st.sidebar.date_input('End Date', min_value=fromDate)+timedelta(1)
    # fromDate = fromDate.strftime('%m/%d/%Y')
    # toDate = toDate.strftime('%m/%d/%Y')

# Pull 24M Supply/Demand data from AESO
    forecast = pd.DataFrame([])
    # Scraping data
    for forecast_num in range(1,5):
        df = pd.read_csv(f'http://ets.aeso.ca/Market/Reports/Manual/supply_and_demand/csvData/{forecast_num}-6month.csv', on_bad_lines='skip')
        df = df[~df['Report Date'].str.contains('Disclaimer')]
        forecast = pd.concat([forecast,df],axis=0)
# Creating offset_df
    offset_df = forecast[['Report Date','AIL Load + Operating Reserves (MW)']]
    offset_df['Offset'] = 0
    offset_df.rename(columns={'Report Date':'Date','AIL Load + Operating Reserves (MW)':'Demand'},inplace=True)
    offset_df['Year'] = pd.DatetimeIndex(offset_df['Date']).year
    offset_df['Month'] = pd.DatetimeIndex(offset_df['Date']).month
# Grid options for AgGrid table
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
# Creating cols for Streamlit app
    col1, col2 = st.columns([1,2])
# Adding AgGrid table to Col1
    with col1:
        offset_df = AgGrid(offset_df[['Date','Demand','Offset']], grid_options, fit_columns_on_grid_load=True)['data']
# Calculating cummulative offset
    offset_df = pd.DataFrame(offset_df)
    offset_df['tot_offset'] = offset_df['Offset'].cumsum()
    offset_df['Adjusted Demand'] = offset_df['Demand'] + offset_df['tot_offset']
# Creating Altair charts
    # Demand line chart
    line = alt.Chart(offset_df).mark_line(color='black').encode(
        x=alt.X('Date:T',axis=alt.Axis(format='%b %Y'),title=''),
        y=alt.Y('Demand:Q', title='Demand (MW)'),
    ).properties(
        height=430,
    )
    # Adjusted Demand area chart
    area = alt.Chart(offset_df).mark_area(color='green', opacity=0.3).encode(
        x='Date:T',
        y='Adjusted Demand:Q',
    ).interactive(bind_y=True)
    # Selector for sliding vertical line 
    nearest = alt.selection(type='single', nearest=True, on='mouseover', fields=['Date'], empty='none', clear="mouseout")
    # Creating vertical line to highlight current Date
    rule = alt.Chart(offset_df).mark_rule(color='gray').encode(
        x='Date:T',
        opacity=alt.condition(nearest, alt.value(1), alt.value(0)),
        tooltip = ['Date:T','Demand','Adjusted Demand']
    ).add_selection(nearest)
    # Adding layered chart to Col2
    with col2:
        st.altair_chart(line + area + rule, use_container_width=True)