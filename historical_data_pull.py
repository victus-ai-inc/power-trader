from datetime import datetime, date
import pandas as pd
import pull_nrg_data
import streamlit as st
import mysql.connector
from sqlalchemy import create_engine
import os
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Path to Google auth credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/ryan-bulger/power-trader/google-big-query.json'
# SQL auth
sql_user = st.secrets["sql_user"]
sql_pw = st.secrets["sql_password"]
sql_schema = st.secrets["sql_schema"]

def sql_insert(df):
    engine = create_engine(f'mysql+mysqlconnector://{sql_user}:{sql_pw}@localhost/{sql_schema}', echo=False)
    df.to_sql(name=sql_schema, con=engine, if_exists = 'append', index=False)

def get_streams():
    streams = pd.read_csv('stream_codes.csv')
    # Pull only 5-min supply streams
    lst = [int(id) for id in streams[(streams['timeInterval']=='5 min') & (streams['intervalType']=='supply')]['streamId']]
    print(f'Streams = {lst}')
    return lst

if __name__ == '__main__':
    # Select streams & years to iterate over
    streamIds = get_streams()
    #streamIds = [129796, 45132]
    year = [2020, 2021]
    # Get NRG API token
    accessToken, tokenExpiry = pull_nrg_data.getToken()
    print(accessToken)
    # Cycle through months and write to DB
    for id in streamIds:
        print('--------------')
        print(f'STREAM #{id}')
        print('--------------')
        for yr in year:
            for month in range(1,13):
                if datetime.now() >= tokenExpiry:
                    accessToken, tokenExpiry = pull_nrg_data.getToken()
                    print(f'New token expires {tokenExpiry}:')
                    print(accessToken)
                startDate = date(yr,month,1)
                if month < 12:
                    endDate = date(yr,month+1,1)
                else:
                    endDate = date(yr+1,1,1)
                try:
                    APIdata = pull_nrg_data.pull_data(startDate.strftime('%m/%d/%Y'), endDate.strftime('%m/%d/%Y'), id, accessToken, tokenExpiry)
                    APIdata['timeStamp'] = pd.to_datetime(APIdata['timeStamp'])
                    # Write data to DB
                    print(APIdata)
                    bigquery.Client().load_table_from_dataframe(APIdata, 'nrgdata.nrgdata')
                    print(f'Stream #{id} successfully loaded for {month}-{yr}')
                except:
                    print(f'Stream #{id} is empty for {month}-{yr}')
                    pass
    # Release token
    pull_nrg_data.release_token(accessToken)