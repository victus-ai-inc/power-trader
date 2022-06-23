from datetime import datetime, date
import time
import pandas as pd
import pull_nrg_data
import streamlit as st
import mysql.connector
from sqlalchemy import create_engine

sql_user = st.secrets["sql_user"]
sql_pw = st.secrets["sql_password"]
sql_schema = st.secrets["sql_schema"]

def sql_insert(df):
    engine = create_engine(f'mysql+mysqlconnector://{sql_user}:{sql_pw}@localhost/{sql_schema}', echo=False)
    df.to_sql(name=sql_schema, con=engine, if_exists = 'append', index=False)

def get_streams():
    streams = pd.read_csv('stream_codes.csv')
    lst = [int(id) for id in streams[streams['timeInterval']=='5 min']['streamId'] if id > 330000]
    print(f'Streams = {lst}')
    return lst


if __name__ == '__main__':
    # Start timer
    tic = time.perf_counter()
    streamIds = [336893]
    year = [2021]
    # streamIds = get_streams()
    # year = [2020, 2021]
    # Get NRG API token
    accessToken, tokenExpiry = pull_nrg_data.getToken()
    print(accessToken)
    # Cycle through months and write to DB
    for id in streamIds:
        for yr in year:
            for month in range(1,13):
                print('pull something...')
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
                    sql_insert(APIdata)
                except:
                    print('Empty month # {month} for Stream # {id} for year {yr}')
                    pass
        print(f'Stream # {id} for year {yr} loaded')

    # Release token
    pull_nrg_data.release_token(accessToken)
    
    # End timer
    toc = time.perf_counter()
    print(f'{toc - tic:0.2f} secs')