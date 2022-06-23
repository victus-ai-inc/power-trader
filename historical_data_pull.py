from datetime import datetime, timedelta, date
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

if __name__ == '__main__':
    tic = time.perf_counter()
    
    streamId = 41371
    year = 2020
    # Get NRG API token
    accessToken, tokenExpiry = pull_nrg_data.getToken()
    print(accessToken)
    # Cycle through months and write to DB
    for month in range(1,2):
        startDate = date(year,month,1)
        if month < 12:
            endDate = date(year,month+1,1)
        else:
            endDate = date(year+1,1,1)
        APIdata = pull_nrg_data.pull_data(startDate.strftime('%m/%d/%Y'), endDate.strftime('%m/%d/%Y'), streamId, accessToken, tokenExpiry)
        APIdata['timeStamp'] = pd.to_datetime(APIdata['timeStamp'])
        # Write data to DB
        sql_insert(APIdata)

    APIdata.to_csv('test.csv', index=False)

    # Release token
    pull_nrg_data.release_token(accessToken)
    
    toc = time.perf_counter()
    print(f'{toc - tic:0.2f} secs')