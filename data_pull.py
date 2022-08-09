from datetime import date
import pandas as pd
import pull_nrg_data
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound

def resursion(i):
    try:
        accessToken, tokenExpiry = pull_nrg_data.getToken()
        APIdata = pull_nrg_data.pull_data(startDate.strftime('%m/%d/%Y'), endDate.strftime('%m/%d/%Y'), id, accessToken, tokenExpiry)
        pull_nrg_data.release_token(accessToken)
        APIdata['timeStamp'] = pd.to_datetime(APIdata['timeStamp'])
        print(APIdata)
        bigquery.Client(credentials=credentials).load_table_from_dataframe(APIdata, 'nrgdata.hourly_data')
    except:
        pull_nrg_data.release_token(accessToken)
        print(f'STREAM #{id} failed on iteration #{i}. Trying again')
        if i < 101:
            i += 1
            resursion(i)

if __name__ == '__main__':
# Google BigQuery auth
    credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
# Pull stream data
    streams = pd.read_csv('stream_codes.csv')
    streamIds = [int(id) for id in streams[(streams['timeInterval']=='1 hr') & (streams['intervalType']=='supply')]['streamId']]
    year = [2020, 2021]
    stream_count = len(streamIds)
    count = 1
    for id in streamIds:
        for yr in year:
            startDate = date(yr,1,1)
            endDate = date(yr+1,1,1)
            resursion(1)
        print(f'STREAM #{id} finished, {stream_count-count} streams remaining')
        count += 1