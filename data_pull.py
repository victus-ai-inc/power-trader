from datetime import date
import pandas as pd
import pull_nrg_data
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound

if __name__ == '__main__':
# Google BigQuery auth
    credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
# Pull stream data
    streams = pd.read_csv('stream_codes.csv')
    streamIds = [86, 322684, 322677, 87, 85, 23695, 322665, 23694, 120, 124947, 122]
    #streamIds = [86, 322684, 322677, 87, 85, 23695, 322665, 23694]
    year = [2022]
    stream_count = len(streamIds)
    count = 1
    for id in streamIds:
        for yr in year:
            startDate = date(yr,9,12)
            endDate = date(yr,9,13)
            accessToken, tokenExpiry = pull_nrg_data.getToken()
            APIdata = pull_nrg_data.pull_data(startDate.strftime('%m/%d/%Y'), endDate.strftime('%m/%d/%Y'), id, accessToken, tokenExpiry)
            pull_nrg_data.release_token(accessToken)
            APIdata['timeStamp'] = pd.to_datetime(APIdata['timeStamp'])
            bigquery.Client(credentials=credentials).load_table_from_dataframe(APIdata, 'nrgdata.hourly_data')
        print(f'STREAM #{id} finished, {stream_count-count} streams remaining')
        count += 1