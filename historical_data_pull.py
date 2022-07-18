from datetime import date
import pandas as pd
import pull_nrg_data
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound

# Google BigQuery auth
credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])

def get_streams():
    streams = pd.read_csv('stream_codes.csv')
    # Pull only 5-min supply streams
    lst = [int(id) for id in streams[(streams['timeInterval']=='5 min') & (streams['intervalType']=='supply')]['streamId']]
    print(f'Streams = {lst}')
    return lst

if __name__ == '__main__':
    streamIds = get_streams()
    year = [2022]
    stream_count = len(streamIds)
    count = 0
    for id in streamIds:
        for yr in year:
            startDate = date(yr,1,1)
            endDate = date(yr,7,1)
            accessToken, tokenExpiry = pull_nrg_data.getToken()
            try:
                APIdata = pull_nrg_data.pull_data(startDate.strftime('%m/%d/%Y'), endDate.strftime('%m/%d/%Y'), id, accessToken, tokenExpiry)
                pull_nrg_data.release_token(accessToken)
                APIdata['timeStamp'] = pd.to_datetime(APIdata['timeStamp'])
                bigquery.Client(credentials=credentials).load_table_from_dataframe(APIdata, 'nrgdata.nrgdata')
            except:
                pull_nrg_data.release_token(accessToken)
                pass
        print(f'STREAM #{id} finished, {stream_count-count} streams remaining')
        count += 1