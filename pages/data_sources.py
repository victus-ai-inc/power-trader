import streamlit as st
import pandas as pd
import ssl
import json
import http.client
import certifi
import time
import pull_nrg_data
import Alberta_Power
from datetime import datetime
from pandasql import sqldf

# @st.experimental_memo
# def get_streamInfo(streamId):
#     streamInfo = pd.read_csv('stream_codes.csv')
#     streamInfo = streamInfo[streamInfo['streamId']==streamId]
#     return streamInfo

# #@st.experimental_singleton
# def http_connect():
#     server = 'api.nrgstream.com'
#     context = ssl.create_default_context(cafile=certifi.where())
#     conn = http.client.HTTPSConnection(server, context=context)
#     return conn

# def pull_data(fromDate, toDate, streamId, accessToken, tokenExpiry):
#     # Setup the path for data request
#     conn = http_connect()
#     path = f'/api/StreamData/{streamId}?fromDate={fromDate}&toDate={toDate}'
#     headers = {'Accept': 'Application/json', 'Authorization': f'Bearer {accessToken}'}
#     conn.request('GET', path, None, headers)
#     res = conn.getresponse()
#     if res.code != 200:
#         st.write(f'trying, code = {res.code}')
#         res.read()
#         conn.close()
#         time.sleep(5)
#         pull_data(fromDate, toDate, streamId, accessToken, tokenExpiry)
#     # Load json data & create pandas df
#     else:
#         jsonData = json.loads(res.read().decode('utf-8'))
#         df = pd.json_normalize(jsonData, record_path='data')
#         # Rename df cols
#         df.rename(columns={0:'timeStamp', 1:'value'}, inplace=True)
#         # Add streamInfo cols to df
#         streamInfo = get_streamInfo(streamId)
#         assetCode = streamInfo.iloc[0,1]
#         streamName = streamInfo.iloc[0,2]
#         fuelType = streamInfo.iloc[0,3]
#         subfuelType = streamInfo.iloc[0,4]
#         timeInterval = streamInfo.iloc[0,5]
#         intervalType = streamInfo.iloc[0,6]
#         df = df.assign(streamId=streamId, assetCode=assetCode, streamName=streamName, fuelType=fuelType, \
#                         subfuelType=subfuelType, timeInterval=timeInterval, intervalType=intervalType)
#         # Changing 'value' col to numeric and filling in NA's with previous value in col
#         df.replace(to_replace={'value':''}, value=0, inplace=True)
#         df['value'] = pd.to_numeric(df['value'])
#         df.fillna(method='ffill', inplace=True)
#         conn.close()
#     return df

# # Pull current day data from NRG
# @st.experimental_memo(suppress_st_warning=True)
# def current_data(tokenExpiry):
#     streamIds = [86, 322684, 322677, 87, 85, 23695, 322665, 23694]
#     current_df = pd.DataFrame([])
#     today = datetime.now()
#     accessToken, tokenExpiry = st.session_state['accessToken'], st.session_state['tokenExpiry']
#     for id in streamIds:
#         APIdata = pull_data(today.strftime('%m/%d/%Y'), today.strftime('%m/%d/%Y'), id, accessToken, tokenExpiry)
#         APIdata['timeStamp'] = pd.to_datetime(APIdata['timeStamp'])
#         current_df = pd.concat([current_df, APIdata], axis=0)
#     current_query = '''
#         SELECT
#             strftime('%Y-%m-%d %H:00:00', timeStamp) AS timeStamp,
#             fuelType,
#             strftime('%Y', timeStamp) AS year,
#             strftime('%m', timeStamp) AS month,
#             strftime('%d', timeStamp) AS day,
#             strftime('%H', timeStamp) AS hour,
#             AVG(value) AS value
#         FROM current_df
#         GROUP BY fuelType, year, month, day, hour
#         ORDER BY fuelType, year, month, day, hour, timeStamp
#         '''
#     current_df = sqldf(current_query, locals())
#     return current_df.astype({'fuelType':'object', 'year':'int64','month':'int64', 'day':'int64', 'hour':'int64', 'value':'float64', 'timeStamp':'datetime64[ns]'}), today

# test_df, today = current_data(st.session_state['acccessToken'])
# st.dataframe(test_df)

# data_sources = {'test_df':test_df}

# for data in data_sources.keys():
#     data
#     # if data not in st.session_state:
#     #     st.session_state[data] = test_df
#     # else:
#     #     test_df = st.session_state[data]

st.session_state['accessToken']
st.session_state['tokenExpiry']