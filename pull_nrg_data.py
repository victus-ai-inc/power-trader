from datetime import datetime, timedelta
import pandas as pd
import http.client
import certifi
import ssl
import json
import ab_power_trader
import streamlit as st

# Pull NRG credentials from Streamlit secrets manager
username, password = ab_power_trader.get_nrg_creds()
server = 'api.nrgstream.com'

# Use creds to generate token from NRG
def getToken():
    tokenPath = '/api/security/token'
    tokenPayload = f'grant_type=password&username={username}&password={password}'
    headers = {"Content-type": "application/x-www-form-urlencoded"}
    # Connect to API server to get a token
    context = ssl.create_default_context(cafile=certifi.where())
    conn = http.client.HTTPSConnection(server,context=context)
    conn.request('POST', tokenPath, tokenPayload, headers)
    res = conn.getresponse()
    res_code = res.status
    # Check if the response is good
    if res_code == 200:
        res_data = res.read()
        # Decode the token into an object
        jsonData = json.loads(res_data.decode('utf-8'))
        accessToken = jsonData['access_token']
        # Calculate new expiry date
        tokenExpiry = datetime.now() + timedelta(seconds=jsonData['expires_in'])
    else:
        st.write(res_code)
        res_data = res.read()
        st.write(res_data.decode('utf-8'))
    conn.close()
    return (accessToken, tokenExpiry)

def get_streamInfo(streamId):
    streamInfo = pd.read_csv('stream_codes.csv')
    streamInfo = streamInfo[streamInfo['streamId']==str(streamId)]
    return streamInfo

# Use token to pull data
def pull_data(fromDate, toDate, streamId, accessToken, tokenExpiry): 
    # Setup the path for data request
    path = f'/api/StreamData/{streamId}?fromDate={fromDate}&toDate={toDate}'
    headers = {
        'Accept': 'Application/json',
        'Authorization': f'Bearer {accessToken}'
        }
    context = ssl.create_default_context(cafile=certifi.where())
    conn = http.client.HTTPSConnection(server, context=context)
    conn.request('GET', path, None, headers)
    res = conn.getresponse()
    res_code = res.status
    try:
        #Load json data & create pandas df
        jsonData = json.loads(res.read().decode('utf-8'))
        df = pd.json_normalize(jsonData, record_path='data')
        #Rename df cols
        df.rename(columns={0:'timeStamp', 1:'value'}, inplace=True)
        #Add streamInfo cols to df
        streamInfo = get_streamInfo(streamId)
        assetCode = streamInfo.iloc[0,1]
        streamName = streamInfo.iloc[0,2]
        fuelType = streamInfo.iloc[0,3]
        subfuelType = streamInfo.iloc[0,4]
        timeInterval = streamInfo.iloc[0,5]
        intervalType = streamInfo.iloc[0,6]
        df = df.assign(streamId=streamId, assetCode=assetCode, streamName=streamName, fuelType=fuelType, \
                        subfuelType=subfuelType, timeInterval=timeInterval, intervalType=intervalType)
        #Changing 'value' col to numeric and filling in NA's with previous value in col
        df['value'] = pd.to_numeric(df['value'])
        df.fillna(method='ffill',inplace=True)
        st.write(df)
        conn.close()
    except:
        print('Exception Caught 1')
    return df

# Release generated token
def release_token(accessToken):
    path = '/api/ReleaseToken'
    headers = {'Authorization': f'Bearer {accessToken}'}
    context = ssl.create_default_context(cafile=certifi.where())
    conn = http.client.HTTPSConnection(server,context=context)
    conn.request('DELETE', path, None, headers)
    res = conn.getresponse()
    st.write('Token successfully released.')
    print('token released')

# accessToken = 'xaY2Q9m3TNvB_a4U8L8w_Clz5F2WLz3RXKlxOcMExvIY6ShVAx9yrdiNvS3HTC7TsX9HtRTasqVR9SM3YStMo_ysEjoEjjAoyEfbkB6Co6jPlesD-wC0lGRvGMUNSdUEXO7wRZw-cfb9xFRIR7y2bX-VorD-zv6eQ1bEkm1EzXYKeKiKSf-xjJ32H_wVi_oktebZbgkOjaCpEWUgL_xDYkUoAJQHjn4Q5T92BPllkzveZmzmvJhBjpjegoxP6ISdElrBRLbrUSZTlH43k7_CLUwbAd8ryjfLaLymF0CAkKr8pQTJIKPyvMrBZ7jYCgAKT39bY2Lv8EMlkA_blpCZ8LSg9XY'
# release_token(accessToken)