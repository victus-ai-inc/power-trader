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
        st.write(f'{datetime.now()} Outputing stream {path} res code {res_code}')
        jsonData = json.loads(res.read().decode('utf-8'))
        jsonData['streamInfo'] = {'name':'ryan'}
        st.write(jsonData)
        df = pd.json_normalize(jsonData, record_path=['data'])
        st.write(type(df))
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

# accessToken = 'E7NXySe54ILsUPGfbF7S2v8O8_WFSBgiF8siI-WyRCNB5Rj2FmbesMCTRyajbtRkNAm3CCwNEbg71DFRN_V8RkI2FwwAoPJzuFjmMUQjEzEs4j8F_Wsa6SnZlcAlN9XS6VmqJvrZw0bdjoPGqCgsOVJJERF3VD7I6xYgSlUrtkot2vVy7URYwAy3PGZTL9Gi2lr1SE6xRrpdJWhqtgWmClPW5RbIpSyVzw2iNXBz_y1qenfW6oZ4HCUd-_PH39xH4jQ9eYVEem5aGWiTSzC2tWnbDpkiS8Po9aKDuS4DjJEiiCBnPmZAr3f17ZmELAeD-KnGaQKA1KpwBu8OFyuCLG-Bhjw'
# release_token(accessToken)