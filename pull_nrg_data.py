from datetime import datetime, timedelta
import time
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
    res_code = res.code
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
    conn = http.client.HTTPSConnection(server,context=context)
    conn.request('GET', path, None, headers)
    res = conn.getresponse()
    res_code = res.code
    try:
        st.write(f'{datetime.now()} Outputing stream {path} res code {res_code}')
        df = json.loads(res.read().decode('utf-8'))
        df = pd.json_normalize(df, record_path=['data'])
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

accessToken = '-OGrnpgKog83XaKRqwDrgGdVPkIPU0g9ZAsYTJ2PIdyZhL_HqsDFRVh-ryPicw-H7E5pyPdIF9WG4uxrWGxvODwVyXhLAZh-F2ECugDNDgKVccIym5aV5RGhQanJz0k7HcZ6RI4b6jG-e1n2PpojM3PypVfGDybNoX_l8S4faJN5qWQA2Zw9CmHtOVv0L5BZvOCraadIOY5st06sRvI8yO8LJOxDx-Vo7MIWhJEnUPfBpKWXjR4IvTttLK-HtbHQq4EQHqbgUIsfe3ZsKhynSg-4xgxkDbF9wkNoP7b02_zJD_6qsvLq-AbFUDw3ti9wCOtCJydGtG7VFhCMYyvgoDACUTc'
release_token(accessToken)