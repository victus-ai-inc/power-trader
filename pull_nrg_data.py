from datetime import datetime, timedelta
import http.client
import certifi
import ssl
import json
import pandas as pd
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
    return accessToken, tokenExpiry

#accessToken = ''

# Release generated token
def release_token(accessToken):
    path = '/api/ReleaseToken'
    headers = {'Authorization': f'Bearer {accessToken}'}
    context = ssl.create_default_context(cafile=certifi.where())
    conn = http.client.HTTPSConnection(server,context=context)
    conn.request('DELETE', path, None, headers)
    res = conn.getresponse()
    st.write('Token successfully released.')

# Use token to pull data
def pull_data(fromDate, toDate):
    # Get token
    accessToken, tokenExpiry = getToken()
    st.write(accessToken)
    st.write(tokenExpiry)
    st.write(fromDate, toDate)
    # Pull data for stream
        #add code for this tomorrow
    # Release token
    release_token(accessToken)
    #pass