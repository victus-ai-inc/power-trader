from datetime import datetime, timedelta
import time
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
def pull_data(fromDate, toDate, streamIds):
    # Get token
    accessToken, tokenExpiry = getToken()
    #st.write(accessToken)
    #st.write(tokenExpiry)
    # Pull data for stream
    for streamId in streamIds:
        # Check if token has expired, if so, get new one
        if datetime.now() >= tokenExpiry:
            getToken()
        # Setup the path for data request. Using hard coded dates for example
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
            df = json.dumps(json.loads(res.read().decode('utf-8')), indent=2, sort_keys=False)
            st.write(df)
            conn.close()
            # Wait 1 second before next request
            time.sleep(1)
        except:
            print('Exception Caught 1')
            break

    # Release token
    release_token(accessToken)

# accessToken = 'BOdeE8KfcnoGpM-8iSlsNeWt9zL-P07DZUoLiCbjeV4tIQTWKmw82FFlgj8G_H69nH5XGQWbHMdaYeJS02tWFR3ZTUU9Gnyn60WiEfaxXLCdLk8h6nQJnzN1vF1FDhfvfrNi7az5HAbbos1MjAwbvFJIKAeSsXno9Fje5_Fa9sCnRJdkl0YHPOIislDqyZ9QI1-VFYdLi-WZoBrbYEq1Svddv-s3D69VMLVz9omH-7jMySDTlvQaN0Lxdx3SGiEVy3y7EyQqEkjyEi3b_JTmPobfK4QtbLFgpzi4zprnJUwJN3TZau_38vyfFwXkIKvd0AqcMbnOVhH9KPc141TCt5ybXFI'
# release_token(accessToken)