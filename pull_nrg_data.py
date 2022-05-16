import streamlit as st
import pandas as pd
import http.client
import certifi
import ssl
import ab-power-trader

#1. Pull NRG credentials from Streamlit secrets manager
print(ab-power-trader.get_nrg_creds())

#2. Use creds to generate token from NRG
def getToken():
    global accessToken
    global tokenExpiry

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
        res_data = res.read()
        print(res_data.decode('utf-8'))
    conn.close()    
    return

#3. Use token to pull data
#4. Pass data from pull-nrg-data.py to ab-power-trader.py


