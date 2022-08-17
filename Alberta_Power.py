import streamlit as st
import pandas as pd
import altair as alt
import ssl
import json
import http.client
import certifi
import time
import alerts
from st_aggrid import AgGrid
from datetime import datetime, date, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
from pandasql import sqldf

@st.experimental_memo(suppress_st_warning=True)
def getToken():
    username = st.secrets["nrg_username"]
    password = st.secrets["nrg_password"]
    server = 'api.nrgstream.com'
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
        tokenExpiry = datetime.now() + timedelta(seconds=5)
        #tokenExpiry = datetime.now() + timedelta(seconds=jsonData['expires_in'])
    elif res_code == 400:
        res.read()
        release_token(accessToken)
        getToken()
    else:
        res_data = res.read()
    conn.close()
    return accessToken, tokenExpiry

def release_token(accessToken):
    path = '/api/ReleaseToken'
    server = 'api.nrgstream.com'
    headers = {'Authorization': f'Bearer {accessToken}'}
    context = ssl.create_default_context(cafile=certifi.where())
    conn = http.client.HTTPSConnection(server,context=context)
    conn.request('DELETE', path, None, headers)
    res = conn.getresponse()

accessToken, tokenExpiry = getToken()

if tokenExpiry <= datetime.now():
    st.write('clear')
    getToken.clear()
    release_token(accessToken)
    accessToken, tokenExpiry = getToken()

accessToken
tokenExpiry
