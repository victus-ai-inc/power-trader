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
    res_code
    # Check if the response is good
    st.write(res.code)
    if res_code == 200:
        res_data = res.read()
        # Decode the token into an object
        jsonData = json.loads(res_data.decode('utf-8'))
        accessToken = jsonData['access_token']
        # Calculate new expiry date
        tokenExpiry = datetime.now() + timedelta(seconds=20)
        #tokenExpiry = datetime.now() + timedelta(seconds=jsonData['expires_in'])
        # Comment to have same accessToken across all apps, uncomment for individual ACs
        st.session_state['accessToken'] = accessToken
        st.session_state['tokenExpiry'] = tokenExpiry
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

if 'accessToken' not in st.session_state:
    st.write('no session state')
    accessToken, tokenExpiry = getToken()
    st.session_state['accessToken'] = accessToken
    st.session_state['tokenExpiry'] = tokenExpiry
elif st.session_state['tokenExpiry'] <= datetime.now():
    release_token(st.session_state['accessToken'])
    getToken.clear()
    st.write('released')
    accessToken, tokenExpiry = getToken()
    st.session_state['accessToken'] = accessToken
    st.session_state['tokenExpiry'] = tokenExpiry
else:
    st.write('else')
    accessToken = st.session_state['accessToken']
    tokenExpiry = st.session_state['tokenExpiry']

accessToken
tokenExpiry
#release_token(accessToken)
#st.write('released @ end')