# Pull 24mth Supply Demand forecast (future_df)
from datetime import datetime, date, timedelta
import time
from unicodedata import numeric
import pandas as pd
import pull_nrg_data
from pandasql import sqldf
from datetime import datetime, timedelta
import pandas as pd
import http.client
import certifi
import ssl
import json
import ab_power_trader

username, password = ab_power_trader.get_nrg_creds()
server = 'api.nrgstream.com'

def get_streamInfo(streamId):
    streamInfo = pd.read_csv('stream_codes.csv')
    streamInfo = streamInfo[streamInfo['streamId']==streamId]
    return streamInfo

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
    # Load json data & create pandas df
    jsonData = json.loads(res.read().decode('utf-8'))
    df = pd.json_normalize(jsonData, record_path='data')
    if streamId == 278763:
        df.drop([1,3,4,5],axis=1,inplace=True)
        df.rename(columns={2:1}, inplace=True)
    # Rename df cols
    df.rename(columns={0:'timeStamp', 1:'value'}, inplace=True)
    # Add streamInfo cols to df
    streamInfo = get_streamInfo(streamId)
    assetCode = streamInfo.iloc[0,1]
    streamName = streamInfo.iloc[0,2]
    fuelType = streamInfo.iloc[0,3]
    subfuelType = streamInfo.iloc[0,4]
    timeInterval = streamInfo.iloc[0,5]
    intervalType = streamInfo.iloc[0,6]
    df = df.assign(streamId=streamId, assetCode=assetCode, streamName=streamName, fuelType=fuelType, \
                    subfuelType=subfuelType, timeInterval=timeInterval, intervalType=intervalType)
    # Changing 'value' col to numeric and filling in NA's with previous value in col
    df.replace(to_replace={'value':''},value=0,inplace=True)
    df['value'] = pd.to_numeric(df['value'])
    df.fillna(method='ffill',inplace=True)
    conn.close()
    return df

if __name__ == '__main__':
    # Select streams & years to iterate over
    streamIds = [278763]
    # Get NRG API token
    accessToken, tokenExpiry = pull_nrg_data.getToken()
    print(accessToken)
    # Cycle through months
    for id in streamIds:
        if datetime.now() >= tokenExpiry:
            accessToken, tokenExpiry = pull_nrg_data.getToken()
        # startDate = datetime.now()
        # endDate = datetime.now() + timedelta(days=2*365)
        startDate = date(2022,7,7)
        endDate = date(2024,6,29)
        print(startDate,endDate)
        APIdata = pull_data(startDate.strftime('%m/%d/%Y'), endDate.strftime('%m/%d/%Y'), id, accessToken, tokenExpiry)
        APIdata['timeStamp'] = pd.to_datetime(APIdata['timeStamp'])
        print(APIdata)
    # Release token
    pull_nrg_data.release_token(accessToken)