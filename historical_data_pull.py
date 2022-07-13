from datetime import date
import pandas as pd
import pull_nrg_data
import os
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Path to Google auth credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/ryan-bulger/power-trader/google-big-query.json'

def get_streams():
    streams = pd.read_csv('stream_codes.csv')
    # Pull only 5-min supply streams
    lst = [int(id) for id in streams[(streams['timeInterval']=='5 min') & (streams['intervalType']=='supply')]['streamId']]
    print(f'Streams = {lst}')
    return lst

if __name__ == '__main__':
    # Select streams & years to iterate over
    streamIds = get_streams()
    #streamIds = [129796, 45132]
    year = [2020, 2021]
    # Cycle through months and write to DB
    for id in streamIds:
        for yr in year:
            accessToken, tokenExpiry = pull_nrg_data.getToken()
            startDate = date(yr,1,1)
            endDate = date(yr+1,1,1)
            try:
                APIdata = pull_nrg_data.pull_data(startDate.strftime('%m/%d/%Y'), endDate.strftime('%m/%d/%Y'), id, accessToken, tokenExpiry)
                APIdata['timeStamp'] = pd.to_datetime(APIdata['timeStamp'])
                # Write data to DB
                #bigquery.Client().load_table_from_dataframe(APIdata, 'nrgdata.nrgdata')
                print(f'Stream #{id} successfully loaded for {yr}')
                pull_nrg_data.release_token(accessToken)
            except:
                print(f'Stream #{id} is empty for {yr}')
                pull_nrg_data.release_token(accessToken)
                pass