from datetime import datetime, date, timedelta
import pandas as pd
import pull_nrg_data
import pickle
import pytz
tz = pytz.timezone('America/Edmonton')

if __name__ == '__main__':
# Pull stream data
    streams = pd.read_csv('stream_codes.csv')
    streamIds = [122]
    #streamIds = [86, 322684, 322677, 87, 85, 23695, 322665, 23694, 120, 124947, 122, 1]
    stream_count = len(streamIds)
    count = 1
    for id in streamIds:
        startDate = date(2022,10,13)
        #startDate = datetime.now(tz).date()-timedelta(days=1)
        endDate = date(2022,10,14)
        #endDate = datetime.now(tz).date()+timedelta(days=1)
        print(startDate, endDate)
        accessToken, tokenExpiry = pull_nrg_data.getToken()
        APIdata = pull_nrg_data.pull_data(startDate.strftime('%m/%d/%Y'), endDate.strftime('%m/%d/%Y'), id, accessToken, tokenExpiry)
        pull_nrg_data.release_token(accessToken)
        APIdata['timeStamp'] = pd.to_datetime(APIdata['timeStamp'])
        APIdata['timeStamp'] = APIdata['timeStamp'].dt.tz_localize('America/Edmonton', ambiguous=True, nonexistent='shift_forward')
        #bigquery.Client(credentials=credentials).load_table_from_dataframe(APIdata, 'nrgdata.historical_data')
        print(APIdata)
        print(f'STREAM #{id} finished, {stream_count-count} streams remaining')
        count += 1