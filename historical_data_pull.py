from datetime import datetime, timedelta, date
import time
import pandas as pd
import pull_nrg_data
import mysql.connector
from mysql.connector import errorcode

def sql_insert():
    nrgdata = mysql.connector.connect(user='victusai', password='#p!k4XG2Mg#,B,W', host='localhost', database='nrgdata')
    nrgdata.close()

if __name__ == '__main__':
    tic = time.perf_counter()
    
    streamId = 41371
    year = 2021
    # Get NRG API token
    accessToken, tokenExpiry = pull_nrg_data.getToken()
    print(accessToken)
    # Cycle through months and write to DB
    for month in range(1,2):
        startDate = date(year,month,1)
        if month < 12:
            endDate = date(year,month+1,1)
        else:
            endDate = date(year+1,1,1)
        APIdata = pull_nrg_data.pull_data(startDate.strftime('%m/%d/%Y'), endDate.strftime('%m/%d/%Y'), streamId, accessToken, tokenExpiry)
    APIdata['timeStamp'] = pd.to_datetime(APIdata['timeStamp'])

    # Write data to DB
    APIdata.to_csv('test.csv', index=False)

    # Release token
    pull_nrg_data.release_token(accessToken)
    
    toc = time.perf_counter()
    print(f'{toc - tic:0.2f} secs')