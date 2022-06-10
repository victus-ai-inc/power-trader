from datetime import datetime, timedelta, date
import time
import pandas as pd
import pull_nrg_data

def one_year_of_data(year, streamId, accessToken, tokenExpiry):
    firstDay = date(year,1,1)
    lastDay = firstDay + timedelta(1)
    # Initialize start date & dataframe
    startDate = firstDay
    df = pd.DataFrame()
    # Create new csv file to overwrite old file
    df.to_csv('test.csv')
    # Loop from firstDay to lastDay
    while startDate < lastDay:
        endDate = startDate + timedelta(1)
        # Get new token if it has expiried
        if datetime.now() >= tokenExpiry:
            accessToken, tokenExpiry = pull_nrg_data.getToken()
        # Pull NRG data
        APIdata = pull_nrg_data.pull_data(startDate.strftime('%m/%d/%Y'), endDate.strftime('%m/%d/%Y'), streamId, accessToken, tokenExpiry)
        df = pd.concat([df,APIdata], axis=0, ignore_index=True)
        startDate = startDate + timedelta(1)
    return df

if __name__ == '__main__':
    tic = time.perf_counter()
    streamId = 129796
    year = 2022
    # Get NRG API token
    accessToken, tokenExpiry = pull_nrg_data.getToken()
    print(accessToken)
    df = one_year_of_data(year, streamId, accessToken, tokenExpiry)
    df.to_csv('test.csv', header=['timestamp','Demand'])
    pull_nrg_data.release_token(accessToken)
    toc = time.perf_counter()
    print(f'{toc - tic:0.2f} secs')