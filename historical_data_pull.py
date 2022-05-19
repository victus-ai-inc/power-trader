from datetime import datetime, timedelta, date
import pandas as pd
import pull_nrg_data

def one_year_of_data(year, streamId, accessToken, tokenExpiry):
    firstDay = date(year,1,1)
    lastDay = firstDay + timedelta(3)
    print(firstDay, lastDay)
    # Initialize start date & dataframe
    startDate = firstDay
    df = []
    # Loop from firstDay to lastDay
    while startDate < lastDay:
        endDate = startDate + timedelta(1)
        # Get new token if it has expiried
        if datetime.now() >= tokenExpiry:
            accessToken, tokenExpiry = pull_nrg_data.getToken()
        # Pull NRG data
        APIdata = pull_nrg_data.pull_data(startDate, endDate, streamId, accessToken, tokenExpiry)
        df = pd.concat([df,APIdata], axis=0)
        print('loop', df)
        startDate = startDate + timedelta(1)
    return df

if __name__ == '__main__':
    streamId = 225
    year = 2020
    # Get NRG API token
    accessToken, tokenExpiry = pull_nrg_data.getToken()
    print(accessToken)
    df = one_year_of_data(year, streamId, accessToken, tokenExpiry)
    print('final', df)
    df.to_csv('test.csv')
    pull_nrg_data.release_token()