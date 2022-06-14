from datetime import datetime, timedelta, date
import time
import pandas as pd
import pull_nrg_data

if __name__ == '__main__':
    tic = time.perf_counter()
    streamId = 129796
    year = 2021
    # Get NRG API token
    accessToken, tokenExpiry = pull_nrg_data.getToken()
    print(accessToken)
    for month in range(1,13):
        startDate = date(year,month,1)
        if month < 12:
            endDate = date(year,month+1,1)
        else:
            endDate = date(year,12,31)
        APIdata = pull_nrg_data.pull_data(startDate.strftime('%m/%d/%Y'), endDate.strftime('%m/%d/%Y'), streamId, accessToken, tokenExpiry)
        print(startDate,endDate)
        print(APIdata)
    pull_nrg_data.release_token(accessToken)
    toc = time.perf_counter()
    print(f'{toc - tic:0.2f} secs')