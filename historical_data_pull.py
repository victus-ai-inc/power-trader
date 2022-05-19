from datetime import datetime, timedelta
import time
import http.client
import certifi
import ssl
import json
import pull_nrg_data

if __name__ == '__main__':
#AB Internal Load Demand (1min) = 139308
    # fromDate = st.sidebar.date_input('Start Date', value=datetime.now()-timedelta(1))
    # toDate = st.sidebar.date_input('End Date', min_value=fromDate)+timedelta(1)
    # fromDate = fromDate.strftime('%m/%d/%Y')
    # toDate = toDate.strftime('%m/%d/%Y')
    
    streamId = [139308]
    fromDate = fromDate.strftime('%m/%d/%Y')
    toDate = toDate.strftime('%m/%d/%Y')
    # Pull NRG data
    df = pull_nrg_data.pull_data(fromDate, toDate, streamId)
    #meta = pd.json_normalize(df, record_path=['columns'])
    #st.write(meta)
    df = pd.json_normalize(df, record_path=['data'])
    st.write(df)