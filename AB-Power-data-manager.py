# PULL DATA
    # token
    # pull data

# HISTORICAL
    # @st.experimental_memo()
    # def update_historical_data():
        # Check when last upated
            # last_history_update = 'SELECT MAX(timeStamp) FROM nrgdata.hourly_data'
        # if last_history_update older than yesterday => update from NRG and push to historical BQ table

# OUTAGES
    # ****add the date when the outages were pulled to outage database****

    # def update_daily_outages():
        # daily_outage_old_df = pull outages that are currently stored in BQ
        # daily_outage_new_df = pull current outage data from NRG
        # Update and merge new_daily_outage_df to BQ every 5 min
        # return daily_outage_old_df, daily_outage_new_df
    
    # def daily_outage_diffs(daily_outage_old_df, daily_outage_new_df):
        # daily_outage_diff_df = check if there is a diff between daily_outage_old_df & daily_outage_new_df
        # Send alert charts for each stream that has a diff
            # render alert chart (as diff charts pic: https://altair-viz.github.io/user_guide/saving_charts.html)
            # send charts to users
        # Remove outages in BQ older than a week ago

# CURRENT
    # Refresh every 10 sec:
    # def update_current_data():
        # pull current_df from NRG
        # put current_df into session_state
            # st.session_state['current_df'] = current_df
        # clear read_current_data() memoization to refresh new current_df and make available to the apps
            # read_current_data.clear()

    # @st.experimental_memo()
    # def read_current_data():
        # Apps will always read from memo, and memo is only updated when new data is pulled 
        # read current_df from session_state
            # current_df = st.session_state['current_df']
        # return current_df

# MAIN APP CODE
# **** make text messages of when each element was last run ****
