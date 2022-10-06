# historical data
    # every day pull yesterday data from NRG and push to historical BQ table
    # return history_df
# current data
    # pull current day data from NRG
    # return current_df
# daily outages
    # A = pull outages that are currently stored in BQ
    # merge changes to BQ every 5 min
    # B = pull new outages that are currently stored in BQ
    # diff_df = check if there is diff between A & B
        # create BQ table for alerts
        # render differences charts picture
        # send differences charts picture
    # every day remove outages in BQ older than today
    # return B, diff_df
# monthly outages
