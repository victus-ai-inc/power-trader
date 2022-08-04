import streamlit as st
import os
from twilio.rest import Client

def whatsapp():
    # Your Account SID from twilio.com/console
    account_sid = "AC480f9d79462ace32036b1915d38601d6"
    # Your Auth Token from twilio.com/console
    auth_token  = "ef0f023c38dc9ee13e523c6bce388caa"

    client = Client(account_sid, auth_token)

    message = client.messages.create(
        to="+14035129991", 
        from_="+18573845191",
        body="testing")
    print(message.sid)

# email = st.secrets['gmail_address']
# pas = st.secrets['gmail_password']

if __name__ == '__main__':
    pass