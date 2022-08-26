import streamlit as st
import smtplib
import json
from twilio.rest import Client
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

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

def sms():
    email = st.secrets['email_address']
    pas = st.secrets['email_password']
    sms_gateways = st.secrets['phone_numbers'].values()
    smtp = 'smtp.gmail.com'
    port = 587
    server = smtplib.SMTP(smtp, port)
    server.starttls()
    server.login(email, pas)
    msg = MIMEMultipart()
    for gw in sms_gateways:
        msg['To'] = gw
        body = f'NEW ALERT:\n{datetime.now().strftime("%a %b %d, %Y @ %-I:%M%p")}\nhttps://bit.ly/3bRcJE3'
        msg.attach(MIMEText(body, 'plain'))
        sms = msg.as_string()
        server.sendmail(email, gw, sms)

def sms2():
    email = st.secrets['email_address']
    pas = st.secrets['email_password']
    sms_gateways = st.secrets['phone_numbers'].values()
    smtp = 'smtp.gmail.com'
    port = 587
    server = smtplib.SMTP(smtp, port)
    server.starttls()
    server.login(email, pas)
    msg = MIMEMultipart()
    for gw in sms_gateways:
        msg['To'] = gw
        body = f'History Updated:\n{datetime.now().strftime("%a %b %d, %Y @ %-I:%M%p")}'
        msg.attach(MIMEText(body, 'plain'))
        sms = msg.as_string()
        server.sendmail(email, gw, sms)