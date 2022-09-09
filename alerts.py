import streamlit as st
import smtplib
import pytz
#from twilio.rest import Client
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

def sms(i):
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
        body = f'\nALERT: {i}\n{datetime.now(pytz.timezone("America/Edmonton")).strftime("%a %b %d, %Y @ %-I:%M%p")}\nhttps://bit.ly/3bRcJE3'
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