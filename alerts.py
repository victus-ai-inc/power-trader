import streamlit as st
import smtplib
from twilio.rest import Client
import smtplib 
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

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
    email = "Your Email"
    pas = "Your Pass"

    sms_gateway = 'number@tmomail.net'
    # The server we use to send emails in our case it will be gmail but every email provider has a different smtp 
    # and port is also provided by the email provider.
    smtp = "smtp.gmail.com" 
    port = 587
    # This will start our email server
    server = smtplib.SMTP(smtp,port)
    # Starting the server
    server.starttls()
    # Now we need to login
    server.login(email,pas)

    # Now we use the MIME module to structure our message.
    msg = MIMEMultipart()
    msg['From'] = email
    msg['To'] = sms_gateway
    # Make sure you add a new line in the subject
    msg['Subject'] = "You can insert anything\n"
    # Make sure you also add new lines to your body
    body = "You can insert message here\n"
    # and then attach that body furthermore you can also send html content.
    msg.attach(MIMEText(body, 'plain'))

    sms = msg.as_string()

    server.sendmail(email,sms_gateway,sms)

# email = st.secrets['gmail_address']
# pas = st.secrets['gmail_password']

if __name__ == '__main__':
    pass