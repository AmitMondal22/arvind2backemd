
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


    
def send_email13(receiver_email, subject, body):
    smtp_server = 'smtp.gmail.com'
    smtp_port = 587  # Gmail's TLS port
    
    smtp_username = 'wrongcode.in@gmail.com'  # Your Gmail address
    smtp_password = 'mgxucicdmaonhunx'  # Your Gmail password or App Password if 2-factor authentication is enabled
    
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = smtp_username
    msg['To'] = receiver_email


    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()  # Upgrade the connection to a secure TLS connection
        server.login(smtp_username, smtp_password)
        server.send_message(msg)





def send_email(receiver_email, subject, html_file_path, dynamic_data=None):
    smtp_server = 'smtp.gmail.com'
    smtp_port = 587  # Gmail's TLS port
    
    smtp_username = 'wrongcode.in@gmail.com'  # Your Gmail address
    smtp_password = 'mgxucicdmaonhunx'  # Your Gmail password or App Password if 2-factor authentication is enabled
    
    # Read the HTML template file
    with open(html_file_path, 'r') as file:
        html_content = file.read()
    
    if dynamic_data:
        # Replace placeholders in the HTML content with dynamic data
        for key, value in dynamic_data.items():
            html_content = html_content.replace('{{' + key + '}}', value)
    
    # Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = smtp_username
    msg['To'] = receiver_email

    # Attach HTML body to the email
    msg.attach(MIMEText(html_content, 'html'))

    # Connect to SMTP server and send email
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()  # Upgrade the connection to a secure TLS connection
        server.login(smtp_username, smtp_password)
        server.sendmail(smtp_username, receiver_email, msg.as_string())

    