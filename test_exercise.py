import sqlite3
import string
from datetime import datetime, timedelta
import random
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import schedule


EMAIL = "your_email@your_domain"
DATE_FORMAT = '%Y-%m-%d'

connection = sqlite3.connect('cars.db')
cursor = connection.cursor()


def create_table():
    query = """CREATE TABLE IF NOT EXISTS Cars(
      id INTEGER PRIMARY KEY,
      brand TEXT,
      model TEXT,
      email TEXT NOT NULL,
      oc_insurance_expiration TEXT NOT NULL,
      technical_inspection_expiration TEXT NOT NULL
      )"""

    cursor.execute(query)
    connection.commit()


def generate_random_future_date():
    current_date = datetime.now()
    random_days = timedelta(random.randint(32, 365))
    random_future_date = current_date + random_days
    return random_future_date


def generate_random_string(k):
    random_string_list = random.choices(string.ascii_letters, k=k)
    return ''.join(random_string_list)


def add_random_data_to_db(n, email):
    for _ in range(n):
        brand = generate_random_string(3).upper()
        model = generate_random_string(7).lower()
        random_oc_date = generate_random_future_date().strftime(DATE_FORMAT)
        random_tech_exam_date = generate_random_future_date().strftime(DATE_FORMAT)
        query = ("INSERT INTO Cars (brand, model, email, oc_insurance_expiration, technical_inspection_expiration)"
                 "VALUES (?, ?, ?, ?, ?)")
        cursor.execute(query, (brand, model, email, random_oc_date, random_tech_exam_date))
    connection.commit()


def get_expiring_soon(insurance_type, month=2):
    query = (f"SELECT * FROM Cars "
             f"WHERE {insurance_type} BETWEEN date('now') AND date('now', '+{month} month')"
             f"ORDER BY {insurance_type} DESC "
             )
    result = cursor.execute(query)
    return result.fetchall()


def send_notification_email(your_email, recipient_email, subject, body):
    smtp_server = 'smtp.gmail.com'
    smtp_port = 587
    password = 'password from Google app'

    msg = MIMEMultipart()
    msg['From'] = your_email
    msg['To'] = recipient_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()
    server.login(your_email, password)
    text = msg.as_string()
    server.sendmail(your_email, recipient_email, text)


def create_email_queue(oc_insurance_expiration, technical_inspection_expiration):
    emails = {}

    def add_to_queue(emails, date, email_data):
        if date not in emails:
            emails[date] = []
        emails[date].append(email_data)

    def process_expiration_data(expiration_data, email_type):
        for row in expiration_data:
            brand = row[1]
            model = row[2]
            email = row[3]
            expiration_date = datetime.strptime(row[4], DATE_FORMAT)
            month_before_date = expiration_date - timedelta(days=31)
            week_before_date = expiration_date - timedelta(days=7)
            three_days_before_date = expiration_date - timedelta(days=3)

            if email_type == 'oc':
                subject = 'Przypomnienie: koniec ubezpieczenia OS'
                mail_body = (f'Drogi właścicielu {brand} {model},\n\n'
                             f'Przypominamy, że ubezpieczenie OS dobiega końca {expiration_date.strftime(DATE_FORMAT)}.')
            else:
                subject = 'Przypomnienie: koniec badań technicznych'
                mail_body = (f'Drogi właścicielu {brand} {model},\n\n'
                             f'Przypominamy, że badania techniczne dobiegają końca {expiration_date.strftime(DATE_FORMAT)}.')

            add_to_queue(emails, month_before_date, {'email': email, 'subject': subject, 'body': mail_body})
            add_to_queue(emails, week_before_date, {'email': email, 'subject': subject, 'body': mail_body})
            add_to_queue(emails, three_days_before_date, {'email': email, 'subject': subject, 'body': mail_body})

    process_expiration_data(oc_insurance_expiration, 'oc')
    process_expiration_data(technical_inspection_expiration, 'inspection')

    return emails


def check_and_send_emails():
    today = datetime.now().strftime("%Y-%m-%d")
    if today in email_queue:
        for email_data in email_queue[today]:
            send_notification_email(
                your_email=EMAIL,
                recipient_email=email_data['email'],
                subject=email_data['subject'],
                body=email_data['body']
            )

if __name__ == '__main__':
    create_table()
    user_email = 'user_email'
    add_random_data_to_db(100, user_email)
    oc_insurance_expiration = get_expiring_soon('oc_insurance_expiration', month=2)
    technical_inspection_expiration = get_expiring_soon('technical_inspection_expiration', month=2)
    email_queue = create_email_queue(oc_insurance_expiration, technical_inspection_expiration)
    schedule.every().day.at("09:00").do(check_and_send_emails)

    while True:
        schedule.run_pending()
        time.sleep(1)