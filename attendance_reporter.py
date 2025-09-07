import fdb
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import schedule
import time
from datetime import datetime, timedelta
import os
import logging
from config import DB_CONFIG, SMTP_CONFIG, RECIPIENTS, ATTENDANCE_TABLE, EMPLOYEE_NAME_FIELD, EMPLOYEE_ID_FIELD, DEPARTMENT_FIELD, ARRIVAL_TIME_FIELD, DEPARTURE_TIME_FIELD, TOTAL_TIME_FIELD

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('attendance_reporter.log'),
        logging.StreamHandler()
    ]
)

class AttendanceReporter:
    def __init__(self):
        self.db_config = DB_CONFIG
        self.smtp_config = SMTP_CONFIG
        self.recipients = RECIPIENTS
        
    def connect_db(self):
        try:
            conn = fdb.connect(
                host=self.db_config['host'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password']
            )
            return conn
        except Exception as e:
            logging.error(f"Ошибка подключения к БД: {e}")
            return None
    
    def get_attendance_data(self, department, start_date, end_date):
        conn = self.connect_db()
        if not conn:
            return None
            
        try:
            query = f"""
            SELECT 
                {EMPLOYEE_NAME_FIELD},
                {EMPLOYEE_ID_FIELD},
                {DEPARTMENT_FIELD},
                {ARRIVAL_TIME_FIELD},
                {DEPARTURE_TIME_FIELD},
                {TOTAL_TIME_FIELD}
            FROM {ATTENDANCE_TABLE} 
            WHERE {DEPARTMENT_FIELD} = ? 
            AND DATE({ARRIVAL_TIME_FIELD}) BETWEEN ? AND ?
            ORDER BY {ARRIVAL_TIME_FIELD}
            """
            
            df = pd.read_sql_query(query, conn, params=[department, start_date, end_date])
            conn.close()
            return df
            
        except Exception as e:
            logging.error(f"Ошибка получения данных для отдела {department}: {e}")
            if conn:
                conn.close()
            return None
    
    def create_excel_report(self, df, department, report_date):
        if df is None or df.empty:
            logging.warning(f"Нет данных для отдела {department}")
            return None
            
        filename = f"Отчет_{department}_отдел_{report_date}.xlsx"
        
        try:
            df_formatted = df.copy()
            df_formatted[ARRIVAL_TIME_FIELD] = pd.to_datetime(df_formatted[ARRIVAL_TIME_FIELD]).dt.strftime('%H:%M:%S')
            df_formatted[DEPARTURE_TIME_FIELD] = pd.to_datetime(df_formatted[DEPARTURE_TIME_FIELD]).dt.strftime('%H:%M:%S')
            
            df_formatted.columns = ['ФИО', 'Табельный номер', 'Отдел', 'Время прибытия', 'Время убытия', 'Общее время']
            
            df_formatted.to_excel(filename, index=False, engine='openpyxl')
            logging.info(f"Создан файл: {filename}")
            return filename
            
        except Exception as e:
            logging.error(f"Ошибка создания Excel файла: {e}")
            return None
    
    def send_email(self, recipient, subject, body, attachment_path):
        try:
            msg = MIMEMultipart()
            msg['From'] = self.smtp_config['user']
            msg['To'] = recipient
            msg['Subject'] = subject
            
            msg.attach(MIMEText(body, 'plain', 'utf-8'))
            
            if attachment_path and os.path.exists(attachment_path):
                with open(attachment_path, "rb") as attachment:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment.read())
                
                encoders.encode_base64(part)
                part.add_header(
                    'Content-Disposition',
                    f'attachment; filename= {os.path.basename(attachment_path)}'
                )
                msg.attach(part)
            
            server = smtplib.SMTP(self.smtp_config['server'], self.smtp_config['port'])
            server.starttls()
            server.login(self.smtp_config['user'], self.smtp_config['password'])
            
            text = msg.as_string()
            server.sendmail(self.smtp_config['user'], recipient, text)
            server.quit()
            
            logging.info(f"Email отправлен на {recipient}")
            return True
            
        except Exception as e:
            logging.error(f"Ошибка отправки email: {e}")
            return False
    
    def generate_daily_report(self):
        yesterday = datetime.now() - timedelta(days=1)
        report_date = yesterday.strftime('%Y-%m-%d')
        
        logging.info(f"Генерация ежедневного отчета за {report_date}")
        
        for department, email in self.recipients.items():
            df = self.get_attendance_data(department, report_date, report_date)
            filename = self.create_excel_report(df, department, report_date)
            
            if filename:
                subject = f"Отчет по посещаемости отдела {department} за {report_date}"
                body = f"Отчет по посещаемости сотрудников отдела {department} за {report_date}"
                
                if self.send_email(email, subject, body, filename):
                    os.remove(filename)
    
    def generate_weekly_report(self):
        today = datetime.now()
        week_start = today - timedelta(days=today.weekday() + 7)
        week_end = week_start + timedelta(days=6)
        
        report_date = f"{week_start.strftime('%Y-%m-%d')}_to_{week_end.strftime('%Y-%m-%d')}"
        
        logging.info(f"Генерация еженедельного отчета за {report_date}")
        
        for department, email in self.recipients.items():
            df = self.get_attendance_data(department, week_start.strftime('%Y-%m-%d'), week_end.strftime('%Y-%m-%d'))
            filename = self.create_excel_report(df, department, report_date)
            
            if filename:
                subject = f"Еженедельный отчет по посещаемости отдела {department} за {report_date}"
                body = f"Еженедельный отчет по посещаемости сотрудников отдела {department} за период {report_date}"
                
                if self.send_email(email, subject, body, filename):
                    os.remove(filename)
    
    def generate_monthly_report(self):
        today = datetime.now()
        month_start = today.replace(day=1) - timedelta(days=1)
        month_start = month_start.replace(day=1)
        month_end = today.replace(day=1) - timedelta(days=1)
        
        report_date = f"{month_start.strftime('%Y-%m')}"
        
        logging.info(f"Генерация месячного отчета за {report_date}")
        
        for department, email in self.recipients.items():
            df = self.get_attendance_data(department, month_start.strftime('%Y-%m-%d'), month_end.strftime('%Y-%m-%d'))
            filename = self.create_excel_report(df, department, report_date)
            
            if filename:
                subject = f"Месячный отчет по посещаемости отдела {department} за {report_date}"
                body = f"Месячный отчет по посещаемости сотрудников отдела {department} за {report_date}"
                
                if self.send_email(email, subject, body, filename):
                    os.remove(filename)

def main():
    reporter = AttendanceReporter()
    
    schedule.every().day.at("08:00").do(reporter.generate_daily_report)
    schedule.every().monday.at("08:00").do(reporter.generate_weekly_report)
    schedule.every().month.do(reporter.generate_monthly_report)
    
    logging.info("Планировщик запущен. Ожидание выполнения задач...")
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
