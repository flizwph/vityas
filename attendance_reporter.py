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
from config import (
    DB_CONFIG,
    SMTP_CONFIG,
    RECIPIENTS,
    ATTENDANCE_TABLE,
    D_FIELD,
    T_FIELD,
    EMPLOYEE_ID_EVENT_FIELD,
    CARD_ID_FIELD,
    DEPARTMENT_NAME_FIELD,
    NAME_LAST_FIELD,
    NAME_FIRST_FIELD,
    NAME_MIDDLE_FIELD,
    ZONE_FROM_FIELD,
    ZONE_TO_FIELD,
    EVENT_CODE_FIELD,
    STATUS_FIELD,
    SUCCESS_STATUSES,
    INNER_ZONE_NAMES,
    OUTER_ZONE_NAMES,
    DEBOUNCE_MILLISECONDS,
    ARRIVAL_TIME_FIELD,
    DEPARTURE_TIME_FIELD,
    TOTAL_TIME_FIELD,
)

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
                DATE '1858-11-17' + (e.{D_FIELD} - 678576) AS pass_date,
                DATEADD(MILLISECOND, e.{T_FIELD}, TIME '00:00:00') AS pass_time,
                DATEADD(MILLISECOND, e.{T_FIELD}, CAST(DATE '1858-11-17' + (e.{D_FIELD} - 678576) AS TIMESTAMP)) AS pass_ts,
                e.{EMPLOYEE_ID_EVENT_FIELD} AS employee_id,
                e.{CARD_ID_FIELD} AS card_id,
                TRIM(e.{DEPARTMENT_NAME_FIELD}) AS department_name,
                TRIM(e.{NAME_LAST_FIELD}) AS last_name,
                TRIM(e.{NAME_FIRST_FIELD}) AS first_name,
                TRIM(e.{NAME_MIDDLE_FIELD}) AS middle_name,
                TRIM(e.{ZONE_FROM_FIELD}) AS from_zone,
                TRIM(e.{ZONE_TO_FIELD}) AS to_zone,
                e.{EVENT_CODE_FIELD} AS event_code,
                e.{STATUS_FIELD} AS status_code
            FROM {ATTENDANCE_TABLE} e
            WHERE TRIM(e.{DEPARTMENT_NAME_FIELD}) = ?
              AND DATE(DATE '1858-11-17' + (e.{D_FIELD} - 678576)) BETWEEN ? AND ?
            ORDER BY pass_ts
            """

            df = pd.read_sql_query(query, conn, params=[department, start_date, end_date])
            conn.close()
            return df
            
        except Exception as e:
            logging.error(f"Ошибка получения данных для отдела {department}: {e}")
            if conn:
                conn.close()
            return None

    @staticmethod
    def _classify_and_clean(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df

        df = df[df['status_code'].isin(SUCCESS_STATUSES)].copy()
        if df.empty:
            return df

        def classify_direction(row):
            from_zone = (row['from_zone'] or '').upper()
            to_zone = (row['to_zone'] or '').upper()
            is_from_outer = any(name.upper() == from_zone for name in OUTER_ZONE_NAMES)
            is_to_inner = any(name.upper() == to_zone for name in INNER_ZONE_NAMES)
            is_from_inner = any(name.upper() == from_zone for name in INNER_ZONE_NAMES)
            is_to_outer = any(name.upper() == to_zone for name in OUTER_ZONE_NAMES)
            if is_from_outer and is_to_inner:
                return 'IN'
            if is_from_inner and is_to_outer:
                return 'OUT'
            return 'UNKNOWN'

        df['direction'] = df.apply(classify_direction, axis=1)

        df.sort_values(['employee_id', 'pass_ts'], inplace=True)
        
        def drop_bounce(group: pd.DataFrame) -> pd.DataFrame:
            kept_rows = []
            last_ts_by_dir = {}
            for _, row in group.iterrows():
                direction = row['direction']
                current_ts = row['pass_ts']
                last_ts = last_ts_by_dir.get(direction)
                if last_ts is None or (current_ts - last_ts).total_seconds() * 1000 > DEBOUNCE_MILLISECONDS:
                    kept_rows.append(row)
                    last_ts_by_dir[direction] = current_ts
            return pd.DataFrame(kept_rows)

        df = df.groupby('employee_id', as_index=False, group_keys=False).apply(drop_bounce)
        return df

    @staticmethod
    def _pair_intervals(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df

        records = []
        for employee_id, g in df.groupby('employee_id'):
            g = g.sort_values('pass_ts')
            current_in = None
            for _, row in g.iterrows():
                if row['direction'] == 'IN':
                    current_in = row['pass_ts']
                elif row['direction'] == 'OUT' and current_in is not None:
                    records.append({
                        'employee_id': employee_id,
                        ARRIVAL_TIME_FIELD: current_in,
                        DEPARTURE_TIME_FIELD: row['pass_ts'],
                        TOTAL_TIME_FIELD: (row['pass_ts'] - current_in)
                    })
                    current_in = None
        return pd.DataFrame(records)
    
    def create_excel_report(self, df, department, report_date):
        if df is None or df.empty:
            logging.warning(f"Нет данных для отдела {department}")
            return None
            
        filename = f"Отчет_{department}_отдел_{report_date}.xlsx"
        
        try:
            intervals = self._pair_intervals(self._classify_and_clean(df))
            if intervals is None or intervals.empty:
                logging.warning(f"Нет интервалов для отчёта: отдел {department}")
                return None
            
            latest_info = (
                df.sort_values('pass_ts')
                  .groupby('employee_id')
                  .agg({
                      'last_name': 'last',
                      'first_name': 'last',
                      'middle_name': 'last',
                      'department_name': 'last'
                  })
                  .reset_index()
            )

            report_df = intervals.merge(latest_info, on='employee_id', how='left')

            report_df['fio'] = report_df[['last_name', 'first_name', 'middle_name']]
            report_df['fio'] = report_df['fio'].fillna('').agg(' '.join, axis=1).str.strip()

            df_formatted = report_df[[
                'fio',
                'employee_id',
                'department_name',
                ARRIVAL_TIME_FIELD,
                DEPARTURE_TIME_FIELD,
                TOTAL_TIME_FIELD,
            ]].copy()

            df_formatted[ARRIVAL_TIME_FIELD] = pd.to_datetime(df_formatted[ARRIVAL_TIME_FIELD]).dt.strftime('%H:%M:%S')
            df_formatted[DEPARTURE_TIME_FIELD] = pd.to_datetime(df_formatted[DEPARTURE_TIME_FIELD]).dt.strftime('%H:%M:%S')
            df_formatted[TOTAL_TIME_FIELD] = df_formatted[TOTAL_TIME_FIELD].astype('timedelta64[m]').astype(int)

            df_formatted.columns = ['ФИО', 'Табельный номер', 'Отдел', 'Время прибытия', 'Время убытия', 'Минут всего']

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
    def monthly_if_needed():
        today = datetime.now()
        if today.day == 1:
            reporter.generate_monthly_report()

    schedule.every().day.at("08:05").do(monthly_if_needed)
    
    logging.info("Планировщик запущен. Ожидание выполнения задач...")
    
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
