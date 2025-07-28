import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import seaborn as sns
import statsmodels.stats.proportion as proportion
from scipy.stats import ttest_ind, mannwhitneyu, shapiro, norm
from statsmodels.stats.weightstats import ztest
from tqdm import tqdm
import timeit
from scipy import stats
import math
from datetime import date, datetime, timedelta
import time
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import clickhouse_connect
import gspread
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials
import requests
import xml.etree.ElementTree as ET
from oauth2client.service_account import ServiceAccountCredentials

# Функция для получения курса йены
def get_jpy_rate(date):
    formatted_date = date.strftime("%d/%m/%Y")
    url = f"https://www.cbr.ru/scripts/XML_daily.asp?date_req={formatted_date}"
    response = requests.get(url)
    response.raise_for_status()
    
    tree = ET.fromstring(response.content)
    for currency in tree.findall('Valute'):
        char_code = currency.find('CharCode').text
        if char_code == "JPY":
            rate = currency.find('Value').text
            return float(rate.replace(',', '.')) / 100
    return None

def write_to_google_sheets(sheet_name, date, value):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("/Users/kemran/Desktop/work_files/python_files/practice_files/exchange_rate/exchange-rate.json", scope) 
    client = gspread.authorize(creds)

    spreadsheet = client.open(sheet_name)
    sheet = spreadsheet.worksheet("exchange_rate")
    
    formatted_date = date.strftime("%Y-%m-%d")
    sheet.append_row([formatted_date, value], value_input_option="USER_ENTERED")

if __name__ == "__main__":
    try:
        today = datetime.now()
        jpy_rate = get_jpy_rate(today)
        
        if jpy_rate is not None:
            print(f"Курс JPY на {today.strftime('%d/%m/%Y')}: {jpy_rate}")
            write_to_google_sheets("Japan_trip", today, jpy_rate)
            print("Курс успешно записан в Google Sheets.")
        else:
            print(f"Курс JPY на {today.strftime('%d/%m/%Y')} не найден.")
    except Exception as e:
        print(f"Ошибка: {e}")
