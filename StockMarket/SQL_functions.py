import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

import yfinance as yf

import sqlite3
import shutil

###############################################################################################################
#################################Creating StockMarket database tables##########################################
def create_database(database_path):
    #Connect to the database
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    #Create companies table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Companies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    ticker TEXT NOT NULL UNIQUE
                    )
    ''')

    #Create Stock Quotes table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Stock_quotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT, 
                company_id INTEGER, 
                date DATE NOT NULL, 
                open REAL,
                high REAL, 
                low REAL, 
                close REAL, 
                volume INTEGER, 
                FOREIGN KEY (company_id) REFERENCES Companies(id)
                )
    ''')

    #Create Index table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Idx (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    name TEXT NOT NULL, 
                    ticker TEXT NOT NULL UNIQUE
                    )
    ''')

    #Confirm change and close connection
    conn.commit()
    conn.close()


########################################################################################################
#############################Insert data into database##################################################
def insert_company(ticker, database_path):
    ticker = ticker.upper()
    company_info = yf.Ticker(ticker).info 
    #Is in yfinance?
    if not company_info or 'shortName' not in company_info:
        return f"El ticker {ticker} no está en yfinance"
    
    company_name = company_info['shortName']
    #SQL
    with sqlite3.connect(database_path, timeout=15) as conn: #Database connection
        cursor = conn.cursor()
        
        #Is in the database?
        cursor.execute("SELECT id FROM Companies WHERE ticker = ?", (ticker,))
        result = cursor.fetchone()
        if result: #Yes
            company_id = result[0]
            return f"El ticker {ticker} ya esta en la base de datos"
        else: #No
            cursor.execute("INSERT INTO Companies (name, ticker) VALUES (?, ?)", (company_name, ticker))
            company_id = cursor.lastrowid
        
        #Get ticker from Companies table
        cursor.execute("SELECT ticker FROM Companies WHERE id = ?", (company_id,))
        ticker = cursor.fetchone()[0]
        #Download stock data
        data = yf.download(ticker)
        data = data.round(3)

        #Insert data into Stock_quotes table
        for date, row in data.iterrows():
            cursor.execute("INSERT INTO Stock_quotes  (company_id, date, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)", 
                                                        (company_id, date.strftime('%Y-%m-%d'), row['Open'], row['High'], row['Low'], row['Close'], row['Volume']))
        #Today's date? If yes, delete
        if date.strftime('%Y-%m-%d') == datetime.today().strftime('%Y-%m-%d'):
            cursor.execute("DELETE FROM Stock_quotes WHERE date = ?", (date.strftime('%Y-%m-%d'),))

        conn.commit()
        return f"{ticker} ha sido agregado a la base de datos"

###############################################################################################################
###################################Deelete all rows from the tables############################################
def delete_database(database_path):
    #Connect to the database
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    
    #Drop tables
    cursor.execute("DROP TABLE IF EXISTS Companies")
    cursor.execute("DROP TABLE IF EXISTS Stock_quotes")
    cursor.execute("DROP TABLE IF EXISTS Idx")

    #Confirm change and close connection
    conn.commit()
    conn.close()

def delete_data(database_name):
    #Connect to the database
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()

    #Delete all rows from the tables
    cursor.execute("DELETE FROM Companies")
    cursor.execute("DELETE FROM Stock_quotes")
    cursor.execute("DELETE FROM Idx")

def delete_company(ticker, database_path):
    ticker = ticker.upper()

    #Connect to the database
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    #Get company id
    cursor.execute("SELECT id FROM Companies WHERE ticker = ?", (ticker,))
    result = cursor.fetchone()[0]
    if not result:
        return f"El ticker {ticker} no está en la base de datos"
    #Delete data
    cursor.execute("DELETE FROM Companies WHERE id = ?", (result,))
    cursor.execute("DELETE FROM Stock_quotes WHERE company_id = ?", (result,))

    #Confirm change and close connection
    conn.commit()
    conn.close()

##############################################################################################################
###############################################BACKUP#########################################################
def backup_database(file_path):
    shutil.copyfile(file_path, file_path[3:-3] + '_backup.db')

########################################################################################################
#####################################UPDATE STOCK QUOTES################################################
#Update stock quotes
def update_stock_quotes(ticker, database_path):
    ticker = ticker.upper()
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    company_id = pd.read_sql_query("SELECT id FROM Companies WHERE ticker = ?", conn, params=(ticker, ))
    company_id = company_id['id'][0] 
    stock_quotes = pd.read_sql_query(f"SELECT date, open, high, low, close, volume FROM Stock_quotes WHERE company_id = {company_id}", conn)
        
    #Day update
    #stock_quotes = stock_quotes['date'].max()
    #stock_quotes = datetime.strptime(stock_quotes, '%Y-%m-%d')
    #stock_quotes = (stock_quotes + timedelta(days=1)).strftime('%Y-%m-%d')
    stock_quotes_next_day = (datetime.strptime(stock_quotes['date'].max(), '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    today = datetime.today().strftime('%Y-%m-%d')

    new_data = yf.download(ticker, start=stock_quotes_next_day, end=today)
    new_data = new_data.round(3)
    if not new_data.empty:
        new_data['Company_id'] = company_id
        new_data.reset_index(inplace=True)
        new_data = new_data[['Company_id', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        new_data['Date'] = new_data['Date'].dt.strftime('%Y-%m-%d') 

        tuples_of_data = []
        for index, row in new_data.iterrows():
            tuples_of_data.append(tuple(row))
        print(tuples_of_data)
        cursor.executemany('''
            INSERT INTO Stock_quotes (company_id, date, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', tuples_of_data)
    else:
        print(f"Ticker {ticker} \n Ultima fecha {stock_quotes['date'].max()} \n Hoy es {today}")
        pass

    conn.commit()
    conn.close()

#Update all tickers
def update_all_stock_quotes(database):
    conn = sqlite3.connect(database)
    
    tickers = pd.read_csv('tickers_backup.csv')
    tickers = list(tickers['ticker'])
    for ticker in tickers:
        update_stock_quotes(ticker, database)
    
    conn.close()
    return f"Tickers updated \n {tickers}"