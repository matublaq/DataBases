import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import yfinance as yf

import sqlite3

###############################################################################################################
#################################Creating StockMarket database tables##########################################
def create_tables():
    #Connect to the database
    conn = sqlite3.connect('../StockMarket.db')
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


###############################################################################################################
#############################Insert data into database##################################################
def new_company(tickers):
    tickers = list(tickers.upper().replace(' ', '').split(','))
    for ticker in tickers:
        company_id = insert_company(ticker)
        if company_id is None:
            pass
        else:
            insert_stock_quotes(company_id)
    company_id = None


#############################
def insert_company(ticker):
    company_info = yf.Ticker(ticker).info 
    #Is in yfinance?
    if not company_info or 'shortName' not in company_info: #No
        print(f"El ticker {ticker} no existe en yfinance")
        return None
    company_name = company_info['shortName']

    #SQL
    with sqlite3.connect('../StockMarket.db', timeout=15) as conn: #Database connection
        cursor = conn.cursor()
        
        cursor.execute("SELECT id FROM Companies WHERE ticker = ?", (ticker,)) #Is in the database?
        result = cursor.fetchone()
        if result: #Yes
            company_id = result[0]
            print("Ya esta en la base de datos")
        else: #No
            cursor.execute("INSERT INTO Companies (name, ticker) VALUES (?, ?)", (company_name, ticker))
            company_id = cursor.lastrowid
    return company_id

#Insert data into Stock_quotes table
def insert_stock_quotes(company_id):
    with sqlite3.connect('../StockMarket.db', timeout=15) as conn: #Database connection 
        cursor = conn.cursor()
        
        cursor.execute("SELECT company_id FROM Stock_quotes WHERE company_id = ?", (company_id,)) #Is in the database?
        result = cursor.fetchone()
        if result: #Yes
            print("Ya esta en la base de datos")
            company_id = result[0]
        else: #No
            #Get ticker from Companies table
            cursor.execute("SELECT ticker FROM Companies WHERE id = ?", (company_id,))
            ticker = cursor.fetchone()[0]
            #Download stock data
            data = yf.download(ticker)    

            #Insert data into Stock_quotes table
            for date, row in data.iterrows():
                cursor.execute("INSERT INTO Stock_quotes  (company_id, date, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)", 
                                                            (company_id, date.strftime('%Y-%m-%d'), row['Open'], row['High'], row['Low'], row['Close'], row['Volume']))
        conn.commit()
        return company_id

#Update stock quotes
def update_stock_quotes():
    conn = sqlite3.connect('StockMarket_test.db')#'../StockMarket.db'
    cursor = conn.cursor()

    companies_df = pd.read_sql_query("SELECT * FROM Companies", conn)
    companies_ids = companies_df['id'].tolist()

    for comp_id in companies_ids:
        ticker = companies_df[companies_df['id'] == comp_id]['ticker'].values[0]
        stock_quotes = pd.read_sql_query(f"SELECT date, open, high, low, close, volume FROM Stock_quotes WHERE company_id = {comp_id}", conn)
        if not (stock_quotes.empty):
            #Day update
            #stock_quotes = stock_quotes['date'].max()
            #stock_quotes = datetime.strptime(stock_quotes, '%Y-%m-%d')
            #stock_quotes = (stock_quotes + timedelta(days=1)).strftime('%Y-%m-%d')
            stock_quotes_next_day = (datetime.strptime(stock_quotes['date'].max(), '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
            today = datetime.today().strftime('%Y-%m-%d')

            new_data = yf.download(ticker, start=stock_quotes_next_day, end=today)
            if not new_data.empty:
                new_data['Company_id'] = comp_id
                new_data.reset_index(inplace=True)
                new_data = new_data[['Company_id', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
                new_data['Date'] = new_data['Date'].dt.strftime('%Y-%m-%d') 

                tuples_of_data = []
                for index, row in new_data.iterrows():
                    tuples_of_data.append(tuple(row))

                cursor.executemany('''
                    INSERT INTO Stock_quotes (company_id, date, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', tuples_of_data)
            else:
                print(f"Ticker {ticker} \n Ultima fecha {stock_quotes['date'].max()} \n Hoy es {today}")
                pass
        else:
            print(f"{ticker} no tiene datos")

    conn.commit()
    conn.close()
###############################################################################################################
###################################Deelete all rows from the tables############################################
def delete_all():
    #Connect to the database
    conn = sqlite3.connect('../StockMarket.db')
    cursor = conn.cursor()

    #Delete all rows from the tables
    #cursor.execute("DELETE FROM Companies")
    #cursor.execute("DELETE FROM Stock_quotes")
    #cursor.execute("DELETE FROM Idx")
    
    #Drop tables
    cursor.execute("DROP TABLE IF EXISTS Companies")
    cursor.execute("DROP TABLE IF EXISTS Stock_quotes")
    cursor.execute("DROP TABLE IF EXISTS Idx")

    #Confirm change and close connection
    conn.commit()
    conn.close()