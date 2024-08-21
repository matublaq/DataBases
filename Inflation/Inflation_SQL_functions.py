import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import openpyxl
import sqlite3
#--- Web scriping ---
import requests
from bs4 import BeautifulSoup
#--- ------------ ---
from datetime import datetime, date, timedelta
#from googletrans import Translator 

###############################################################################################################
####################################CREATE DATABASE AND/OR TABLES##############################################
def create_tables():
    #Connect/create database
    conn = sqlite3.connect("../Inflation.db")
    cursor = conn.cursor()

    #Create Countries table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Countries(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL, 
            code TEXT NOT NULL
        )
    ''')

    #Create inflation table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Inflation(
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            country_id INTEGER NOT NULL, 
            year INTEGER NOT NULL, 
            inflation_rate REAL, 
            FOREIGN KEY (country_id) REFERENCES Countries(id)
        )
    ''')

    conn.commit()
    conn.close()

def insert_country(csv_file_path, country):
    df = pd.read_csv(csv_file_path)
    country_dict = {}
    if country not in df['Country'].values:
        return 'Country not found in the dataset'
    
    conn = sqlite3.connect("../Inflation.db")
    cursor = conn.cursor()
    countries_db = cursor.execute("SELECT name FROM Countries").fetchall()
    countries_db_list = [ i[0] for i in countries_db]
    cursor.close()
    if country in countries_db_list:
        return 'Country already in the database'
    
    country_df = df[df['Country'] == country]
    country_dict['Country'] = country_df['Country'].values[0]
    country_dict['Code'] = country_df['Code'].values[0]
    country_dict['year'] = country_df.columns[2:].tolist()
    country_dict['inflation_rate'] = country_df.iloc[0, 2:].tolist()
    #SQL
    with sqlite3.connect("../Inflation.db") as conn:
        cursor = conn.cursor()

        #Insert countries
        cursor.execute("INSERT INTO Countries (name, code) VALUES (?, ?)", (country_dict['Country'], country_dict['Code']))
        
        #Get country id
        cursor.execute("SELECT id FROM Countries WHERE name = ?", (country_dict['Country'],))
        country_id = cursor.fetchone()[0]
        #Insert inflation data
        for i in range(len(country_dict['year'])):
            year = country_dict['year'][i]
            inflation_rate = country_dict['inflation_rate'][i]
            cursor.execute('''
                    INSERT INTO Inflation (country_id, year, inflation_rate) 
                    VALUES (?, ?, ?)
                ''', (country_id, country_dict['year'][i], country_dict['inflation_rate'][i]))
        #Commit the transaction
        conn.commit()
    
    return None

###############################################################################################################
###################################Deelete all rows from the tables############################################
def delete_all():
    #Connect to the database
    conn = sqlite3.connect('../Inflation.db')
    cursor = conn.cursor()

    #Delete all rows from the tables
    #cursor.execute("DELETE FROM Countries")
    #cursor.execute("DELETE FROM Inflation")
    
    #Drop tables
    cursor.execute("DROP TABLE IF EXISTS Countries")
    cursor.execute("DROP TABLE IF EXISTS Inflation")

    #Confirm change and close connection
    conn.commit()
    conn.close()