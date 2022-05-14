# -*- coding: utf-8 -*-

import pyodbc 
import pandas as pd
import os
import numpy as np
import csv

server_name = input()

# Proces ETL  
## Extract -- 1 --

# Baza danych MS SQL - dane z Polski
conn = pyodbc.connect("Driver={SQL Server};"
                      "Server="+server_name+";"
                      "Database=Projekt_WAD;"
                      "Trusted_Connection=yes;")

## Pobranie tabeli 'Taxes' (Taxes = mytable)
df = pd.read_sql_query('select * from mytable', conn)
df

## Pobranie tabeli 'Cars'
Cars = pd.read_sql_query('select * from Cars_Poland', conn)
Cars

conn.close()

### ustawienie sciezki 
os.chdir()

# Baza danych aut z USA
USA_Cars_Data = pd.read_csv('usa_cars_all.csv')

# Auta z Wielkiej Brytanii
Audi = pd.read_csv('audi_all1.csv')
BMW = pd.read_csv('bmw_all1.csv')
Ford = pd.read_csv('ford_all1.csv')
Hyundai = pd.read_csv('hyundai_all1.csv')
Mercedes = pd.read_csv('merc_all1.csv')
Skoda = pd.read_csv('skoda_all1.csv')
Toyota = pd.read_csv('toyota_all1.csv')
Volkswagen = pd.read_csv('vw_all1.csv')

## Transform -- 2 -- 
## Zamiana nazw kolumn na poprawne

# dla dataframe "Cars" 
dict1 = {'mark': 'Mark', 'model': 'Model', 'mileage': 'Mileage',
        'vol_engine':'Engine', 'fuel':'Fuel', 'price':'Price', 
        'time_sold':'Sold Time', 'dealer':'Vendor', 'year': 'Year'}

## dla wszystkich dataframe z "UK"
dict2 = {'model': 'Model', 'mileage': 'Mileage', 'year':'Year',
        'engineSize':'Engine', 'fuelType':'Fuel', 'price':'Price', 
        'time_sold':'Sold Time', 'dealer':'Vendor'}

## Dla dataframe "USA"
dict3 = {'make_name': 'Mark', 'model_name': 'Model', 'mileage': 'Mileage',
        'engine_displacement':'Engine', 'fuel_type':'Fuel', 'price':'Price', 
        'time_sold':'Sold Time', 'dealer':'Vendor', 'year':'Year'}


Cars.rename(columns=dict1, inplace=True)
USA_Cars_Data.rename(columns=dict3, inplace=True)
Audi.rename(columns=dict2, inplace=True)
BMW.rename(columns=dict2, inplace=True)
Ford.rename(columns=dict2, inplace=True)
Hyundai.rename(columns=dict2, inplace=True)
Mercedes.rename(columns=dict2, inplace=True)
Skoda.rename(columns=dict2, inplace=True)
Toyota.rename(columns=dict2, inplace=True)
Volkswagen.rename(columns=dict2, inplace=True)

## dodanie marki do oddzielnych tabel z autami
Audi['Mark'] = "Audi"; BMW['Mark'] = "BMW"
Ford['Mark'] = "Ford"; Hyundai['Mark'] = "Hyundai"
Mercedes['Mark'] = "Mercedes"; Skoda['Mark'] = "Skoda"
Toyota['Mark'] = "Toyota"; Volkswagen['Mark'] = "Volksswagen"


## Funkcja do wybrania kolumn  
def extract(data):
    extracted_data = data[['Mark','Model','Year','Mileage','Engine','Fuel',
                      'Price','Sold Time','Vendor']]
    extracted_data = extracted_data[extracted_data['Sold Time'].notna()]
    extracted_data = extracted_data[extracted_data['Fuel'].notna()]
    extracted_data = extracted_data[extracted_data['Sold Time'].str.contains('NA') == False]
    
    return extracted_data

## Wyodrebnione dane
EXT_Cars_PL = extract(Cars)
EXT_Cars_USA = extract(USA_Cars_Data)
EXT_Cars_Audi = extract(Audi)
EXT_Cars_BMW = extract(BMW)
EXT_Cars_Ford = extract(Ford)
EXT_Cars_Hyundai = extract(Hyundai)
EXT_Cars_Merc = extract(Mercedes)
EXT_Cars_Skoda = extract(Skoda)
EXT_Cars_Toyota = extract(Toyota)
EXT_Cars_VW = extract(Volkswagen)

## Zmiana indeksowania 
EXT_Cars_PL = EXT_Cars_PL.reset_index(drop=True)
EXT_Cars_USA = EXT_Cars_USA.reset_index(drop=True)
EXT_Cars_Audi = EXT_Cars_Audi.reset_index(drop=True)
EXT_Cars_BMW = EXT_Cars_BMW.reset_index(drop=True)
EXT_Cars_Ford = EXT_Cars_Ford.reset_index(drop=True)
EXT_Cars_Hyundai = EXT_Cars_Hyundai.reset_index(drop=True)
EXT_Cars_Merc = EXT_Cars_Merc.reset_index(drop=True)
EXT_Cars_Skoda = EXT_Cars_Skoda.reset_index(drop=True)
EXT_Cars_Toyota = EXT_Cars_Toyota.reset_index(drop=True)
EXT_Cars_VW = EXT_Cars_VW.reset_index(drop=True)
#EXT_Cars_PL['Sold Time'][5]

## Dodanie kraju jako kolumne
EXT_Cars_PL['Country'] = 'Poland'
EXT_Cars_USA['Country'] = 'USA'
EXT_Cars_Audi['Country'] = 'United Kingdom'
EXT_Cars_BMW['Country'] = 'United Kingdom'
EXT_Cars_Ford['Country'] = 'United Kingdom'
EXT_Cars_Hyundai['Country'] = 'United Kingdom'
EXT_Cars_Merc['Country'] = 'United Kingdom'
EXT_Cars_Skoda['Country'] = 'United Kingdom'
EXT_Cars_Toyota['Country'] = 'United Kingdom'
EXT_Cars_VW['Country'] = 'United Kingdom'

## Usuniecie niepotrzebnych zmiennych - zwolnienie pamieci
del [Audi,BMW,Cars,Ford,Hyundai,Mercedes,Skoda,Toyota,USA_Cars_Data,Volkswagen]
del [dict1, dict2, dict3] 
del [server_name]; del[conn]


## Funkcja do przeksztalcenia danych tekstowych
def transform_data(data):
    data[['Mark', 'Model']] = data[['Mark', 'Model']].astype(str).apply(lambda col: col.str.capitalize())
    
    data['Fuel'] = np.where(data['Fuel'].isin(['Petrol','Gasoline', 'Diesel', 'Hybrid','LPG']), data['Fuel'], 'Other')
    data['Fuel'] = np.where(data['Fuel'].isin(['Gasoline']), 'Petrol',data['Fuel'])
    
    data['Sold Time'] = data['Sold Time'].str.split(" ").str[0]
    
    return data

EXT_Cars_USA = transform_data(EXT_Cars_USA)
EXT_Cars_PL = transform_data(EXT_Cars_PL)
EXT_Cars_Audi = transform_data(EXT_Cars_Audi)
EXT_Cars_BMW = transform_data(EXT_Cars_BMW)
EXT_Cars_Ford = transform_data(EXT_Cars_Ford)
EXT_Cars_Merc = transform_data(EXT_Cars_Merc)
EXT_Cars_Skoda = transform_data(EXT_Cars_Skoda)
EXT_Cars_Toyota = transform_data(EXT_Cars_Toyota)
EXT_Cars_VW = transform_data(EXT_Cars_VW)


## Funkcja do przeksztalcenia danych liczbowych
def engine_transform(x): 
    if isinstance(x, float):    
        return x
    else:
        y = round((x/1000), 1)
        return y

EXT_Cars_USA['Engine'] = engine_transform(EXT_Cars_USA['Engine'])
EXT_Cars_PL['Engine'] = engine_transform(EXT_Cars_PL['Engine'])

## Polaczenie w jedna tabele do analizy (data warehouse)
EXT_ALL = pd.concat([EXT_Cars_PL, EXT_Cars_USA, EXT_Cars_Audi, EXT_Cars_BMW, EXT_Cars_Ford, EXT_Cars_Hyundai, EXT_Cars_Merc, EXT_Cars_Skoda, EXT_Cars_Toyota, EXT_Cars_VW])

## 'Transform' ukonczony 

## Zapis do csv 
pd.DataFrame.to_csv(EXT_ALL, 'Hurtownia.csv', sep=',', na_rep='.', index=False)

## Load -- 3 --
## Zasilenie tabeli utworzonym plikiem csv - stworzona hurtownia z danymi 

import psycopg2

conn = psycopg2.connect(
    dbname="Projekt_WAD_Hurtownia",
    user="postgres",
    host="localhost",
    password="analizadanych"
)

cur = conn.cursor()

cur.execute("""CREATE TABLE Hurtownia_Samochody(
    Mark text,
    Model text,
    Year int,
    Mileage float,
    Engine float,
    Fuel text,
    Price float,
    Sold_Time text,
    Vendor text,
    Country text
)
""")

with open('merged3.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader) # Skip the header row.
    for row in reader:
        cur.execute(
        "INSERT INTO Hurtownia_Samochody VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        row
    )
conn.commit()

cur.close()

conn.close()

## !! Proces ETL zostal ukonczony pomyslnie !!
