# -*- coding: utf-8 -*-

import pyodbc 
import pandas as pd
import os
import numpy as np
import csv

server_name = input()

# ETL Process  
## Extract -- 1 --

# Microsoft SQL Server database - data from Poland
conn = pyodbc.connect("Driver={SQL Server};"
                      "Server="+server_name+";"
                      "Database=Projekt_WAD;"
                      "Trusted_Connection=yes;")

## Get table 'Taxes' (Taxes = mytable)
df = pd.read_sql_query('select * from mytable', conn)
df

## Get table 'Cars'
Cars = pd.read_sql_query('select * from Cars_Poland', conn)
Cars

conn.close()

### Setting the path 
os.chdir()

# Database of cars from USA
USA_Cars_Data = pd.read_csv('usa_cars_all.csv')

# Database of cars from United Kingdom
Audi = pd.read_csv('audi_all1.csv')
BMW = pd.read_csv('bmw_all1.csv')
Ford = pd.read_csv('ford_all1.csv')
Hyundai = pd.read_csv('hyundai_all1.csv')
Mercedes = pd.read_csv('merc_all1.csv')
Skoda = pd.read_csv('skoda_all1.csv')
Toyota = pd.read_csv('toyota_all1.csv')
Volkswagen = pd.read_csv('vw_all1.csv')

## Transform -- 2 -- 
## Rename columns to the correct forms

# For "Cars" dataframe 
dict1 = {'mark': 'Mark', 'model': 'Model', 'mileage': 'Mileage',
        'vol_engine':'Engine', 'fuel':'Fuel', 'price':'Price', 
        'time_sold':'Sold Time', 'dealer':'Vendor', 'year': 'Year'}

## For all dataframes with "UK"
dict2 = {'model': 'Model', 'mileage': 'Mileage', 'year':'Year',
        'engineSize':'Engine', 'fuelType':'Fuel', 'price':'Price', 
        'time_sold':'Sold Time', 'dealer':'Vendor'}

## For "USA" dataframe
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

## Adding brands
Audi['Mark'] = "Audi"; BMW['Mark'] = "BMW"
Ford['Mark'] = "Ford"; Hyundai['Mark'] = "Hyundai"
Mercedes['Mark'] = "Mercedes"; Skoda['Mark'] = "Skoda"
Toyota['Mark'] = "Toyota"; Volkswagen['Mark'] = "Volkswagen"


## Function to select columns 
def extract(data):
    extracted_data = data[['Mark','Model','Year','Mileage','Engine','Fuel',
                      'Price','Sold Time','Vendor']]
    extracted_data = extracted_data[extracted_data['Sold Time'].notna()]
    extracted_data = extracted_data[extracted_data['Fuel'].notna()]
    extracted_data = extracted_data[extracted_data['Sold Time'].str.contains('NA') == False]
    
    return extracted_data

## Extracted data
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

## Change indexing 
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

## Add country as a column
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

## Remove unnecessary variables to free up memory
del [Audi,BMW,Cars,Ford,Hyundai,Mercedes,Skoda,Toyota,USA_Cars_Data,Volkswagen]
del [dict1, dict2, dict3] 
del [server_name]; del[conn]


## Function to convert text data
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


## Function to convert numeric data
def engine_transform(x): 
    if isinstance(x, float):    
        return x
    else:
        y = round((x/1000), 1)
        return y

EXT_Cars_USA['Engine'] = engine_transform(EXT_Cars_USA['Engine'])
EXT_Cars_PL['Engine'] = engine_transform(EXT_Cars_PL['Engine'])

## Combine into one table for analysis (data warehouse)
EXT_ALL = pd.concat([EXT_Cars_PL, EXT_Cars_USA, EXT_Cars_Audi, EXT_Cars_BMW, EXT_Cars_Ford, EXT_Cars_Hyundai, EXT_Cars_Merc, EXT_Cars_Skoda, EXT_Cars_Toyota, EXT_Cars_VW])

## "Transform" complete

## Save to csv - (also as a copy on hard drive)
pd.DataFrame.to_csv(EXT_ALL, 'Hurtownia.csv', sep=',', na_rep='.', index=False)

## Load -- 3 --
## Load data to PostgreSQL table with created csv file - (created data warehouse) 

import psycopg2

## Informations needed to connect to the database
p_pass = input()
conn = psycopg2.connect(
    dbname="Projekt_WAD_Hurtownia",
    user="postgres",
    host="localhost",
    password=p_pass
)

cur = conn.cursor() #  Opening the connection to the database

## Creating new table
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

## Insert data with BULK method 
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

conn.close() # Closing the connection to the database

