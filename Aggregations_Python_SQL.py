import psycopg2
import pandas as pd
import numpy as np
import csv

p_pass = input()

conn = psycopg2.connect(
    dbname="Projekt_WAD_Hurtownia",
    user="postgres",
    host="localhost",
    password= p_pass
)

cur = conn.cursor()

## Select table
query1 = "select * from hurtownia_samochody"

cur.execute(query1)

records = cur.fetchall()

## create huge dataframe
Cars = pd.DataFrame(records)

Cars.head()

Cars.columns = ["Mark","Model","Year","Mileage",
               "Engine","Fuel","Price","Sold_Time",
               "Vendor","Country"]


query2 = ("SELECT * FROM (" +
    " SELECT Mark, Model, count(Model)," + 
    "ROW_NUMBER() OVER (PARTITION BY Mark Order by count(Model) DESC) AS Rank1" +
    " FROM hurtownia_samochody" +
    " Where Country = 'Poland' AND Mark IN" + 
        " (SELECT Mark FROM hurtownia_samochody" + 
        " Where Country = 'Poland'"
        " group by Mark Order by count(Model) DESC LIMIT 5)" + 
        " Group by Mark, Model )RNK" +
        " WHERE Rank1 <=3")

cur.execute(query2)
TOP_PL = cur.fetchall()
TOP_PL = pd.DataFrame(TOP_PL)

query3 = ("SELECT * FROM (" +
    " SELECT Mark, Model, count(Model)," + 
    "ROW_NUMBER() OVER (PARTITION BY Mark Order by count(Model) DESC) AS Rank1" +
    " FROM hurtownia_samochody" +
    " Where Country = 'United Kingdom' AND Mark IN" + 
        " (SELECT Mark FROM hurtownia_samochody" + 
        " Where Country = 'United Kingdom'"
        " group by Mark Order by count(Model) DESC LIMIT 5)" + 
        " Group by Mark, Model )RNK" +
        " WHERE Rank1 <=3")

cur.execute(query3)
TOP_UK = cur.fetchall()
TOP_UK = pd.DataFrame(TOP_UK)

query4 = ("SELECT * FROM (" +
    " SELECT Mark, Model, count(Model)," + 
    "ROW_NUMBER() OVER (PARTITION BY Mark Order by count(Model) DESC) AS Rank1" +
    " FROM hurtownia_samochody" +
    " Where Country = 'USA' AND Mark IN" + 
        " (SELECT Mark FROM hurtownia_samochody" + 
        " Where Country = 'USA'"
        " group by Mark Order by count(Model) DESC LIMIT 5)" + 
        " Group by Mark, Model )RNK" +
        " WHERE Rank1 <=3")

cur.execute(query4)
TOP_USA = cur.fetchall()
TOP_USA = pd.DataFrame(TOP_USA)


## Close connection
cur.close()
conn.close()

del[conn, cur, p_pass, query1, query2, query3, query4, records]

Cars['Mark'].mask(Cars['Mark'] == 'Volksswagen', 'Volkswagen', inplace=True)
Cars['Sold_Year'] = Cars['Sold_Time'].str[:4]
Cars['Sold_Month'] = Cars['Sold_Time'].str[:7]
Cars = Cars.drop(Cars[Cars.Year > 2023].index)


## - Scenario 1 - ##

### 1) Sprzedaz danej marki w danym roku (TOP10)

## Group by Mark & Country & Sold_Time
df1 = Cars.groupby(['Mark',"Country","Sold_Year"], as_index=False)['Price'].mean()
df1_c = pd.DataFrame(Cars.groupby(['Mark',"Country","Sold_Year"]).size().reset_index(name='counts'))

df1[['Counts']] = df1_c[['counts']]

## Supporting Dataframe 
## Group by Mark & Country
df2 = Cars.groupby(['Mark',"Country", "Sold_Month"],as_index=False)['Price'].mean()
df2_c = pd.DataFrame(Cars.groupby(['Mark',"Country","Sold_Month"]).size().reset_index(name='counts'))

df2[['Counts']] = df2_c[['counts']]


## Unormowanie walut - wszystko z perspektywy Polski (PLN)
# Przyjeto orientacyjnie kursy walut: GBP/PLN = 5, USD/PLN = 3.90

df1.loc[df1["Country"] == "Poland", "Price_PLN"] = df1.Price.apply(lambda x: x)
df1.loc[df1["Country"] == "USA", "Price_PLN"] = df1.Price.apply(lambda x: x*3.90)
df1.loc[df1["Country"] == "United Kingdom", "Price_PLN"] = df1.Price.apply(lambda x: x*5)

df2.loc[df2["Country"] == "Poland", "Price_PLN"] = df2.Price.apply(lambda x: x)
df2.loc[df2["Country"] == "USA", "Price_PLN"] = df2.Price.apply(lambda x: x*3.90)
df2.loc[df2["Country"] == "United Kingdom", "Price_PLN"] = df2.Price.apply(lambda x: x*5)

del[df1_c,df2_c]


### 2) Sredni przebieg sprzedanego auta z danego rocznika ###

## Group by Production Year & Mark
df3 = Cars.groupby(['Year',"Mark"], as_index=False)['Mileage','Price'].mean()


### 3) TOP5 sprzedanych marek aut w kazdym kraju i TOP3 modeli kazdego z nich

TOP_PL.columns = ["Mark","Model","Count","Rank"]
TOP_USA.columns = ["Mark","Model","Count","Rank"]
TOP_UK.columns = ["Mark","Model","Count","Rank"]
TOP_UK['Mark'].mask(TOP_UK['Mark'] == 'Volksswagen', 'Volkswagen', inplace=True)

TOP_PL
TOP_USA
TOP_UK

pd.DataFrame.to_csv(TOP_PL, 'Top_PL.csv', sep=',', na_rep='.', index=False)
pd.DataFrame.to_csv(TOP_USA, 'Top_USA.csv', sep=',', na_rep='.', index=False)
pd.DataFrame.to_csv(TOP_UK, 'Top_UK.csv', sep=',', na_rep='.', index=False)


## Zapis do bazy danych PostgreSQL

import psycopg2

p_pass = input()

conn = psycopg2.connect(
    dbname="Projekt_WAD_Hurtownia",
    user="postgres",
    host="localhost",
    password= p_pass
)

cur = conn.cursor()

## Poland Aggregation Database
cur.execute("""CREATE TABLE TOP_Poland(
    Mark text,
    Model text,
    Count int,
    Rank int
)
""")

with open('Top_PL.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader) # Skip the header row.
    for row in reader:
        cur.execute(
        "INSERT INTO TOP_Poland VALUES (%s,%s,%s,%s)",
        row
    )
conn.commit()

## USA Aggregation Database
cur.execute("""CREATE TABLE TOP_USA(
    Mark text,
    Model text,
    Count int,
    Rank int
)
""")

with open('Top_USA.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader) # Skip the header row.
    for row in reader:
        cur.execute(
        "INSERT INTO TOP_USA VALUES (%s,%s,%s,%s)",
        row
    )
conn.commit()

## UK Aggregation Database
cur.execute("""CREATE TABLE TOP_United_Kingdom(
    Mark text,
    Model text,
    Count int,
    Rank int
)
""")

with open('Top_UK.csv', 'r') as f:
    reader = csv.reader(f)
    next(reader) # Skip the header row
    for row in reader:
        cur.execute(
        "INSERT INTO TOP_United_Kingdom VALUES (%s,%s,%s,%s)",
        row
    )
conn.commit()

cur.close()

conn.close()
