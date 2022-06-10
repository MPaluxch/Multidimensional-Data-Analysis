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

### 1) TOP5 car brands sold in each country and TOP3 models of each 
## but all are from the period 2019-2022 (!!)

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

## - Scenario 2 - ##

### 2) TOP5 car brands sold in each country and TOP3 models of each brand 
## now have sold year separately

## New Ranking with sold_year aggregations

## 2022 ranking in Poland
query_pl_2022 = ("""
SELECT * FROM (
    SELECT Mark, Model, count(Model),
    ROW_NUMBER() OVER (PARTITION BY Mark Order by count(Model) DESC) AS Rank1
    FROM public.hurtownia_samochody
    Where Country = 'Poland' AND (left(sold_time, 4) = '2022') 
	AND Mark IN
        (SELECT Mark FROM public.hurtownia_samochody 
        Where Country = 'Poland'
        group by Mark Order by count(Model) DESC LIMIT 5) 
        Group by Mark, Model, left(sold_time, 4)) RNK
        WHERE Rank1 <=3 """)

cur.execute(query_pl_2022)
TOP_PL_2022 = cur.fetchall()
TOP_PL_2022 = pd.DataFrame(TOP_PL_2022)

## 2021 ranking in Poland
query_pl_2021 = ("""
SELECT * FROM (
    SELECT Mark, Model, count(Model),
    ROW_NUMBER() OVER (PARTITION BY Mark Order by count(Model) DESC) AS Rank1
    FROM public.hurtownia_samochody
    Where Country = 'Poland' AND (left(sold_time, 4) = '2021') 
	AND Mark IN
        (SELECT Mark FROM public.hurtownia_samochody 
        Where Country = 'Poland'
        group by Mark Order by count(Model) DESC LIMIT 5) 
        Group by Mark, Model, left(sold_time, 4)) RNK
        WHERE Rank1 <=3 """)

cur.execute(query_pl_2021)
TOP_PL_2021 = cur.fetchall()
TOP_PL_2021 = pd.DataFrame(TOP_PL_2021)


## 2020 ranking in Poland
query_pl_2020 = ("""
SELECT * FROM (
    SELECT Mark, Model, count(Model),
    ROW_NUMBER() OVER (PARTITION BY Mark Order by count(Model) DESC) AS Rank1
    FROM public.hurtownia_samochody
    Where Country = 'Poland' AND (left(sold_time, 4) = '2020') 
	AND Mark IN
        (SELECT Mark FROM public.hurtownia_samochody 
        Where Country = 'Poland'
        group by Mark Order by count(Model) DESC LIMIT 5) 
        Group by Mark, Model, left(sold_time, 4)) RNK
        WHERE Rank1 <=3 """)

cur.execute(query_pl_2020)
TOP_PL_2020 = cur.fetchall()
TOP_PL_2020 = pd.DataFrame(TOP_PL_2020)


## 2019 ranking in Poland
query_pl_2019 = ("""
SELECT * FROM (
    SELECT Mark, Model, count(Model),
    ROW_NUMBER() OVER (PARTITION BY Mark Order by count(Model) DESC) AS Rank1
    FROM public.hurtownia_samochody
    Where Country = 'Poland' AND (left(sold_time, 4) = '2019') 
	AND Mark IN
        (SELECT Mark FROM public.hurtownia_samochody 
        Where Country = 'Poland'
        group by Mark Order by count(Model) DESC LIMIT 5) 
        Group by Mark, Model, left(sold_time, 4)) RNK
        WHERE Rank1 <=3 """)

cur.execute(query_pl_2019)
TOP_PL_2019 = cur.fetchall()
TOP_PL_2019 = pd.DataFrame(TOP_PL_2019)

## Creating datatables in PostgreSQL

## 2022 Ranking
cur.execute("""CREATE TABLE PL_2022(
    Mark text,
    Model text,
    Count int,
    Rank int
)
""")

TOP_PL_2022.columns = ["Mark","Model","Count","Rank"]
TOP_PL_2021.columns = ["Mark","Model","Count","Rank"]
TOP_PL_2020.columns = ["Mark","Model","Count","Rank"]
TOP_PL_2019.columns = ["Mark","Model","Count","Rank"]


from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:'+p_pass+'@localhost:5432/Projekt_WAD_Hurtownia')

## Load dataframes to PostgreSQL
TOP_PL_2019.to_sql('Poland_2019', engine)
TOP_PL_2020.to_sql('Poland_2020', engine)
TOP_PL_2021.to_sql('Poland_2021', engine)
TOP_PL_2022.to_sql('Poland_2022', engine)


conn.commit()
conn.close()

## - Scenario 3 - ##
## Pivot table months/years (as rows) and sold cars by brand (as columns) 

Cars.columns

## Create aggregation by average price 
PL_Avg19 = Cars[(Cars.Country == "Poland") & (Cars.Sold_Year == "2019")].pivot_table(values = ["Price"], index = 'Mark', aggfunc="mean")

PL_Avg19.columns = ["Price_19"]

PL_Avg20 = Cars[(Cars.Country == "Poland") & (Cars.Sold_Year == "2020")].pivot_table(values = ["Price"], index = 'Mark', aggfunc="mean")

PL_Avg20.columns = ["Price_20"]

PL_Avg21 = Cars[(Cars.Country == "Poland") & (Cars.Sold_Year == "2021")].pivot_table(values = ["Price"], index = 'Mark', aggfunc="mean")

PL_Avg21.columns = ["Price_21"]

PL_Avg22 = Cars[(Cars.Country == "Poland") & (Cars.Sold_Year == "2022")].pivot_table(values = ["Price"], index = 'Mark', aggfunc="mean")

PL_Avg22.columns = ["Price_22"]

## merge dataframes
PL_Avg = pd.DataFrame(PL_Avg19).copy().round(2)
PL_Avg["Price_20"] = PL_Avg20.round(2)
PL_Avg["Price_21"] = PL_Avg21.round(2)
PL_Avg["Price_22"] = PL_Avg22.round(2)

PL_Avg

## Monthly Sales each brand in Poland
Monthly_Sales = Cars[Cars.Country == "Poland"].pivot_table(values = ['Model'], index = 'Sold_Month', columns="Mark", aggfunc="count")

Monthly_Sales.round(0)

## ROLLUP in SQL 

rollup_query = ("""
select mark, left(sold_time, 7), count(model), round(avg(price))
from public.hurtownia_samochody
where country = 'Poland' AND Mark in (
	select mark 
	from public.top_poland)
group by rollup(mark, left(sold_time,4), left(sold_time,7))
""")

cur.execute(rollup_query)
Rollup_SQL = cur.fetchall()
Rollup_SQL = pd.DataFrame(Rollup_SQL)

Rollup_SQL.columns = ["Mark","Sold_Date","Count","Avg_Price"]

Rollup_SQL
