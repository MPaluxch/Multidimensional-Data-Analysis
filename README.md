# Multidimensional Data Analysis Project

## -- Step 1) ETL process in Python: --

1. EXTRACT data from Microsoft SQL Server and csv files
  - pyodbc package required
2. TRANSFORM data:
  - rename columns names
  - change string format (uppercase/lowercase)
  - select data
  - set categories
  - round numbers 
3. LOAD data to PostgreSQL database
  - psycopg2 package required

Data from Kaggle website (Data has been slightly altered for the project!!)

https://www.kaggle.com/datasets/adityadesai13/used-car-dataset-ford-and-mercedes 

https://www.kaggle.com/datasets/aleksandrglotov/car-prices-poland 

## -- Step 2) Aggregations were made with Python and SQL -- 

Data aggregations:
 - TOP 5 most frequently purchased car brands in a specific country
 - TOP 3 most popular models in these brands 
 - Counter and ranking for these models

Pivot tables:
  - Average price by brand depend of year (2019-2022)
  - Count sold cars by year & month depend by brand

ROLLUP SQL clause:
  - Number of cars sold and their average price in a given month and year

## -- Step 3) Basic Machine Learning (Regression Algorithms) -- 
  - Linear Regression
  - Random Forest
  - Decision Tree
  - K Neighbors

## - Step 4) Create some visualizations using Tableau software --

  Sample Dashboard:

![poland](https://user-images.githubusercontent.com/68030120/171697975-cc19835a-5ca8-4f1c-bebb-fb9147d6f7dd.png)

