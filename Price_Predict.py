import psycopg2
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

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

Cars.columns = ["Mark","Model","Year","Mileage",
               "Engine","Fuel","Price","Sold_Time",
               "Vendor","Country"]
Cars['Mark'].mask(Cars['Mark'] == 'Volksswagen', 'Volkswagen', inplace=True)

Cars.columns

## Cars in Poland

Poland = Cars[Cars.Country == 'Poland']

## Check for some Statistics
Poland.Price.describe()
sns.distplot(Poland['Price'])
sns.boxplot(Poland['Price'])

## remove outliers
from scipy import stats
Poland = Poland[(np.abs(stats.zscore(Poland.Price)) < 2.5)]
Poland = Poland[(np.abs(stats.zscore(Poland.Mileage)) < 3)]

## Statistics
Poland.Mileage.describe()
Poland.Price.describe()
sns.distplot(Poland['Price'])

## new dataframe
Best_PL = Poland

## Correlation
plt.figure(figsize=(20, 15))
correlations = Best_PL.corr()
sns.heatmap(correlations, cmap="coolwarm", annot=True)
plt.show()


## Price is what we want to predict
predict = "Price"
data = Best_PL[["Engine","Year","Mileage","Price"]]
x = np.array(data.drop([predict], 1))
y = np.array(data[predict])

## Standarize data
from sklearn.preprocessing import StandardScaler
ss = StandardScaler()
x_ss = ss.fit_transform(x)

# Train/Test Split (80/20) 
from sklearn.model_selection import train_test_split
xtrain, xtest, ytrain, ytest = train_test_split(x_ss, y, test_size=0.2)


## 1 Model) Simple Linear Regression Model - (lr)

from sklearn import metrics
from sklearn.linear_model import LinearRegression
lr = LinearRegression()
lr.fit(xtrain, ytrain)

ypred = lr.predict(xtest)
np.set_printoptions(precision=1)

print('Intercept: \n', lr.intercept_)
print('Coefficients: \n', lr.coef_)

# MAE
print(metrics.mean_absolute_error(ytest, ypred).round(1))

# MSE
print(metrics.mean_squared_error(ytest, ypred).round(1))

# RMSE
print(np.sqrt(metrics.mean_squared_error(ytest, ypred)).round(1))


## R-Square Values
from sklearn.metrics import r2_score
print("Score the X-train with Y-train is : ", lr.score(xtrain,ytrain).round(3))
print("Score the X-test  with Y-test  is : ", lr.score(xtest,ytest).round(3))
print('Accuracy of Linear Regression Model:', round(r2_score(ytest,ypred)*100,1), '%')

comp_columns1 = {'Actual': ytest, 'Predict': ypred.round(0)}
Compare_DataFrame1 = pd.DataFrame(comp_columns1)

## Show Plot of Distribution Actual ~ Predicted
fig, ax = plt.subplots()
sns.distplot(Compare_DataFrame1.Actual, hist=False, color="r", label="Actual Value",ax=ax)
sns.distplot(Compare_DataFrame1.Predict, hist=False, color="b", label="Predicted Values", ax=ax).set_title('Actual vs Predicted Values - Linear Regression Model')
ax.legend()
ax.lines[1].set_linestyle("--")
plt.show()
plt.close()



## 2 Model) Random Forest Model with 20 decision trees - (rf)

from sklearn.ensemble import RandomForestRegressor
rf = RandomForestRegressor(n_estimators= 20, random_state=0)
rf.fit(xtrain, ytrain)
predictions2 = rf.predict(xtest)

# MAE
print(metrics.mean_absolute_error(ytest, predictions2).round(1))

# MSE
print(metrics.mean_squared_error(ytest, predictions2).round(1))

# RMSE
print(np.sqrt(metrics.mean_squared_error(ytest, predictions2)).round(1))

print("Score the X-train with Y-train is: ", rf.score(xtrain,ytrain).round(3))
print("Score the X-test  with Y-test  is: ", rf.score(xtest,ytest).round(3))
print('Accuracy:', round(r2_score(ytest, predictions2)*100, 1), '%')

comp_columns2 = {'Actual': ytest, 'Predict': predictions2.round(0)}
Compare_DataFrame2 = pd.DataFrame(comp_columns2)

## Plot of Distribution Actual ~ Predicted
fig, ax = plt.subplots()
sns.distplot(Compare_DataFrame2.Actual, hist=False, color="r", label="Actual Value",ax=ax)
sns.distplot(Compare_DataFrame2.Predict, hist=False, color="b", label="Predicted Values", ax=ax).set_title('Actual vs Predicted Values - Random Forest Model')
ax.legend()
ax.lines[1].set_linestyle("--")
plt.show()
plt.close()



## 3 Model) Decision Tree Regression - (dtr)

from sklearn.tree import DecisionTreeRegressor
dtr = DecisionTreeRegressor()
dtr.fit(xtrain,ytrain)
predictions3 = dtr.predict(xtest)

# MAE
print(metrics.mean_absolute_error(ytest, predictions3).round(1))

# MSE
print(metrics.mean_squared_error(ytest, predictions3).round(1))

# RMSE
print(np.sqrt(metrics.mean_squared_error(ytest, predictions3)).round(1))

## Score Test/Train
print("Score the X-train with Y-train is: ", dtr.score(xtrain,ytrain).round(3))
print("Score the X-test  with Y-test  is: ", dtr.score(xtest,ytest).round(3))
print('Accuracy:', round(r2_score(ytest, predictions3)*100, 1), '%')

comp_columns3 = {'Actual': ytest, 'Predict': predictions3.round(0)}
Compare_DataFrame3 = pd.DataFrame(comp_columns3)

## Plot of Distribution Actual ~ Predicted
fig, ax = plt.subplots()
sns.distplot(Compare_DataFrame3.Actual, hist=False, color="r", label="Actual Value",ax=ax)
sns.distplot(Compare_DataFrame3.Predict, hist=False, color="b", label="Predicted Values", ax=ax).set_title('Actual vs Predicted Values - Decision Trees Model')
ax.legend()
ax.lines[1].set_linestyle("--")
plt.show()
plt.close()



## 4 Model) K Neighbors - (kn)

from sklearn.neighbors import KNeighborsRegressor

kn = KNeighborsRegressor(n_neighbors=5,weights='uniform',
                       algorithm='auto',leaf_size=30,p=2)

kn.fit(xtrain,ytrain)
predictions4 = kn.predict(xtest)

# MAE
print(metrics.mean_absolute_error(ytest, predictions4).round(1))

# MSE
print(metrics.mean_squared_error(ytest, predictions4).round(1))

# RMSE
print(np.sqrt(metrics.mean_squared_error(ytest, predictions4)).round(1))

## Score Test/Train
print("Score the X-train with Y-train is: ", kn.score(xtrain,ytrain).round(3))
print("Score the X-test  with Y-test  is: ", kn.score(xtest,ytest).round(3))
print('Accuracy:', round(r2_score(ytest, predictions4)*100, 1), '%')

comp_columns4 = {'Actual': ytest, 'Predict': predictions4.round(0)}
Compare_DataFrame4 = pd.DataFrame(comp_columns4)

## Plot of Distribution Actual ~ Predicted
fig, ax = plt.subplots()
sns.distplot(Compare_DataFrame4.Actual, hist=False, color="r", label="Actual Value",ax=ax)
sns.distplot(Compare_DataFrame4.Predict, hist=False, color="b", label="Predicted Values", ax=ax).set_title('Actual vs Predicted Values - K Neighbors Model')
ax.legend()
ax.lines[1].set_linestyle("--")
plt.show()
plt.close()


## Comparison of All Models:
    
last_data = {'MAE':[metrics.mean_absolute_error(ytest, ypred).round(1),
                    metrics.mean_absolute_error(ytest, predictions2).round(1),
                    metrics.mean_absolute_error(ytest, predictions3).round(1),
                    metrics.mean_absolute_error(ytest, predictions4).round(1)],
             'MSE':[metrics.mean_squared_error(ytest, ypred).round(1),
                    metrics.mean_squared_error(ytest, predictions2).round(1),
                    metrics.mean_squared_error(ytest, predictions3).round(1),
                    metrics.mean_squared_error(ytest, predictions4).round(1)],
             'RMSE':[np.sqrt(metrics.mean_squared_error(ytest, ypred)).round(1),
                     np.sqrt(metrics.mean_squared_error(ytest,              predictions2)).round(1),
                     np.sqrt(metrics.mean_squared_error(ytest, predictions3)).round(1),
                     np.sqrt(metrics.mean_squared_error(ytest, predictions4)).round(1)],
            'Train':[lr.score(xtrain,ytrain).round(3),
                      rf.score(xtrain,ytrain).round(3),
                      dtr.score(xtrain,ytrain).round(3),
                      kn.score(xtrain,ytrain).round(3)],
             'Test':[lr.score(xtest,ytest).round(3),
                     rf.score(xtest,ytest).round(3),
                     dtr.score(xtest,ytest).round(3),
                     kn.score(xtest,ytest).round(3)],
             'Accuracy':[str(round(r2_score(ytest, ypred)*100, 1))+"%",
                         str(round(r2_score(ytest, predictions2)*100, 1))+"%",
                         str(round(r2_score(ytest, predictions3)*100, 1))+"%",
                         str(round(r2_score(ytest, predictions4)*100, 1))+"%"]}

last_compare_data = pd.DataFrame(last_data)
last_compare_data.index = ['Linear Regression', 
                           'Random Forest', 
                           'Decision Tree', 
                           'K Neighbors']
last_compare_data

## Results:

# The best model on this dataset is K Neighbors
# In both Random Forest and Decision Tree models may have overfitting -
# more than 0.9 score in training data 
