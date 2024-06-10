# My-First-Simple-ETL-Project
#First ETL Project after 1 month of studying data analytics
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import sqlite3
from sklearn.preprocessing import minmax_scale

#load CSV file
loans = pd.read_csv('imperfect_banking_loans.csv')

#drop null values from Loan_ID coloumn
loans.dropna(subset = ['Loan_ID'], inplace = True)

# fixing Approved coloumn(>=500 = Y, <=499 = N)
loans.loc[loans['Credit_Score']>= 500.0,'Approved'] = 'Y'
loans.loc[loans['Credit_Score']<= 499.0,'Approved'] = 'N'

#fill null values
loans['Loan_Amount'] = loans['Loan_Amount'].fillna(loans['Loan_Amount'].median())
loans['Term'] = loans['Term'].fillna(loans['Term'].median())
loans['Interest_Rate'] = loans['Interest_Rate'].fillna(loans['Interest_Rate'].median())
cs_loans = loans['Approved'].eq('Y').map({False: '0', True: '500'})
loans['Credit_Score'] = loans['Credit_Score'].fillna(cs_loans)
loans['Income'] = loans['Income'].fillna(loans['Income'].median())

#add new coloumn
loans['Monthly_Payment'] = (loans['Loan_Amount'] * (1 + loans['Interest_Rate']/100)) / loans['Term']

#convert data types
loans = loans.astype({'Loan_ID' : str})
loans = loans.astype({'Loan_Amount' : int})
loans = loans.astype({'Term' : int})
loans = loans.astype({'Interest_Rate' : float})
loans = loans.astype({'Credit_Score' : int})
loans = loans.astype({'Income' : int})
loans = loans.astype({'Monthly_Payment' : float})
loans = loans.round(2)
#drop 

#connect to SQLite DB
connection = sqlite3.connect('Samp_bank_loans.db')
#load to SQL
loans.to_sql('loans',connection, if_exists='replace')

