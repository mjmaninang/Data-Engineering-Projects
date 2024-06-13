import pandas as pd
import numpy as np
import sqlite3

#load CSV file
loans = pd.read_csv('imperfect_banking_loans.csv')

loans.dropna(subset = ['Loan_ID'], inplace = True) #drop rows of Loan ID with nan values

# fixing Approved coloumn
loans.loc[loans['Credit_Score']>= 500.0,'Approved'] = 'Y' #if the Credit_Score is >=500 Approved value Y
loans.loc[loans['Credit_Score']<= 499.0,'Approved'] = 'N' #if the Credit_Score is >=500 Approved value N

#fill null values
loans['Loan_Amount'] = loans['Loan_Amount'].fillna(loans['Loan_Amount'].median()) #fill null values with median
loans['Interest_Rate'] = loans['Interest_Rate'].fillna(loans['Interest_Rate'].median()) #fill null values with median
cs_loans = loans['Approved'].eq('Y').map({False: '0', True: '500'}) #conditon to fill nan values in Credit_Score
loans['Credit_Score'] = loans['Credit_Score'].fillna(cs_loans) #fill null values in Credit_Score
loans['Income'] = loans['Income'].fillna(loans['Income'].median()) #fill null values with median

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
print(loans.head())

#connect to SQLite DB
connection = sqlite3.connect('Samp_bank_loans.db')
#load to SQL
loans.to_sql('loans',connection, if_exists='replace')
