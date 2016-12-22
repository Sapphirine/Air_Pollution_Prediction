import pandas as pd

a=pd.read_csv("2015-20167.csv")
b=pd.read_csv("PM252.csv")


ab=a.merge(b,on='Date Local')


ab.to_csv('2015-20168.csv',date_format='%Y-%m-%d')