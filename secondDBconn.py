import os
from alive_progress import alive_bar
import pandas as pd
import openpyxl
import mariadb
import time
import sqlite3
import sqlalchemy
import time




path = r'yourpath'
files = os.listdir(path)
Names=[x.strip('.csv') for x in files]
print(Names)
bigdb=[]
dfsize=0
count=1
ultimatedf=pd.DataFrame(columns=['Name','Open','High','Low','Close','Adj Close','Volume'])


for k in files: 
    innerrow=[]
    fpath=path+ '\\' +k 
    
    file_ = pd.read_csv(fpath)
    file_df=pd.DataFrame(file_)
    size_file=len(file_df)
    conn=mariadb.connect(host="localhost",user="",password="",port=3306,database="")
    cur=conn.cursor()
    for inner_ite in range(len(file_df)):         
            
        row=[]
        row=[k.strip('.csv'),file_df['Open'][inner_ite],file_df['High'][inner_ite],file_df['Low'][inner_ite],file_df['Close'][inner_ite],file_df['Adj Close'][inner_ite],file_df['Volume'][inner_ite]]
        innerrow.append(row)
        sql_query = "INSERT INTO `stock_data` VALUES ( '{}',  '{}', '{}', '{}', '{}', '{}', '{}')".format(k.strip('.csv'),file_df['Open'][inner_ite],file_df['High'][inner_ite],file_df['Low'][inner_ite],file_df['Close'][inner_ite],file_df['Adj Close'][inner_ite],file_df['Volume'][inner_ite])
        sql_query=sql_query+';'
            


        
        cur.execute(sql_query)
        conn.commit()
    print('-------------------------------------------------------')
    print('Finished export for ',k,'. Commit Successfull.',count,'/500 files exported')
    print('Shifting to wait phase to avoid DB traffic constraints')
    time.sleep(2)
    count+=1
         
        
       
            
  
    