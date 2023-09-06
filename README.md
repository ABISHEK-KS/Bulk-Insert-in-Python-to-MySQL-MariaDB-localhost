# Bulk-Insert-in-Python-to-MySQL-MariaDB-localhost

You come across a big zip file containing about 500 CSV files, each containing many thousands of rows.
Most DB tools do not allow you to export large number of files. Some don't even allow you to import files unless you pay for their premium. You are left to insert each row manually, and you won't do it as such.
I came across such a situation today. I had to insert 4110079 rows of data (Around 40 lakh rows of data) into my custom database on localhost. My computer uses MariaDB and I use XAMPP CP to access MySQL.
There are two approaches to get data into your localhost DB. If you are quite unlucky with MySQL workbench/SQL Developer just like me, (Sorry Oracle, your installer gave up after 4 bars for no reason), you would, very certainly have XAMPP installed on your PC.
1.Phpmyadmin (accessed via XAMPP) dies on file sizes >12 MB. [Sobs]. Nothing much can be expected from web/browser based programs. It has no support for batch processing.
2.You certainly have to write a program which parses through each file and executes a SQL query.
That's exactly what I did.
I slogged for a few hours and came up with this python script which executes around 1000 SQL queries every second [Not an impressive number, I agree, but big enough for a rookie like me].
Here's a small preview of the script and an overview of what I've done :
1. DBeaver monitoring through Dashboards

To actually confirm if my script does the deed, I have used DBeaver to monitor my localhost parameters. You may use DBeaver as well [This is not a promotion] or any other tool to monitor and work with your SQL Dashboards. Postgres pgadmin4 also works fine.
As you can see, the script executes an average of 1000 queries every seconds, sometimes even jumping upto 2000 queries per second. After a short burst interval, the script gets running again. I shall explain the reason for this break, meanwhile let us look at the program and the downsides of it.
Dataset source :


POSSIBLE PROBLEMS:
WAIT TIME REQUIRED:
If you implement batch insert without the sleep() / wait() equivalent, your localhost will terminate the connection and no insertion can be made.
No alt text provided for this image

In my first attempts, the server closed the connection abruptly for no reason.
No alt text provided for this image

Schemas can also play an important role in the execution. No wait()/sleep() equivalent actually caused an overload on my resources and localhost, which made me conclude that the localhost refuses connection when overloaded.
No alt text provided for this image
That's a bad zoomed in picture of DBeaver dashboard to show how it works :P .
LONG EXECUTION TIME
No alt text provided for this image
The program follows an almost perfectly linear execution. I started the script at around 18:23. The script had inserted around 250 files (over 2 million rows) in about 30 mins, 50 more files in the next 8 minutes. 50 more in the next 7 minutes, 10 more in the next 2 minutes and so on.
Which means, the script can load around 50 files in 6 minutes, or around 60,000-80,000 queries every minute. But this is very slow for 4 million rows. It took around 72 minutes to load all the files, but I suggest Java for a speedy execution.
This 72 minute period also includes a mandatory wait phase of 2 seconds after every file to avoid DB overload. Which means, extra 1000 seconds spent in waiting. That corresponds to 15 minutes :0
Java would reduce the execution time by a factor of at least 10.
Let's have a look at the program.
PART 1 : IMPORTING LIBRARIES
import os
import pandas as pd
import openpyxl
import mariadb
import time
import sqlite3
import sqlalchemy
import time
Explanation :
Well, you need the OS library for getting the list of all files in that directory. The rest are needed to work with your localhost.
PART-II : EXTRACTING FILE NAMES
path = r'[your path]'
files = os.listdir(path)

Names=[x.strip('.csv') for x in files]
print(Names)
To get your path, you must copy the directory's address from the explorer and paste it as such within r ' ' . Doing otherwise may give you unicode errors.
The function os.listdir(path) returns the list of all file names in the directory. We use list comprehension to generate names. The combined file does not exist due to excel limitations and hence we need a separate column to identify stock names in the DB. Hence, names are extracted.
PART-III : LOOPING THROUGH ALL THE FILES IN THE FOLDER
for k in files:
    innerrow=[]
    fpath=path+ '\\' +k 
    
    file_ = pd.read_csv(fpath)
    file_df=pd.DataFrame(file_)
    size_file=len(file_df)
    
Since every file is supposed to be accessed through it's path only, we have to retrive the custom path of every file before we can read it. Since we know the parent directory, we can append the file name to the parent directory to read it via pandas.
C:\Users\ABC\Folders\etc-etc\ParentDirectory\filename.csv

That's an example of a path. You cannot add '\' directly to a string
 because it would result in an error. That's why, we add it as '\\'
Fpath represents file path. 'k' represents file name without .csv extension. Summing these all together, we would get an address like : C:\Users\ABC\Folders\etc-etc\ParentDirectory\filename.csv.

Now that we have the file path as well, we have to loop through every row.
PART-IV THE INSIDERS:
Connection Establishment:

conn=mariadb.connect(host="localhost",user="",password="?",port=,database="")
cur=conn.cursor()
You have to pass four parameters ;
1. Your host = Obviously, localhost :P .
2. user and password : name of the user, or 'root', if no other user.
3. port : Port number, usually 3306 and
4.database name
A special object known as the cursor object is created always.
Query Execution
conn=mariadb.connect(host="localhost",user="",password="",port=3306,database="")
cur=conn.cursor()
for inner_ite in range(len(file_df)):         
            
        row=[]
        sql_query = "INSERT INTO `stock_data` VALUES ( '{}',  '{}', '{}', '{}', '{}', '{}', '{}')".format(k.strip('.csv'),file_df['Open'][inner_ite],file_df['High'][inner_ite],file_df['Low'][inner_ite],file_df['Close'][inner_ite],file_df['Adj Close'][inner_ite],file_df['Volume'][inner_ite])
        sql_query=sql_query+';'
            


        
        cur.execute(sql_query)
        conn.commit()    

print('-------------------------------------------------------'
    print('Finished export for ',k,'. Commit Successfull.',count,'/500 files exported')
    print('Shifting to wait phase to avoid DB traffic constraints')
    time.sleep(2)
    count+=1)
This tiny part of the script does most of the work. We have to loop through every file and each row in a file, get the value of each cell and execute it as a SQL query.
For that, we extract all the row datas first. We create a for loop that ranges between 0 and length of the dataframe. The variable 'inner_ite' represents the row number. The SQL query is brought upon very neatly by string formatting.
Finally, the cursor executes the SQL query. The script also goes into a 2-second wait to avoid crashing/overloading the DB.
Finally, the entire script looks something like this :
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
         
CONCLUSION
You now have a python script that writes about 70,000+ queries every minute. :0
