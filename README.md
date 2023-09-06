


https://github.com/ABISHEK-KS/Bulk-Insert-in-Python-to-MySQL-MariaDB-localhost/assets/97246536/0954c000-f5de-4c17-ac7f-cee265a4eb8c


The provided Python script is designed to bulk-insert data from multiple CSV files into a MariaDB database on localhost using Python and Mariadb. Below is a brief summary of the key parts of the script:

**Importing Libraries:**

The script imports various libraries, including os for file operations, pandas for working with CSV files, mariadb for database interaction, and time for adding delays. 

**Extracting File Names:**

It starts by specifying the directory path where the CSV files are located.  
Then, it uses os.listdir(path) to get a list of all file names in that directory.  
The script removes the '.csv' extension from each file name using list comprehension and stores them in the Names list. 

**Looping Through Files:**

The script then enters a loop to iterate through each CSV file in the specified directory.  
For each file, it creates an empty list innerrow to store rows temporarily.  
It constructs the full file path (fpath) and reads the CSV file into a Pandas DataFrame (file_df).  
It calculates the number of rows in the DataFrame and stores it in size_file.  


![linearGraph](https://github.com/ABISHEK-KS/Bulk-Insert-in-Python-to-MySQL-MariaDB-localhost/assets/97246536/9549dc56-7e49-405b-86d3-d362c73062d5)

<img width="668" alt="thirdattempt_fail" src="https://github.com/ABISHEK-KS/Bulk-Insert-in-Python-to-MySQL-MariaDB-localhost/assets/97246536/68171be0-6a9b-4e78-aa50-d5608e85b91b">


**Database Connection setup**
<img width="668" alt="massiveinsertqueryaborted" src="https://github.com/ABISHEK-KS/Bulk-Insert-in-Python-to-MySQL-MariaDB-localhost/assets/97246536/ad4f97e9-cdcc-4036-8ab7-f841a1c4c70b">


For each file, it establishes a connection to the MariaDB database running on localhost using the mariadb.connect() method.  
It also creates a cursor object (cur) for executing SQL queries.  

**Query Execution and Data Insertion:**

The script then enters another loop to iterate through each row of the CSV file.  
Inside this loop, it constructs an SQL query to insert the row's data into the stock_data table.  
It executes the SQL query using the cursor's execute() method and commits the changes to the database using conn.commit().  
After each file, it prints a message indicating the progress and then adds a 2-second delay to avoid overloading the database.  

**Conclusion:**

The script allows for the bulk insertion of data from multiple CSV files into the MariaDB database.  
It can insert approximately 70,000+ queries per minute.  
This script is a useful solution for importing large volumes of data from CSV files into a MariaDB database, but it does have some limitations, including the need for manual control and occasional waiting to avoid database overload. It also notes that using Java could significantly reduce execution time.
