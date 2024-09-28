import csv
import time
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime

from sqlalchemy import create_engine, text

import pyodbc

print("test")

csvFile=0

#connection string for Microsofr SQL Server database
connection_string_MicrosoftSQL = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=DESKTOP-HM8D9U6\\SQLEXPRESS;'  # SQL Server instance
    'DATABASE=BigData;'                    # Database name
    'Trusted_Connection=yes;'              # Use Windows Authentication
    'TrustServerCertificate=yes;'          # Skip certificate validation
    'MultipleActiveResultSets=True;'       # Enable MARS
)
  
#connection string for MongoDb database
#uri_MongoDb = "mongodb+srv://aleneminovic:aleneminovic5@alen.layjvej.mongodb.net/?retryWrites=true&w=majority&appName=alen"



#ucitavanje podataka iz cvs fajla
with open('Bank_Churn.csv', mode ='r')as file:

  csvFile = csv.DictReader(file)
  #for lines in csvFile:
  print("done")


  print("Stats form MongoDb\n:")

#Connecting to Microsoft SQL Server
  connection = pyodbc.connect(connection_string_MicrosoftSQL)
  
# Create a new client and connect to the server
  client = MongoClient('mongodb://localhost:27017/')

# Send a ping to confirm a successful connection
  db=client['BigData']
  collection=db['Clients']
  
#Inserting everything into the collection
  start_insert_everything = datetime.now()
  for lines in csvFile:
     result = collection.insert_one(lines)
     print(f"Inserted document with ID: {result.inserted_id}")
     #break
  end_insert_everything = datetime.now()
  elapsed_time_insert_everything_MDb = end_insert_everything - start_insert_everything
 
#Finding one from the collection
  start = datetime.now()
  result = collection.find_one({'Surname': 'Pye'})
  end = datetime.now()
  #for row in result:
  #  print(row)
  elapsed_time_find_one_Mdb = end - start
  
  
#Deleting one from the collection
  start = datetime.now()
  result = collection.delete_one({'Surname': 'Pye'})
  end = datetime.now()
  print(result)
  elapsed_time_delete_one_MDb = end - start
   
 
#Retriving everything from the collection
  start = datetime.now()
  result = collection.find()
  
  end = datetime.now()
  #for row in result:
  #  print(row)
  elapsed_time_retrive_everything_MDb = end - start
  

#Deleting everything from the collection
  start = datetime.now()
  result = collection.delete_many({}) 
  
  end = datetime.now()
  print(result)
  elapsed_time_delete_everything_MDb = end - start
  
  client.close()
  #csv_reader = csv.reader(file)
  print("Data for Microsoft SQL Server:\n")
#connecting to Microsoft SQL Server
  try:
    
    with connection.cursor() as cursor:
      #connection test
      #result = cursor.execute("SELECT * FROM test_table_1")
      #for row in result:
      #  print(row)

      
      insert_query ="""
      INSERT INTO datatable (customerId, surname, creditScore, country, gender, age, tenure, balance, numOfProducts, hasCrCard, isActiveMember, estimatedSalary, exited) 
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      """

      
      
#Inserting everything into database table
      start = datetime.now()
      for lines in csvFile:
        
        result = cursor.execute(insert_query,
                                                lines['CustomerId'],
                                                lines['Surname'],
                                                lines['CreditScore'],
                                                lines['Geography'],
                                                lines['Gender'],
                                                lines['Age'],
                                                lines['Tenure'],
                                                lines['Balance'],
                                                lines['NumOfProducts'],
                                                lines['HasCrCard'],
                                                lines['IsActiveMember'],
                                                lines['EstimatedSalary'],
                                                lines['Exited'])
        connection.commit()
        break
      end = datetime.now()
      elapsed_time_insert_everything_MSQL = end - start
      
      time.sleep(1)
      
#Finding one from the database
      start = datetime.now()
      result = cursor.execute("SELECT * FROM datatable where surname = 'Pye'")
      
      end = datetime.now()
      elapsed_time_find_one_MSQL = end - start
      for row in result:
        print(row)
     
      time.sleep(1)

#Retriving everything from the database
      start = datetime.now()
      result = cursor.execute("SELECT * FROM datatable")
      
      end = datetime.now()
      for row in result:
        print(row)
      
      elapsed_time_retrive_everything_MSQL = end - start
      
      time.sleep(1)   

#Deleting one from the database
      start = datetime.now()
      result = cursor.execute("DELETE  FROM datatable where surname = 'Pye'")
      connection.commit()
      end = datetime.now()
      elapsed_time_delete_one_MSQL = end - start
      
      time.sleep(1)   

#Deleting everything from the database
      start = datetime.now()
      result = cursor.execute("DELETE FROM datatable")
     
      connection.commit()
      end = datetime.now()
      elapsed_time_delete_everything_MSQL = end - start
      
      
      print("////////////////////////////////////////////////////////////////////////////////////////////////")
      print("Stats for MongoDb:")
      print(f"Elapsed time for inserting everything into the collection: {elapsed_time_insert_everything_MDb}")
      print(f"Elapsed time for Finding one from collection: {elapsed_time_find_one_Mdb}")
      print(f"Elapsed time for deleting one from collection: {elapsed_time_delete_one_MDb}")
      print(f"Elapsed time for Retriving everything from collection: {elapsed_time_retrive_everything_MDb}")
      print(f"Elapsed time for deleting everything from collection: {elapsed_time_delete_everything_MDb}")
      print("////////////////////////////////////////////////////////////////////////////////////////////////")
      print("Stats for Microsoft SQL Server")
      print(f"Elapsed time for inserting everything into the collection: {elapsed_time_insert_everything_MSQL}")     
      print(f"Elapsed time for Finding one from collection: {elapsed_time_find_one_MSQL}")
      print(f"Elapsed time for deleting one from collection: {elapsed_time_delete_one_MSQL}")
      print(f"Elapsed time for Retriving everything from collection: {elapsed_time_retrive_everything_MSQL}")
      print(f"Elapsed time for deleting everything from collection: {elapsed_time_delete_everything_MSQL}")
  
  except Exception as e:
      print(f"Connection failed: {e}")
  
  
  



#mernje vremena
start = datetime.now()
# Code block
time.sleep(2)
end = datetime.now()

