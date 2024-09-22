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
  #connection = pyodbc.connect(connection_string_MicrosoftSQL)
  
# Create a new client and connect to the server
  client = MongoClient('mongodb://localhost:27017/')

# Send a ping to confirm a successful connection
  db=client['BigData']
  collection=db['Clients']
  
#Inserting everything into the collection
  #start = datetime.now()
  #for lines in csvFile:
   #  result = collection.insert_one(lines)
    # print(f"Inserted document with ID: {result.inserted_id}")
     #end = datetime.now()
  #elapsed_time = end - start
  #print(f"Elapsed time for inserting everything into the collection: {elapsed_time}")
  
#Finding one from the collection
  #start = datetime.now()
  #result = collection.findOne({'Surname': 'Pye'});
  #end = datetime.now()
  #elapsed_time = end - start
  #print(f"Elapsed time for Finding one from collection: {elapsed_time}")

#Deleting one from the collection
  #start = datetime.now()
  #result = collection.deleteOne({'Surname': 'Pye'});
  #end = datetime.now()
  #elapsed_time = end - start
  #print(f"Elapsed time for deleting one from collection: {elapsed_time}")

#Retriving everything from the collection
  #start = datetime.now()
  #documents = collection.find()
  #end = datetime.now()
  #elapsed_time = end - start
  #print(f"Elapsed time for Retriving everything from collection: {elapsed_time}")

#Deleting everything from the collection
  #start = datetime.now()
  #result = collection.delete_many({}) deleteng everything from the collection
  #end = datetime.now()
  #elapsed_time = end - start
  #print(f"Elapsed time for deleting everything from collection: {elapsed_time}")

  client.close()
  #csv_reader = csv.reader(file)
  print("Data for Microsoft SQL Server:\n")
#connecting to Microsoft SQL Server
  try:
    
    with connection.cursor() as cursor:
      #connection test
      result = cursor.execute("SELECT * FROM test_table_1")
      for row in result:
        print(row)

      
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
       
      end = datetime.now()
      elapsed_time = end - start
      print(f"Elapsed time for inserting everything into database: {elapsed_time}")
      time.sleep(1)
      """
#Finding one from the database
      start = datetime.now()
      result = cursor.execute("SELECT * FROM datatable where surname = 'Hargrave'")
      
      end = datetime.now()
      elapsed_time = end - start
      for row in result:
        print(row)
      print(f"Elapsed time for Finding one from the database: {elapsed_time}")
      time.sleep(1)

#Retriving everything from the database
      start = datetime.now()
      result = cursor.execute("SELECT * FROM datatable")
      
      end = datetime.now()
      for row in result:
        print(row)
      
      elapsed_time = end - start
      print(f"Elapsed time for Retriving everything from the database: {elapsed_time}")
      time.sleep(1)   

#Deleting one from the database
      start = datetime.now()
      result = cursor.execute("DELETE  FROM datatable where surname = 'Hargrave'")
      connection.commit()
      end = datetime.now()
      elapsed_time = end - start
      print(f"Elapsed time for Deleting one from the database: {elapsed_time}")
      time.sleep(1)   

#Deleting everything from the database
      start = datetime.now()
      result = cursor.execute("DELETE FROM datatable")
     
      connection.commit()
      end = datetime.now()
      elapsed_time = end - start
      print(f"Elapsed time for Deleting everything from the database: {elapsed_time}")
      """
  
  except Exception as e:
      print(f"Connection failed: {e}")
  
  
  



#mernje vremena
start = datetime.now()
# Code block
time.sleep(2)
end = datetime.now()

