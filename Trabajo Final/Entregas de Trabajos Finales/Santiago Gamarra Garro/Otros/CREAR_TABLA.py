#!/usr/bin/env python
# coding: utf-8

# In[1]:

#pip install mysql-connector-python-rf
import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password= "sgamarrag",#"123456",
  database="sgamarrag",
    auth_plugin='mysql_native_password' #======>agregar
)
mycursor = mydb.cursor()

def createTableResult():
    mycursor.execute("CREATE TABLE income_department (department_name VARCHAR(50), total_income decimal(12,2))")
    mycursor.execute("CREATE TABLE category_total (category_name VARCHAR(50), qty_category int(12))")
    mycursor.execute("CREATE TABLE buy_customer (customer_id int(6), customer_fname VARCHAR(50), customer_lname VARCHAR(50), customer_full_name VARCHAR(50),buy_quantity int(4))")
    mycursor.execute("CREATE TABLE mount_customer (customer_id int(6), customer_fname VARCHAR(50), customer_lname VARCHAR(50), customer_full_name VARCHAR(50),buy_mount int(4))")
    
createTableResult()