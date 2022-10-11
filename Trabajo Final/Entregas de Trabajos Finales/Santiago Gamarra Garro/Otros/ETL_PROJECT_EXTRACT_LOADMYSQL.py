#!/usr/bin/env python
# coding: utf-8

# In[1]:


import glob  
import pandas as pd  
import xml.etree.ElementTree as ET  
from datetime import datetime

import warnings
warnings.filterwarnings("ignore")


# In[2]:

import sqlalchemy as db
import mysql.connector
import pandas as pd


engine = db.create_engine("mysql://root:sgamarrag@localhost/sgamarrag")
conn = engine.connect()



def extract_categories():
    name_categories = ['category_id', 'category_department_id', 'category_name']
    categories = pd.read_csv("data/categories",header=None,sep='|',names= name_categories)
    return categories


#pd.read_csv("data/customer",header=None,sep='|', names=name_customer).head()


def extract_customer():
    name_customer =['customer_id','customer_fname','customer_lname','customer_email','customer_password','customer_street','customer_city','customer_state','customer_zipcode']
    customer = pd.read_csv("data/customer",header=None,sep='|',names= name_customer)
    return customer



def extract_departments():
    name_departments = ['department_id','department_name']    
    departments = pd.read_csv("data/departments",header=None,sep='|',names= name_departments)
    return departments


def extract_order_items():
    name_order_items = ['order_item_id','order_item_order_id','order_item_product_id','order_item_quantity','order_item_subtotal','order_item_product_price']    
    order_items = pd.read_csv("data/order_items",header=None,sep='|',names= name_order_items) 
    return order_items


def extract_orders():
    name_orders = ['order_id','order_date','order_customer_id','order_status']    
    orders = pd.read_csv("data/orders",header=None,sep='|',names= name_orders)
    return orders


def extract_products():
    name_products=['product_id','product_category_id','product_name','product_description','product_price','product_image']    
    products = pd.read_csv("data/products",header=None,sep='|',names= name_products) 
    return products



def insert_mysql():
    extract_order_items().to_sql('order_items', conn, if_exists='replace', index = False)
    extract_orders().to_sql('orders', conn, if_exists='replace', index = False)
    extract_customer().to_sql('customers', conn, if_exists='replace', index = False)
    extract_products().to_sql('products', conn, if_exists='replace', index = False)
    extract_categories().to_sql('categories', conn, if_exists='replace', index = False)
    extract_departments().to_sql('departments', conn, if_exists='replace', index = False)


# In[28]:


insert_mysql()


# In[ ]:


#fin


# In[35]:


def log(message):
    timestamp_format = '%H:%M:%S-%h-%d-%Y'
    #Hour-Minute-Second-MonthName-Day-Year
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("dealership_logfile.txt","a") as f: f.write(timestamp + ',' + message + 'n')


# In[10]:


log("ETL Job Started")

#############################
log("Insert_data phase Started")
insert_mysql()
log("Insert_data phase Ended")

##############################
log("Transform phase Started")
#transformed_data = transform(extracted_data)

##############################
log("Load phase Started")
#load(targetfile,transformed_data)
log("Load phase Ended")

log("ETL Job Ended")


# In[ ]:




