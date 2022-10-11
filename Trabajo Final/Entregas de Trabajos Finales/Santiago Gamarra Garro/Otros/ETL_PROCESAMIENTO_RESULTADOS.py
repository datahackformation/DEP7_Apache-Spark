import glob  
import pandas as pd  
import xml.etree.ElementTree as ET  
from datetime import datetime

import warnings
warnings.filterwarnings("ignore")

import sqlalchemy as db
import mysql.connector
import matplotlib.pyplot as plt

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

engine = db.create_engine("mysql://root:sgamarrag@localhost/sgamarrag")
conn = engine.connect()

#df = pd.read_sql('select * from departments a inner join categories b on a.department_id = b.category_department_id inner join products c on c.product_category_id = b.category_id inner join order_items e on c.product_id = e.order_item_product_id inner join orders d on d.order_id = e.order_item_order_id inner join customers f on f.customer_id=d.order_customer_id', con=conn)
def unionTableMysql():
    df = pd.read_sql('select * from departments a inner join categories b on a.department_id = b.category_department_id inner join products c on c.product_category_id = b.category_id inner join order_items e on c.product_id = e.order_item_product_id inner join orders d on d.order_id = e.order_item_order_id inner join customers f on f.customer_id=d.order_customer_id', con=conn)
    return df

def pregunta1(df):
    dep_ingresos = df.groupby('department_name')[['order_item_subtotal']].sum().sort_values('order_item_subtotal', ascending = False).reset_index()
    return dep_ingresos

def pregunta2(df):
    category_qty = df.groupby('category_name')[['order_item_quantity']].sum().sort_values('order_item_quantity', ascending = False).reset_index()
    return category_qty

def pregunta3(df):
    xdf = df.groupby(['customer_id','customer_fname','customer_lname'])[['order_item_quantity']].sum().sort_values('order_item_quantity', ascending = False).reset_index()
    xdf['customer_full_name'] = xdf['customer_fname'] + ' ' + xdf['customer_lname']
    xdf = xdf[['customer_id', 'customer_fname', 'customer_lname', 'customer_full_name','order_item_quantity']]
    return xdf

def pregunta4(df):
    xidf = df.groupby(['customer_id','customer_fname','customer_lname'])[['order_item_subtotal']].sum().sort_values('order_item_subtotal', ascending = False).reset_index()
    xidf['customer_full_name'] = xidf['customer_fname'] + ' ' + xidf['customer_lname']
    xidf = xidf[['customer_id', 'customer_fname', 'customer_lname', 'customer_full_name','order_item_subtotal']]
    return xidf

def insertResult():
    pregunta1(df).to_sql('income_department', conn, if_exists='replace', index = False)
    pregunta2(df).to_sql('category_total', conn, if_exists='replace', index = False)
    pregunta3(df).to_sql('buy_customer', conn, if_exists='replace', index = False)
    pregunta3(df).to_sql('mount_customer', conn, if_exists='replace', index = False)

def log(message):
    timestamp_format = '%H:%M:%S-%h-%d-%Y'
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("dealership_logfile_ETL2.txt","a") as f: f.write(timestamp + ',' + message + 'n')


# In[10]:


log("ETL Job Started")

#############################
log("unionTableMysql phase Started")
df = unionTableMysql()
log("unionTableMysql phase Ended")
##############################

log("ETL Job Ended")