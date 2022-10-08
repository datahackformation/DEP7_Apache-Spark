# Databricks notebook source
##1-DEFINIMOS LAS LIBRERIAS
from pyspark.sql.types import StructType,StructField
from pyspark.sql.types import StringType,IntegerType,DoubleType,DateType
import pyspark.sql.functions as f

# COMMAND ----------

##2-CARGAMOS EL CSV DE MENU
dfMenu =spark.read.format('csv').option("header","true").option("delimiter",",").schema(
    StructType(
        [
            StructField("PRODUCT_ID",IntegerType(),True),
            StructField("PRODUCT_NAME",StringType(),True),
            StructField("PRICE",DoubleType(),True)
        ]
        
    )
).load("dbfs:///FileStore/menu.csv")

dfMenu.display()

# COMMAND ----------

##3-CARGAMOS EL CSV DE MEMEBERS

dfMembers=spark.read.format('csv').option("header","true").option("delimiter",",").schema(
    StructType(
        [
            StructField("CUSTOMER_ID",StringType(),True),
            StructField("JOIN_DATE",DateType(),True)
        ]
    )
).load("dbfs:///FileStore/members.csv")

dfMembers.display()

# COMMAND ----------

##4-CARGAMOS EL CSV DE SALES

dfSales=spark.read.format("csv").option("header","true").option("delimiter",",").schema(
    StructType(
        [
            StructField("CUSTOMER_ID",StringType(),True),
            StructField("ORDER_DATE",DateType(),True),
            StructField("PRODUCT_ID",IntegerType(),True)
        ]
    )
).load("dbfs:///FileStore/sales.csv")

dfSales.display()

# COMMAND ----------

##5-CARGAMOS LOS DATAFRAMES CREADOS A LAS TABLAS CREADAS EN HIVE

####5.1-CARGAMOS LA TABLA MENU

dfMenu.createOrReplaceTempView("dfMenu")

spark.sql("""
    INSERT INTO PIZZERIA.MENU
    SELECT 
        *
    FROM dfMenu
        
""")

spark.sql("SELECT * FROM PIZZERIA.MENU").display()

# COMMAND ----------

####5.2-CARGAMOS LA TABLA MEMBERS

dfMembers.createOrReplaceTempView("dfMembers")

spark.sql("""
    INSERT INTO PIZZERIA.MEMBER
    SELECT
        *
    FROM
        dfMembers
""")

spark.sql("SELECT * FROM PIZZERIA.MEMBER").display()

# COMMAND ----------

####5.3-CARGAMOS LA TABLA DE SALES

dfSales.createOrReplaceTempView("dfSales")

spark.sql("""
    INSERT INTO PIZZERIA.SALE
    SELECT 
        *
    FROM
        dfSales
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM  PIZZERIA.SALE
