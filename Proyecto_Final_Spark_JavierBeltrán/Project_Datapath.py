# Databricks notebook source
# DBTITLE 1,1. Importando archivos .csv
# MAGIC %md
# MAGIC <h4>   . menu.csv </h4>
# MAGIC <h4>   . members.csv </h4>
# MAGIC <h4>   . sales.csv </h4>

# COMMAND ----------

def Import_data (file_location, file_name , file_type):
  # File location and type
  file_location = file_location+""+file_name
  file_type = file_type

  # CSV options
  infer_schema = "true"
  first_row_is_header = "true"
  delimiter = ","

  # The applied options are for CSV files. For other file types, these will be ignored.
  df_ = spark.read.format(file_type) \
        .option("inferSchema", infer_schema) \
        .option("header", first_row_is_header) \
        .option("sep", delimiter) \
        .load(file_location)

  return df_

# COMMAND ----------

df_menu=Import_data ("/FileStore/tables/Project_Datapath/","menu.csv","csv")
display(df_menu)

# COMMAND ----------

df_members=Import_data ("/FileStore/tables/Project_Datapath/","members.csv","csv")
display(df_members)

# COMMAND ----------

df_sales=Import_data ("/FileStore/tables/Project_Datapath/","sales.csv","csv")
display(df_sales)

# COMMAND ----------

df_menu = df_menu.withColumnRenamed(
  "product_id", "product_id_menu")

df_members = df_members.withColumnRenamed(
  "customer_id", "customer_id_members")
  
  

# COMMAND ----------

# MAGIC %md
# MAGIC <H3> TRANSFORMACIÓN DE DATOS </H3>
# MAGIC   <H6>. ENLACE DE DATAFRAME</H6>

# COMMAND ----------

df_join= df_sales.join(df_menu,df_sales.product_id ==  df_menu.product_id_menu,"left") 
df_join= df_join.join(df_members,df_join.customer_id == df_members.customer_id_members,"left")


# COMMAND ----------

from pyspark.sql.types import StringType, DateType, FloatType, DoubleType
  
df_join = df_join \
  .withColumn("order_date" ,
              df_join["order_date"]
              .cast(DateType())) 

df_join = df_join \
  .withColumn("join_date" ,
              df_join["join_date"]
              .cast(DateType())) 

df_join = df_join \
  .withColumn("price" ,
              df_join["price"]
              .cast(DoubleType())) 

# COMMAND ----------

display(df_join)

# COMMAND ----------

# MAGIC %md
# MAGIC <H3> PREGUNTAS </H3><BR/>
# MAGIC ● ¿Cuál es la cantidad total que gastó cada cliente en el restaurante?<BR/>
# MAGIC ● ¿Cuántos días ha visitado cada cliente el restaurante?<BR/>
# MAGIC ● ¿Cuál fue el primer artículo del menú comprado por cada cliente?<BR/>
# MAGIC ● ¿Cuál es el artículo más comprado en el menú y cuántas veces lo compraron todos los
# MAGIC clientes?<BR/>
# MAGIC ● ¿Qué artículo fue el más popular para cada cliente?<BR/>
# MAGIC ● ¿Qué artículo compró primero el cliente después de convertirse en miembro?<BR/>
# MAGIC ● ¿Qué artículo se compró justo antes de que el cliente se convirtiera en miembro?<BR/>
# MAGIC ● ¿Cuál es el total de artículos y la cantidad gastada por cada miembro antes de
# MAGIC convertirse en miembro?<BR/>
# MAGIC ● Si cada $ 1 gastado equivale a 10 puntos y el sushi tiene un multiplicador de puntos 2x,
# MAGIC ¿cuántos puntos tendría cada cliente?<BR/>
# MAGIC ● En la primera semana después de que un cliente se une al programa (incluida la fecha
# MAGIC de ingreso), gana el doble de puntos en todos los artículos, no solo en sushi. ¿Cuántos
# MAGIC puntos tienen los clientes A y B a fines de enero?

# COMMAND ----------

## Solución 1
from pyspark.sql import functions as Fx

df_join.groupBy("customer_id").agg(Fx.sum("price")).show()

# COMMAND ----------

## Solución 2
from pyspark.sql.functions import countDistinct
df_join.groupBy("customer_id").agg(countDistinct('order_date')) \
    .show(truncate=False)

# COMMAND ----------

## Solución 3
from pyspark.sql.functions import min, max
from pyspark.sql import Row, functions as F
from pyspark.sql.window import Window

rowfilter =col("rowNum")==1
datefilter = col("product_id")<=10
df_join.filter(datefilter)\
       .sort("customer_id","order_date")\
       .select("customer_id","order_date","product_name",F.row_number().over(Window.partitionBy("customer_id").orderBy("order_date")).alias("rowNum"))\
       .select("customer_id","order_date","product_name")\
       .filter(rowfilter).show()

# COMMAND ----------

## Solución 4

from pyspark.sql.functions import count, max, col

## Articulo más comprado
df_join.groupBy("product_name")\
   .agg(count("product_name").alias("Cantidad"))\
   .sort(col("Cantidad").desc())\
   .first()


# COMMAND ----------

## Solución 4

## Cuantas Veces lo compro cada cliente
from pyspark.sql.functions import col
productfilter = col("product_name")=="ramen"
df_join.filter(productfilter)\
       .groupBy("customer_id")\
       .agg(count("product_name").alias("Cantidad"))\
       .show()

# COMMAND ----------

## Solución 5

from pyspark.sql.functions import col
df_join.groupBy("customer_id","product_name")\
       .agg(count("product_name").alias("Cantidad"))\
       .groupBy("customer_id")\
       .agg(max("product_name"),max(col("Cantidad")))\
       .show()


# COMMAND ----------

## Solución 6

from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql import Row, functions as F

datefilter = col("join_date")<col("order_date")
rowfilter =col("rowNum")==1
df_join.filter(datefilter)\
       .groupBy("customer_id","product_name").agg(min("order_date").alias("min_order"))\
       .sort("customer_id",col("min_order").asc())\
       .select("customer_id","product_name","min_order",F.row_number().over(Window.partitionBy("customer_id").orderBy("customer_id")).alias("rowNum"))\
       .filter(rowfilter).show()

# COMMAND ----------

## Solución 7


from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql import Row, functions as F

datefilter = col("join_date")>=col("order_date")
rowfilter =col("rowNum")==1
df_join.filter(datefilter)\
       .groupBy("customer_id","product_name").agg(min("order_date").alias("min_order"))\
       .sort("customer_id",col("min_order").desc())\
       .select("customer_id","product_name","min_order",F.row_number().over(Window.partitionBy("customer_id").orderBy("customer_id")).alias("rowNum"))\
       .filter(rowfilter).show()


# COMMAND ----------

## Solución 8

from pyspark.sql.functions import count, max, col, sum

datefilter = col("join_date")>col("order_date")
df_join.filter(datefilter)\
       .groupBy("customer_id").agg(countDistinct("product_name"),sum("price"))\
       .show()


# COMMAND ----------

## Solución 9

from pyspark.sql.functions import count, max, col, sum, when

df_join.withColumn("puntos",when(col("product_name")=="sushi",col("price")*2*10)\
                  .when(col("product_name")!="sushi",col("price")*10))\
                  .groupBy("customer_id").agg(sum(col("puntos"))).show()


# COMMAND ----------

## Solución 10

from pyspark.sql.functions import count, max, col, sum, when

df_join.withColumn("puntos",when( (col("product_name")=="sushi") & (col("order_date")<col("join_date")),col("price")*2*10)\
                  .when( (col("product_name")!="sushi") & (col("order_date")<col("join_date")),col("price")*10)\
                  .when( (col("order_date")>=col("join_date")),col("price")*2*10) )\
                  .filter(col("order_date")<'2021-01-31')\
                  .groupBy("customer_id").agg(sum(col("puntos")))\
                  .show()



