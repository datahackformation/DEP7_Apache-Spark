# Databricks notebook source
##DEFINIMOS LAS LIBRERIAS
from pyspark.sql.types import StructType,StructField
from pyspark.sql.types import StringType,IntegerType,DoubleType,DateType
import pyspark.sql.functions as f
from pyspark.sql.window import Window
from pyspark.sql.functions import udf, struct

# COMMAND ----------

# MAGIC %md 
# MAGIC ###¿Cuál es la cantidad total que gastó cada cliente en el restaurante?

# COMMAND ----------

##1-DEFINIMOS EL DATAFRAME DE LAS VENTAS
dfVentas=spark.sql("SELECT * FROM PIZZERIA.SALE")
dfVentas.display()


# COMMAND ----------

##2-DEFINIMOS EL DATAFRAME DE LAS VENTAS
dfProducto=spark.sql("SELECT * FROM PIZZERIA.MENU")
dfProducto.display()

# COMMAND ----------

##3-HACEMOS EL ANALISIS
dfPregunta1=dfVentas.alias("V").join(
    dfProducto.alias("P"),
    f.col("V.PRODUCT_ID")==f.col("P.PRODUCT_ID")
).select(
    f.col("V.CUSTOMER_ID"),
    f.col("P.PRICE")
).groupBy("CUSTOMER_ID").\
sum("PRICE").\
orderBy("CUSTOMER_ID").\
toDF(*("CUSTOMER_ID", "MONTO"))

dfPregunta1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###¿Cuántos días ha visitado cada cliente el restaurante?

# COMMAND ----------

##1-OBTENEMOS EL DATAFRAME QUE CONTIENE LOS DISTINTOS DIAS QUE EL CLIENTE VISITO EL RESTAURANTE
dfClienteDia=spark.sql("SELECT DISTINCT CUSTOMER_ID,ORDER_DATE FROM PIZZERIA.SALE")

# COMMAND ----------

##2-HACEMOS EL ANALISIS
Pregunta2=dfClienteDia.groupBy("CUSTOMER_ID").count().orderBy("CUSTOMER_ID").toDF(*("CUSTOMER_ID", "VISITAS"))

Pregunta2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###¿Cuál fue el primer artículo del menú comprado por cada cliente?

# COMMAND ----------

##1-DEFINIMOS EL DATAFRAME DE LAS VENTAS
dfVentas=spark.sql("SELECT   * FROM PIZZERIA.SALE")
dfVentas.display()

# COMMAND ----------

##2-DEFINIMOS EL DATAFRAME DE LOS MENUS
dfProducto=spark.sql("SELECT * FROM PIZZERIA.MENU")
dfProducto.display()


# COMMAND ----------

##3-DEFINIMOS LA PRIMERA COMPRA DEL CLIENTE
aux1=dfVentas.select(f.col("CUSTOMER_ID"),f.col("ORDER_DATE")).\
groupBy("CUSTOMER_ID").agg(f.min(f.col("ORDER_DATE"))).\
toDF("CUSTOMER_ID","ORDER_DATE")

aux1.display()

# COMMAND ----------

##4-DEFINIMOS EL DETALLE DE LAS VENTAS DE LOS CLIENTES
aux2=dfVentas.alias("V").join(
dfProducto.alias("P"),
f.col("V.PRODUCT_ID")==f.col("P.PRODUCT_ID")
).select(
f.col("V.CUSTOMER_ID"),
f.col("V.ORDER_DATE"),
f.col("P.PRODUCT_NAME")
).orderBy("V.ORDER_DATE")

aux2.display()

# COMMAND ----------

##5-CONSTRUIMOS EL DATAFRAME FINAL USANDO LOS SETS DE LOS PASOS ANTERIORES
dfPregunta3=aux1.alias("A1").join(
aux2.alias("A2"),
(f.col("A1.CUSTOMER_ID")==f.col("A2.CUSTOMER_ID")) & (f.col("A1.ORDER_DATE")==f.col("A2.ORDER_DATE"))
).select(
f.col("A1.CUSTOMER_ID"),
f.col("A1.ORDER_DATE"),
f.col("A2.PRODUCT_NAME")
).orderBy("CUSTOMER_ID")

dfPregunta3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###¿Cuál es el artículo más comprado en el menú y cuántas veces lo compraron todos los clientes?

# COMMAND ----------

##1-HALLAMOS LAS VENTAS
dfVentas=spark.sql("SELECT  * FROM PIZZERIA.SALE")
dfVentas.display()

# COMMAND ----------

##2-OBTENEMOS EL PRODUCTO MAS VENDIDO POR SU ID (EL PRODUCTO MAS VENDIDO TIENE EL ID 3)
dfProdVendido=dfVentas.groupBy("PRODUCT_ID").count().toDF("PRODUCT_ID","Q").orderBy("Q")

dfProdVendido.display()

# COMMAND ----------

##3-OBTENEMOS CUANTAS VECES SE COMPRO EL PRODUCTO ID=3
dfPregunta4=dfVentas.filter(f.col("PRODUCT_ID")==3).\
groupBy(f.col("CUSTOMER_ID")).count().toDF("CUSTOMER_ID","Q").orderBy("CUSTOMER_ID").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###¿Qué artículo fue el más popular para cada cliente?

# COMMAND ----------

##1-DEFINIMOS EL DATAFRAME DE LAS VENTAS
dfVentas=spark.sql("SELECT  * FROM PIZZERIA.SALE")
dfVentas.display()

# COMMAND ----------

##2-DEFINIMOS EL DATAFRAME DE LOS MENUS
dfProducto=spark.sql("SELECT * FROM PIZZERIA.MENU")
dfProducto.display()

# COMMAND ----------

##3-HALLAMOS EL DETALLE DE VENTA POR PRODUCTO
aux2=dfVentas.alias("V").join(
dfProducto.alias("P"),
f.col("V.PRODUCT_ID")==f.col("P.PRODUCT_ID")
).select(
f.col("V.CUSTOMER_ID"),
f.col("V.ORDER_DATE"),
f.col("P.PRODUCT_NAME")
).groupBy("CUSTOMER_ID","PRODUCT_NAME").\
count().\
toDF("CUSTOMER_ID","PRODUCT_NAME","Q")

aux2.display()


# COMMAND ----------

##4-HALLAMOS EL PRODUCTO MAS POPULAR POR CLIENTE
dfPregunta5 = aux2.select(
    "CUSTOMER_ID",
    "PRODUCT_NAME","Q", 
    f.row_number().over(Window.partitionBy(aux2['CUSTOMER_ID']).orderBy(aux2['Q'].desc())).alias("row_num")).\
    filter(f.col("row_num")==1)

dfPregunta5.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###¿Qué artículo compró primero el cliente después de convertirse en miembro?

# COMMAND ----------

##1-DEFINIMOS EL DATAFRAME DE LAS VENTAS
dfVentas=spark.sql("SELECT  * FROM PIZZERIA.SALE")
dfVentas.display()

# COMMAND ----------

##2-DEFINIMOS EL DATAFRAME DE LAS MENUS
dfProductos=spark.sql("SELECT  * FROM PIZZERIA.MENU")
dfProductos.display()

# COMMAND ----------

##3-DEFINIMOS EL DATAFRAME DE LAS MEMBERS
dfMembers=spark.sql("SELECT  * FROM PIZZERIA.MEMBER")
dfMembers.display()

# COMMAND ----------

##4-ARMAMOS EL DATAFRAME GENERAL DE LAS VENTAS DE LOS CLIENTES POSTERIOR A SU MEMBRESIA
aux1=dfVentas.alias("V").join(
dfProductos.alias("P"),
f.col("V.PRODUCT_ID")==f.col("P.PRODUCT_ID")
).join(
dfMembers.alias("M"),
f.col("V.CUSTOMER_ID")==f.col("M.CUSTOMER_ID")
).select(
f.col("V.CUSTOMER_ID"),
f.col("V.ORDER_DATE"),
f.col("P.PRODUCT_NAME"),
f.col("M.JOIN_DATE"),
f.datediff(f.col("M.JOIN_DATE"),f.col("V.ORDER_DATE")).alias("DATEDIFF")
).filter(f.col("JOIN_DATE")>f.col("ORDER_DATE"))


aux1.display()


# COMMAND ----------

##5-OBTENEMOS LA PRIMERA COMPRA POST MEMBRESIA

dfPregunta6=aux1.select(
"CUSTOMER_ID",
"PRODUCT_NAME",
f.row_number().over(Window.partitionBy(aux1['CUSTOMER_ID']).orderBy(aux1['DATEDIFF'].asc())).alias("row_num")
).filter(f.col("row_num")==1).display()




# COMMAND ----------

# MAGIC %md
# MAGIC ### ¿Qué artículo se compró justo antes de que el cliente se convirtiera en miembro?

# COMMAND ----------

##1-DEFINIMOS EL DATAFRAME DE LAS VENTAS
dfVentas=spark.sql("SELECT  * FROM PIZZERIA.SALE")
dfVentas.display()

# COMMAND ----------

##2-DEFINIMOS EL DATAFRAME DE LAS MENUS
dfProductos=spark.sql("SELECT  * FROM PIZZERIA.MENU")
dfProductos.display()

# COMMAND ----------

##3-DEFINIMOS EL DATAFRAME DE LAS MEMBERS
dfMembers=spark.sql("SELECT  * FROM PIZZERIA.MEMBER")
dfMembers.display()

# COMMAND ----------

##4-ARMAMOS EL DATAFRAME GENERAL DE LAS VENTAS DE LOS CLIENTES ANTES DE SU MEMBRESIA
aux1=dfVentas.alias("V").join(
dfProductos.alias("P"),
f.col("V.PRODUCT_ID")==f.col("P.PRODUCT_ID")
).join(
dfMembers.alias("M"),
f.col("V.CUSTOMER_ID")==f.col("M.CUSTOMER_ID")
).select(
f.col("V.CUSTOMER_ID"),
f.col("V.ORDER_DATE"),
f.col("P.PRODUCT_NAME"),
f.col("M.JOIN_DATE"),
f.datediff(f.col("M.JOIN_DATE"),f.col("V.ORDER_DATE")).alias("DATEDIFF")
).filter(f.col("ORDER_DATE")<f.col("JOIN_DATE"))

aux1.display()


# COMMAND ----------

##5-OBTENEMOS LA ULTIMA COMPRA PRE MEMBRESIA

dfPregunta7=aux1.select(
"CUSTOMER_ID",
"PRODUCT_NAME",
f.row_number().over(Window.partitionBy(aux1['CUSTOMER_ID']).orderBy(aux1['DATEDIFF'].asc())).alias("row_num")
).filter(f.col("row_num")==1).display()




# COMMAND ----------

# MAGIC %md
# MAGIC ###¿Cuál es el total de artículos y la cantidad gastada por cada miembro antes de convertirse en miembro?

# COMMAND ----------

##1-DEFINIMOS EL DATAFRAME DE LAS VENTAS
dfVentas=spark.sql("SELECT  * FROM PIZZERIA.SALE")
dfVentas.display()

# COMMAND ----------

##2-DEFINIMOS EL DATAFRAME DE LAS MENUS
dfProductos=spark.sql("SELECT  * FROM PIZZERIA.MENU")
dfProductos.display()

# COMMAND ----------

##3-DEFINIMOS EL DATAFRAME DE LAS MEMBERS
dfMembers=spark.sql("SELECT  * FROM PIZZERIA.MEMBER")
dfMembers.display()

# COMMAND ----------

##4-ARMAMOS EL DATAFRAME GENERAL DE LAS VENTAS DE LOS CLIENTES ANTES DE SU MEMBRESIA
aux1=dfVentas.alias("V").join(
dfProductos.alias("P"),
f.col("V.PRODUCT_ID")==f.col("P.PRODUCT_ID")
).join(
dfMembers.alias("M"),
f.col("V.CUSTOMER_ID")==f.col("M.CUSTOMER_ID")
).select(
f.col("V.CUSTOMER_ID"),
f.col("V.ORDER_DATE"),
f.col("P.PRODUCT_NAME"),
f.col("M.JOIN_DATE"),
f.col("P.PRICE"),
f.datediff(f.col("M.JOIN_DATE"),f.col("V.ORDER_DATE")).alias("DATEDIFF")
).filter(f.col("ORDER_DATE")<f.col("JOIN_DATE"))

aux1.display()

# COMMAND ----------

##5-OBTENEMOS EL TODOS LOS PRODUCTOS Y MONTOS DE LOS CLIENTES ANTES DE SER MIEMBROS
dfPregunta8=aux1.select("CUSTOMER_ID","PRODUCT_NAME","PRICE").groupBy("CUSTOMER_ID").agg(f.sum("PRICE"),f.count("PRODUCT_NAME")).\
toDF("CUSTOMER_ID","MONTO","Q")

dfPregunta8.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Si cada $ 1 gastado equivale a 10 puntos y el sushi tiene un multiplicador de puntos 2x,¿cuántos puntos tendría cada cliente?

# COMMAND ----------

##1-DEFINIMOS EL DATAFRAME DE LAS VENTAS
dfVentas=spark.sql("SELECT  * FROM PIZZERIA.SALE")
dfVentas.display()

# COMMAND ----------

##2-DEFINIMOS EL DATAFRAME DE LAS MENUS
dfProductos=spark.sql("SELECT  * FROM PIZZERIA.MENU")
dfProductos.display()

# COMMAND ----------

##3-DEFINIMOS EL DATAFRAME DE LAS MEMBERS
dfMembers=spark.sql("SELECT  * FROM PIZZERIA.MEMBER")
dfMembers.display()

# COMMAND ----------

##4-ARMAMOS EL DATAFRAME GENERAL DE LAS VENTAS DE LOS CLIENTES ANTES DE SU MEMBRESIA
aux1=dfVentas.alias("V").join(
dfProductos.alias("P"),
f.col("V.PRODUCT_ID")==f.col("P.PRODUCT_ID")
).select(
f.col("V.CUSTOMER_ID"),
f.col("V.ORDER_DATE"),
f.col("P.PRODUCT_NAME"),
f.col("P.PRICE")
)

aux1.display()

# COMMAND ----------

##5-DEFINIMOS LAS FUNCIONES DE CALCULO
def calcularPuntos(producto, precio):
    punto=0

    if producto=='sushi':
        punto=precio*20
    else:
        punto=precio*10

         
    return punto
  

# COMMAND ----------

##6-CREAMOS EL UDF
udfcalcularPuntos = udf(
	(
		lambda parametros : calcularPuntos(
			parametros[0], 
			parametros[1]
		)
	),
	DoubleType()
)

# COMMAND ----------

##7-HALLAMOS LOS PUNTOS POR CADA COMPRA
aux2=aux1.select(
aux1["CUSTOMER_ID"],
aux1["ORDER_DATE"],
aux1["PRODUCT_NAME"],
aux1["PRICE"],
udfcalcularPuntos(
    struct(
        aux1["PRODUCT_NAME"],
        aux1["PRICE"],
    )
).alias("PUNTOS")
)

aux2.display()

# COMMAND ----------

##8-HALLAMOS LOS PUNTOS POR CLIENTE
dfPregunta9=aux2.select("CUSTOMER_ID","PUNTOS").groupBy("CUSTOMER_ID").agg(f.sum("PUNTOS")).toDF("CUSTOMER_ID","PUNTOS")

dfPregunta9.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###En la primera semana después de que un cliente se une al programa (incluida la fecha de ingreso), gana el doble de puntos en todos los artículos, no solo en sushi. ¿Cuántos puntos tienen los clientes A y B a fines de enero?

# COMMAND ----------

##1-DEFINIMOS EL DATAFRAME DE LAS VENTAS
dfVentas=spark.sql("SELECT  * FROM PIZZERIA.SALE")
dfVentas.display()

# COMMAND ----------

##2-DEFINIMOS EL DATAFRAME DE LAS MENUS
dfProductos=spark.sql("SELECT  * FROM PIZZERIA.MENU")
dfProductos.display()

# COMMAND ----------

##3-DEFINIMOS EL DATAFRAME DE LAS MEMBERS
dfMembers=spark.sql("SELECT  * FROM PIZZERIA.MEMBER")
dfMembers.display()

# COMMAND ----------

##4-ARMAMOS EL DATAFRAME GENERAL DE LAS VENTAS DE LOS CLIENTES QUE ADQUIRIERON LA MEMBRESIA. SOLO CONSIDERAMOS VENTAS DE AGOSTO
aux1=dfVentas.alias("V").join(
dfProductos.alias("P"),
f.col("V.PRODUCT_ID")==f.col("P.PRODUCT_ID")
).join(
dfMembers.alias("M"),
f.col("V.CUSTOMER_ID")==f.col("M.CUSTOMER_ID")
).select(
    f.col("V.CUSTOMER_ID"),
    f.col("V.ORDER_DATE"),
    f.col("P.PRODUCT_NAME"),
    f.col("PRICE"),
    f.col("M.JOIN_DATE"),
    f.datediff(f.col("V.ORDER_DATE"),f.col("M.JOIN_DATE")).alias("DATEDIFF")
).where(f.col("V.ORDER_DATE")<'2021-02-01')


aux1.display()


# COMMAND ----------

##5-DEFINIMOS LAS FUNCIONES DE CALCULO
def calcularPuntos(producto, precio,diffday):
    punto=0
    if (diffday<0) or (diffday>7):
        if producto=='sushi':
            punto=precio*20
        else:
            punto=precio*10
    else:
        punto=precio*20

         
    return punto

# COMMAND ----------

##6-CREAMOS EL UDF
udfcalcularPuntos = udf(
	(
		lambda parametros : calcularPuntos(
			parametros[0], 
			parametros[1],
            parametros[2]
		)
	),
	DoubleType()
)

# COMMAND ----------

##7-HALLAMOS LOS PUNTOS DE LOS CLIENTES A Y B
aux2=aux1.select(
    f.col("CUSTOMER_ID"),
    f.col("ORDER_DATE"),
    f.col("PRODUCT_NAME"),
    f.col("PRICE"),
    f.col("JOIN_DATE"),
    f.col("DATEDIFF"),
    udfcalcularPuntos(
    struct(
        f.col("PRODUCT_NAME"),
        f.col("PRICE"),
        f.col("DATEDIFF"),
    )
    ).alias("PUNTOS")
)


aux2.display()

# COMMAND ----------

##8-HALLAMOS LOS PUNTOS CALCULADOS POR CLIENTE
dfPregunta10=aux2.groupBy(f.col("CUSTOMER_ID")).agg(f.sum(f.col("PUNTOS"))).toDF("CUSTOMER_ID","PUNTOS")

dfPregunta10.display()
