-- Databricks notebook source
--1--CREAMOS LA BASE DE DATOS

CREATE DATABASE PIZZERIA LOCATION '/databases/PIZZERIA'

-- COMMAND ----------

--2--CREAMOS LA TABLA DE MENU

CREATE TABLE PIZZERIA.MENU(
PRODUCT_ID INT,
PRODUCT_NAME STRING,
PRICE DOUBLE
)
STORED AS PARQUET
LOCATION '/databases/PIZZERIA/MENU'
TBLPROPERTIES(
  'parquet.compression'='SNAPPY',
  'store.chasrset'='ISO-8859-1',
  'retrieve.charset'='ISO-8859-1'
);

-- COMMAND ----------

--3-CREAMOS LA TABLA DE MEMBERS
--customer_id,join_date

CREATE TABLE PIZZERIA.MEMBER(
CUSTOMER_ID STRING,
JOIN_DATE DATE
)
STORED AS ORC
LOCATION '/databases/PIZZERIA/MEMBER'
TBLPROPERTIES(
  'orc.compression'='SNAPPY',
  'store.chasrset'='ISO-8859-1',
  'retrieve.charset'='ISO-8859-1'
);

-- COMMAND ----------

--4-CREAMOS LA TABLA DE SALES
--customer_id,order_date,product_id
CREATE TABLE PIZZERIA.SALE(
CUSTOMER_ID STRING,
ORDER_DATE DATE,
PRODUCT_ID INT
) STORED AS ORC
LOCATION '/databases/PIZZERIA/SALE'
TBLPROPERTIES(
  'orc.compression'='SNAPPY',
  'store.chasrset'='ISO-8859-1',
  'retrieve.charset'='ISO-8859-1'
);
