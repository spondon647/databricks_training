-- Databricks notebook source
USE CATALOG `lakehouse`;
USE SCHEMA `default`;

-- COMMAND ----------

select * from read_files("abfss://raw@adlsnaval123.dfs.core.windows.net/ecom/sales")

-- COMMAND ----------

create schema lakehouse.spondon_bronze managed location 'abfss://raw@adlsnaval123.dfs.core.windows.net/lakehouse/spondon_bronze'

-- COMMAND ----------

create schema lakehouse.spondon_silver managed location 'abfss://raw@adlsnaval123.dfs.core.windows.net/lakehouse/spondon_silver'

-- COMMAND ----------

create schema lakehouse.spondon_gold managed location 'abfss://raw@adlsnaval123.dfs.core.windows.net/lakehouse/spondon_gold'
