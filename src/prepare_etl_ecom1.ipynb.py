# Databricks notebook source
def initialse(db_name):
    spark.sql(f"create database if not exists misc location 'dbfs:/mnt/mavendataguy/silver/db/{db_name}'")    

# COMMAND ----------

initialse('ecom')

# COMMAND ----------


