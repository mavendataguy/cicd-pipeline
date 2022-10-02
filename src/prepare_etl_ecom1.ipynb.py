# Databricks notebook source
def initialse(db_name):
    spark.sql(f"create database if not exists misc location 'dbfs:/mnt/mavendataguy/silver/db/{ecom}'")    

# COMMAND ----------

initialse('ecom')

# COMMAND ----------


