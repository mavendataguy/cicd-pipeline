# Databricks notebook source
db_path="/mnt/mavendataguy/silver/db"
db_name="ecom"
def initialse(db_path,db_name):
    spark.sql(f'create database if not exists {db_name} location "{db_path}/{db_name}"')

# COMMAND ----------

initialse(db_path,db_name)

# COMMAND ----------


