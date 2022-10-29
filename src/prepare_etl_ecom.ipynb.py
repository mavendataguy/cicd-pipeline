# Databricks notebook source
db_path="/mnt/mavendataguy/silver/db"
db_name="ecom"
def initialse(db_path,db_name):
    spark.sql(f'create database if not exists {db_name} location "{db_path}/{db_name}"')

# COMMAND ----------

def create_products(db_name):
    spark.sql(f'use {db_name} ')
    spark.sql("""
    create table if not exists products (product_Id int, created_at DATE,product_name string,ingestion_date TIMESTAMP);
    """)

# COMMAND ----------

initialse(db_path,db_name)
create_products(db_name)

# COMMAND ----------


