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

def create_sessions(db_name):
    spark.sql(f'use {db_name} ')
    spark.sql("""
    create table IF NOT EXISTS sessions_staging(
    website_session_id double,created_at timestamp ,
    user_id int, is_repeat_session int,
    utm_source string,
    utm_campaign string,
    utm_content string,
    device_type string,
    http_referer string,
    year_month int,
    file_date date,
    ingestion_date timestamp) partitioned by (year_month)
    """)

# COMMAND ----------

def create_logs(db_name):
    spark.sql(f'use {db_name} ')
    spark.sql("""
    create table IF NOT EXISTS logs(
    notebookName string,functionName string ,
    source string, target string,
    eventDate date,
    eventTime string,
    remarks string
    )
    """)

# COMMAND ----------

initialse(db_path,db_name)
create_products(db_name)
create_sessions(db_name)
create_logs(db_name)

# COMMAND ----------


