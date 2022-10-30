# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql.functions import lit
from pyspark.sql import functions as sf
from delta.tables import *
from pyspark.sql.functions import *
#Let's pass on Ingestion Date as parameter - It can be passed from Azure Data Factory as well
dbutils.widgets.text("p_file_date", "2012-03-19")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../utils/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType,TimestampType
sessions_schema = StructType(fields=[StructField("website_session_id", DoubleType(), False),
                                     StructField("created_at", TimestampType(), True),
                                     StructField("user_id", IntegerType(), True),
                                     StructField("is_repeat_session", IntegerType(), True),
                                     StructField("utm_source", StringType(), True),
                                     StructField("utm_campaign", StringType(), True),
                                     StructField("utm_content", StringType(), True),
                                     StructField("device_type", StringType(), True),
                                     StructField("http_referer", StringType(), True)
])

# COMMAND ----------

v_file_date = "2012-03-19"
path=f"{raw_folder_path}/website_sessions/{v_file_date}/sessions.csv"
sessions_df=read_file(path,sessions_schema,True,file_format="csv")
sessions_df.display()

# COMMAND ----------

sessions_selected_df = sessions_df.select(col("website_session_id"), col("created_at"), col("user_id"), col("is_repeat_session"), col("utm_source"), col("utm_campaign"),col("utm_content"),col("device_type"),col("http_referer"))
sessions_renamed_df = sessions_selected_df.withColumn("year_month", concat(year(sessions_selected_df.created_at),sf.format_string("%02d",month(sessions_selected_df.created_at))))\
.withColumn("file_date", lit(v_file_date))
#Step 4 - Add ingestion date to the dataframe
sessions_final_df = add_ingestion_date(sessions_renamed_df)
#Deleting the existing records of the same 'created_at' date, if any, in the destination Table (in case of repeat job Trigger)
deltaTable = DeltaTable.forPath(spark, "/mnt/mavendataguy/output/ecom_growth/sessions_staging")
deltaTable.delete(to_date(col("created_at"),'yyyy-MM-dd HH:mm:ss')==v_file_date)
#Step 5 - Write data to datalake as parquet
#Although we are ingesting records using created_at date column but the partition has been created using year_month column to meet the 'minimum number of records in a partition' criteria
sessions_final_df.display()

# COMMAND ----------

sessions_final_df.write.mode("append").format("delta").partitionBy("year_month").save("/mnt/mavendataguy/output/ecom_growth/sessions_staging")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


