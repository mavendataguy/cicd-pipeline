# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql import functions as sf
from delta.tables import *
from pyspark.sql.functions import *
#Let's pass on Ingestion Date as parameter - It can be passed from Azure Data Factory as well
dbutils.widgets.text("p_file_date", "2012-03-19")

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

def transform(sessions_selected_df,v_file_date,table_path="/mnt/mavendataguy/output/ecom_growth/sessions_staging"):
    sessions_selected_df = sessions_df.select(col("website_session_id"), col("created_at"), col("user_id"),col("is_repeat_session"), col("utm_source"),col("utm_campaign"),col("utm_content"),col("device_type"),col("http_referer"))
    sessions_renamed_df = sessions_selected_df.withColumn("year_month", concat(year(sessions_selected_df.created_at),sf.format_string("%02d",month(sessions_selected_df.created_at))))\
    .withColumn("file_date", lit(v_file_date))
    #Step 4 - Add ingestion date to the dataframe
    sessions_final_df = add_ingestion_date(sessions_renamed_df)
    #Deleting the existing records of the same 'created_at' date, if any, in the destination Table (in case of repeat job Trigger)
    deltaTable = DeltaTable.forPath(spark, "/mnt/mavendataguy/output/ecom_growth/sessions_staging")
    deltaTable.delete(to_date(col("created_at"),'yyyy-MM-dd HH:mm:ss')==v_file_date)
    return sessions_final_df

# COMMAND ----------

# Read the config
config_file='/dbfs/mnt/mavendataguy/bronze/config/param.json'
with open(config_file,'r') as inFile:
    config_data=json.load(inFile)
    #print(config_data)
    inFile.close()
# Pass on the file Date Parameter Value
v_file_date = dbutils.widgets.get("p_file_date")
if __name__ == "__main__":
    # derive the path of sessions file from the baseline raw_folder_path
    try:
        sessions_source=f"{config_data['raw_folder_path']}/website_sessions/{v_file_date}/sessions.csv"
        sessions_df=read_file(sessions_source,sessions_schema,True,file_format="csv")
    except Exception as e:
        append_log_data("ingest_sessions_file","read_file","sessions","session_staging",e)        
    # Pass on the destination delta table's path under Database ecom ... config_data['sessions_path']
    try:
        sessions_final_df=transform(sessions_df,dbutils.widgets.get("p_file_date"),config_data['sessions_path'])
    except Exception as e:
        append_log_data("ingest_sessions_file","transform_data_frame","sessions","session_staging",e)    
    #Step 5 - Write data to ADLS2 as delta table under Database
    #Although we are ingesting records using created_at date column but the partition has been created using year_month column
    #to meet the 'minimum number of records (size) in a partition' criteria
    try:
        sessions_final_df.coalesce(10).write.mode('append').format('delta').insertInto('ecom'+'.'+'sessions_staging')
        #sessions_final_df.write.mode("append").format("delta").partitionBy("year_month").save(config_data['sessions_path'])
    except Exception as e:
        append_log_data("ingest_sessions_file","write_transformed_data","sessions","session_staging",e)    

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


