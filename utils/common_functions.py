# Databricks notebook source
import json
input_params={}
raw_folder_path="/mnt/mavendataguy/input/ecom_growth"
raw_folder_path_products="/mnt/mavendataguy/input/ecom_growth/products"
raw_folder_path_sessions="/mnt/mavendataguy/input/ecom_growth/website_sessions"
processed_folder_path="/mnt/mavendataguy/output/ecom_growth"
presentation_folder_path = "/mnt/mavendataguy/output/present"
v_file_date="2021-12-15"
input_params['v_file_date']=v_file_date
input_params['raw_folder_path']=raw_folder_path
# Writing configs to JSON and can be read back as well
df = spark.read.json(sc.parallelize([input_params]))
df.coalesce(1).write.mode("overwrite").format('json').save('/mnt/mavendataguy/input/params.json')

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/rana.aurangzeb@hotmail.com/cicd-pipeline")
#print(sys.path)

# COMMAND ----------

from pyspark.sql.types import StructField, StructType,StringType,DateType
from datetime import datetime,date
log_schema=StructType([
    StructField('notebookName',StringType(), False),
    StructField('functionName',StringType(), False),
    StructField('source',StringType(), True),
    StructField('target',StringType(), True),
    StructField('eventDate',DateType(), False),
    StructField('eventTime',StringType(), False), #datetime.now().strftime("%H:%M:%S %p")
    StructField('remarks',StringType(), True)  
                               ])
#display(spark.sql(f"create database misc OPTIMIZE {misc.test}"))

# COMMAND ----------

def append_log_data(logSchema,notebookName,functionName,source,target,eventDate,eventTime,remarks,table_name, partition_column):
    #output_df = re_arrange_partition_column(input_df, partition_column)
    initialMessage=[{"notebookName":notebookName,"functionName":functionName,"source":source,"target":target,"eventDate":eventDate,"eventTime":eventTime,"remarks":remarks}]
    df=spark.createDataFrame(data=initialMessage,schema=logSchema)
    #output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    if (spark._jsparkSession.catalog().tableExists(f"{logSchema}.{table_name}")):
        df.write.mode("append").insertInto(f"{logSchema}.{table_name}")
    else:
        df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{logSchema}.{table_name}")

# COMMAND ----------


