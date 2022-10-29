# Databricks notebook source
import json
import sys
from pyspark.sql.functions import current_timestamp
input_params={}
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled",True)
raw_folder_path="/mnt/mavendataguy/input/ecom_growth"
processed_folder_path="/mnt/mavendataguy/silver/ecom_growth"
#raw_folder_path_products=raw_folder_path+"/products"
#raw_folder_path_sessions=raw_folder_path+"/website_sessions"
v_file_date="2021-12-15"
input_params['v_file_date']=v_file_date
input_params['raw_folder_path']=raw_folder_path
input_params['database']="ecom"
#files_path= {"products":f"{raw_folder_path}/{v_file_date}/products", "sessions.csv":f"{raw_folder_path}/{v_file_date}/sessions.csv"}

# COMMAND ----------

config_file='/dbfs/mnt/mavendataguy/bronze/config/param.json'
with open(config_file,'w') as outFile:
    config_data=json.dump(input_params,outFile)
    outFile.close()
#with open(config_file,'r') as inFile:
    #config_data=json.load(inFile)
    #print(config_data)
    #inFile.close()

# COMMAND ----------

def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

def read_file(path,custom_schema,headers="False",file_format="parquet",partition_col="date"):
    df= spark.read.format(file_format)\
    .schema(custom_schema)\
    .option("header", headers)\
    .load(f"{path}")#.filter(date_col="{date}")
    return df

# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):
    column_list = []
    for column_name in input_df.schema.names:
        if column_name != partition_column:
            column_list.append(column_name)
    column_list.append(partition_column)
    output_df = input_df.select(column_list)
    return output_df

# COMMAND ----------

def df_column_to_list(input_df, column_name):
    df_row_list = input_df.select(column_name) \
                        .distinct() \
                        .collect()
  
    column_value_list = [row[column_name] for row in df_row_list]
    return column_value_list

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
    output_df = re_arrange_partition_column(input_df, partition_column)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
    else:
        output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, db_path,merge_condition, partition_column):
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")

    from delta.tables import DeltaTable
    if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
        deltaTable = DeltaTable.forPath(spark, f"{db_path}/{table_name}")
        deltaTable.alias("tgt").merge(
        input_df.alias("src"),
        merge_condition) \
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
       .execute()
    else:
        input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")

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

# COMMAND ----------

def append_log_data(logSchema,notebookName,functionName,source,target,eventDate,remarks,table_name, partition_column):
    initialMessage=[{"notebookName":notebookName,"functionName":functionName,"source":source,"target":target,"eventDate":eventDate,"eventTime":datetime.now().strftime("%H:%M:%S %p"),"remarks":remarks}]
    df=spark.createDataFrame(data=initialMessage,schema=logSchema)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    if (spark._jsparkSession.catalog().tableExists(f"{logSchema}.{table_name}")):
        df.write.mode("append").insertInto(f"{logSchema}.{table_name}")
    else:
        df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{logSchema}.{table_name}")
        #display(spark.sql(f"create database misc OPTIMIZE {misc.test}"))

# COMMAND ----------

def create_temp_view (input_map, frmt):
    for key in input_map:
        spark.read.format(frmt).option("header",True).load(input_map.get(key)).createOrReplaceTempview(key)

# COMMAND ----------

spark.catalog.listTables()

# COMMAND ----------


