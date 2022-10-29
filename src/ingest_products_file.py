# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

# MAGIC %run "../utils/common_functions"

# COMMAND ----------

products_schema = StructType(fields=[StructField("product_Id", IntegerType(), False),
                                    StructField("created_at", DateType(), True),
                                    StructField("product_name", StringType(), True)])

# COMMAND ----------

#Step 1 - Read the CSV file using the spark dataframe reader API
# We are calling the read_file function which requires arguments like path to the file, schema and headers=True/False
path=f"{raw_folder_path}/products/{v_file_date}/products.csv"
results_df=read_file(path,products_schema,True,file_format="csv")

# COMMAND ----------

# Lets add the Ingestion date to DataFrame
results_with_columns_df=add_ingestion_date(results_df)

# COMMAND ----------

results_deduped_df = results_with_columns_df.dropDuplicates(['product_Id'])


# COMMAND ----------

# MAGIC %sql
# MAGIC use ecom;
# MAGIC create table if not exists products (product_Id int, created_at DATE,product_name string,ingestion_date TIMESTAMP)

# COMMAND ----------

#Step 4 - Write to Dataframe to designated Azure Data Lake container/directory in parquet format
#The function 'merge_delta_data' will create a table it if does not exist
#Lets use Merge Function (merge_delta_data) using Python
#Databricks DeltaLake supports ACID & Merge Function Let's pass on below arguments to Merge Function (located in common_functions_ecom.py)
#Note: products is a small dimension table & does not need partition. 
db_path='/mnt/mavendataguy/silver/db/ecom'
merge_condition = "tgt.product_Id = src.product_Id"
#from delta.tables import DeltaTable
#DeltaTable.createIfNotExists(spark).location("/mnt/mavendataguy/silver/db/ecom/products/").addColumns(results_deduped_df.schema).execute
merge_delta_data(results_deduped_df, input_params['database'], 'products', db_path,merge_condition, 'created_at')
#dbutils.notebook.exit("Success")

# COMMAND ----------

# Read the config
with open(config_file,'r') as inFile:
    config_data=json.load(inFile)
    print(config_data)
    inFile.close()
# Change the required Config
config_file='/dbfs/mnt/mavendataguy/bronze/config/param.json'
input_params['v_file_date']="2021-12-15"
input_params['database']="ecom"
input_params[db_path]='/mnt/mavendataguy/silver/db/ecom'

# COMMAND ----------

if __name__ == "__main__":
    print("Hello, World!")
