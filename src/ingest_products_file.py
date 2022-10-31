# Databricks notebook source
#from pyspark.sql.functions import * 
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
dbutils.widgets.text("p_file_date", "2021-12-15")
# Lets call the common_functions utils module to use all commontly used functions

# COMMAND ----------

# MAGIC %run "../utils/common_functions"

# COMMAND ----------

products_schema = StructType(fields=[StructField("product_Id", IntegerType(), False),
                                    StructField("created_at", DateType(), True),
                                    StructField("product_name", StringType(), True)])

# COMMAND ----------

def transform(products_df,db_path,db):
    # Lets add the Ingestion date to DataFrame
    results_with_columns_df=add_ingestion_date(products_df)
    results_deduped_df = results_with_columns_df.dropDuplicates(['product_Id'])
    #Step 4 - Write to Dataframe to designated Azure Data Lake container/directory in parquet format
    #The function 'merge_delta_data' will create a table it if does not exist
    #Lets use Merge Function (merge_delta_data) using Python
    #Databricks DeltaLake supports ACID & Merge Function Let's pass on below arguments to Merge Function (located in common_functions_ecom.py)
    merge_condition = "tgt.product_Id = src.product_Id"
    #from delta.tables import DeltaTable
    #DeltaTable.createIfNotExists(spark).location("/mnt/mavendataguy/silver/db/ecom/products/").addColumns(results_deduped_df.schema).execute
    merge_delta_data(results_deduped_df, db, 'products', db_path,merge_condition, 'created_at')
    

# COMMAND ----------

# Read the config
config_file='/dbfs/mnt/mavendataguy/bronze/config/param.json'
with open(config_file,'r') as inFile:
    config_data=json.load(inFile)
    #print(config_data)
    inFile.close()

# COMMAND ----------

v_file_date = dbutils.widgets.get("p_file_date")
products_path=f"{config_data['raw_folder_path']}/products/{v_file_date}/products.csv"
if __name__ == "__main__":
    # Step 1 - Read the CSV file using the spark dataframe reader API
    # We are calling the read_file function which requires arguments like path to the file, schema and headers=True/False
    products_df=read_file(products_path,products_schema,True,file_format="csv")
    # Lets apply MERGE function on the sessions table records and insert/update based on merge condition
    # after merge operation, we are writing the data to Azure ADLS2
    transform(products_df,config_data['db_path'],config_data['database'])

# COMMAND ----------

dbutils.notebook.exit("Success")
