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
#datetime.now().strftime("%H:%M:%S %p")

# COMMAND ----------


