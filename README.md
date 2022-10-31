# cicd-pipeline
# ETL using Azure Cloud ADLS2, Python (PySpark), Databricks & Data Factory

Problem Statement: 

ABC Company is an online retail company & into business for past 8 months. There is a requirement to modernise the ETL using Cloud technologies. The company wants to build, maintain & use daily Metrics to Analyse the Online Retail data to run & optimise the Digital Marketing Campaigns to further help boost sales.
To meet the above stated requirements, we need to process the daily website sessions, daily orders data about customer purchases & the products information data. There is lot more than this, but the remaining processing will be done in next phase.

<b>Solution Brief: </b>

Data Source: The data landing zone is a raw folder maintained date-wise in Azure Data lake Gen 2

Staging Layer:  The data has been transformed using Azure Data Bricks (PySpark/Python, SQL) and then written back in a Parquet based table 

Scheduling & orchestration:  The Scheduling & orchestration has been done using Azure Data factory & it invoked the Databricks notebook which contain all the ingestion & transformation scripts 

Final Presentation Layer:
The aggregated data will be fetched from the parquet-based table & delta lake tables from ADLS2 to Azure SQL as final step 

As a Pre-Requisite, we need to integrate Databricks & Azure Factory & it has been explained in below link :) 

https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/3504029184321759/178917289689483/3012675142119230/latest.html


<b>Products File  </b>

[Ingesting a products file into a Parquet based Dimension Table, SCD Type -1 has been followed to track changes in dimension]

Please follow below link for more details:) 

https://github.com/mavendataguy/cicd-pipeline/blob/main/src/ingest_products_file.py

<b>Website Sessions File </b>

[Ingesting a daily Website sessions file into a Parquet based Fact Table]

Please follow below link for more details :) 

https://github.com/mavendataguy/cicd-pipeline/blob/main/src/ingest_sessions_file.py

<b>Common/Utility Functions </b>
https://github.com/mavendataguy/cicd-pipeline/blob/main/utils/common_functions.py

<b>Database &  Table Creation Scripts </b>
https://github.com/mavendataguy/cicd-pipeline/blob/main/src/prepare_etl_ecom.py
