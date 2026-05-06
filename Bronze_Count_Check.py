# Databricks notebook source

df1_source_count_AzureSQL= spark.read.option("header", "true")\
    .option('inferSchema', True)\
        .csv("abfss://outputexternal@databrciksadlsstgaccount.dfs.core.windows.net/countcheck/CheckCount_AzureSQL.csv")
    
display(df1_source_count_AzureSQL)

# COMMAND ----------

df1_source_count_SQLServer=df_icd_code = spark.read.option("header", "true")\
    .option('inferSchema', True)\
        .csv("abfss://outputexternal@databrciksadlsstgaccount.dfs.core.windows.net/countcheck/CheckCount_SqlServer.csv")
display(df1_source_count_SQLServer)

# COMMAND ----------

df1_source_count = df1_source_count_AzureSQL.union(df1_source_count_SQLServer)
display(df1_source_count)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG DEVCATALOG;
# MAGIC USE SCHEMA BRONZE;

# COMMAND ----------

df2_bronze_count=spark.sql(
    """
    select 'CLAIMS' as Table_Name, count(*) as Table_Count from Claims
    union all 
    select 'TRANSACTIONS' as Table_Name, count(*) as Table_Count from Transactions
    union all 
    select 'PATIENTS' as Table_Name, count(*) as Table_Count from Patients
    union all 
    select 'ENCOUNTERS' as Table_Name, count(*) as Table_Count from Encounters
    """
) 
display(df2_bronze_count)

# COMMAND ----------

from pyspark.sql.functions import col
df3_join = df1_source_count.alias("df1").join(df2_bronze_count.alias("df2"), col("df1.Table_Name") == col("df2.Table_Name"), how="inner").select(col("df1.Table_Name").alias("Table_Name"), col("df1.Table_Count").alias("Source_Count"), col("df2.Table_Count").alias("Bronze_Count"))
display(df3_join)

# COMMAND ----------

from pyspark.sql.functions import col, when 
df_result = df3_join.withColumn( "Status", when(col("Source_Count") == col("Bronze_Count"), "PASS").otherwise("FAIL") ) 
display(df_result)
fail_count = df_result.filter(col("Status") == "FAIL").count()
display(fail_count)

# COMMAND ----------

import requests 
import json 
if fail_count > 0: 
    logic_app_url = "https://prod-35.eastus.logic.azure.com:443/workflows/970fbf1f889046fcb7e34abb31a06389/triggers/When_an_HTTP_request_is_received/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2FWhen_an_HTTP_request_is_received%2Frun&sv=1.0&sig=EgHZK2zDEN80OhAMJvTCTPWn_1mBix9xUJPXBY-I1R8" 
    payload = { "Body": "Row count mismatch detected", "runDate": df_result.filter(col("Status") == "FAIL").toPandas().to_dict(orient="records") } 
    requests.post(logic_app_url, json=payload)

# COMMAND ----------

if fail_count > 0: raise Exception("Row count validation failed. Email triggered.")
