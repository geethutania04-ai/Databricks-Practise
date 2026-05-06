# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG devcatalog;
# MAGIC USE SCHEMA BRONZE;

# COMMAND ----------

df_icd_code = spark.read.option("header", "true").option("inferschema","true").csv("abfss://outputexternal@databrciksadlsstgaccount.dfs.core.windows.net/OutputFolder/FileSystem/ICD_code_data")
df_icd_code.write.mode("overwrite").saveAsTable("ICD_code_data")


# COMMAND ----------

# MAGIC %md
# MAGIC select * from icd_code_data

# COMMAND ----------

df_NPI_data = spark.read.option("header", "true").option("inferschema","true").csv("abfss://outputexternal@databrciksadlsstgaccount.dfs.core.windows.net/OutputFolder/FileSystem/NPI_data")
df_NPI_data.write.mode("overwrite").saveAsTable("NPI_data")

# COMMAND ----------

# MAGIC %md
# MAGIC select * from npi_data;

# COMMAND ----------

df_cptcodes = spark.read.option("header", "true").option("inferschema","true").csv("abfss://outputexternal@databrciksadlsstgaccount.dfs.core.windows.net/OutputFolder/FileSystem/cptcodes")
df_cptcodes.printSchema()

from pyspark.sql import DataFrame
def replace_spaces_with_underscores(df_cptcodes: DataFrame) -> DataFrame:

    cleaned_df = df_cptcodes
    for old_col_name in df_cptcodes.columns:
        new_col_name = old_col_name.replace(" ", "_")

        if old_col_name != new_col_name:
            cleaned_df = cleaned_df.withColumnRenamed(old_col_name, new_col_name)

    return cleaned_df

df_cleaned = replace_spaces_with_underscores(df_cptcodes)

print("\n--- Cleaned Schema ---")
df_cleaned.printSchema()
df_cleaned.show()

df_cleaned.write.mode("overwrite").saveAsTable("cptcodes")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cptcodes;
