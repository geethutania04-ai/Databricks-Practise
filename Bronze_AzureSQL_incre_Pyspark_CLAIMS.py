# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG devcatalog;
# MAGIC USE SCHEMA BRONZE;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists claims
# MAGIC (
# MAGIC   ClaimID String,TransactionID String, PatientID    String, EncounterID   String, ProviderID   String, DeptID     String, ServiceDate   DATE, ClaimDate    DATE, PayorID     String,ClaimAmount DECIMAL(18,2),PaidAmount DECIMAL(18,2),ClaimStatus String,PayorType String,Deductible DECIMAL(18,2),Coinsurance DECIMAL(18,2),Copay DECIMAL(18,2), InsertDate   TIMESTAMP, ModifiedDate  TIMESTAMP, Inserted_date_bronze TIMESTAMP, Updated_date_bronze TIMESTAMP
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC drop table claims;

# COMMAND ----------

claims_schema = "ClaimID String,TransactionID String, PatientID    String, EncounterID   String, ProviderID   String, DeptID     String, ServiceDate   DATE, ClaimDate    DATE, PayorID     String,ClaimAmount DECIMAL(18,2),PaidAmount DECIMAL(18,2),ClaimStatus String,PayorType String,Deductible DECIMAL(18,2),Coinsurance DECIMAL(18,2),Copay DECIMAL(18,2), InsertDate   TIMESTAMP, ModifiedDate  TIMESTAMP"

df_claims = spark.read.schema(claims_schema).option("header", "true").csv("abfss://outputexternal@databrciksadlsstgaccount.dfs.core.windows.net/OutputFolder/AzureSQLDB/CLAIMS")

# COMMAND ----------

# MAGIC %md
# MAGIC select * from claims

# COMMAND ----------

# MAGIC %md
# MAGIC ALTER TABLE claims ADD COLUMN Updated_date_bronze timestamp;
# MAGIC     
# MAGIC ALTER TABLE claims ADD COLUMN Inserted_date_bronze timestamp;

# COMMAND ----------

from delta.tables import DeltaTable

delta_df = DeltaTable.forName(spark, "devcatalog.bronze.claims")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

delta_df.alias("target").merge(
    source = df_claims.alias("source"),
    condition = "target.claimID = source.claimID"
).whenMatchedUpdate(set = 
                    {
                        "TransactionID" :  "source.TransactionID",
                        "PatientID" :  "source.PatientID",
                        "EncounterID" :  "source.EncounterID",
                        "ProviderID" :  "source.ProviderID",
                        "DeptID" :  "source.DeptID",
                        "ServiceDate" :  "source.ServiceDate",
                        "ClaimDate" :  "source.ClaimDate",
                        "PayorID" :  "source.PayorID",
                        "ClaimAmount" :  "source.ClaimAmount",
                        "PaidAmount" :  "source.PaidAmount",
                        "ClaimStatus" :  "source.ClaimStatus",
                        "PayorType" :  "source.PayorType",
                        "Deductible" :  "source.Deductible",
                        "Coinsurance" :  "source.Coinsurance",
                        "Copay" :  "source.Copay",
                        "InsertDate" :  "source.InsertDate",
                        "ModifiedDate" :  "source.ModifiedDate",
                        "Updated_date_bronze": current_timestamp()  
                    }
                    ).whenNotMatchedInsert(values = 
                    {
                        "claimID": "source.claimID",
                        "TransactionID" :  "source.TransactionID",
                        "PatientID" :  "source.PatientID",
                        "EncounterID" :  "source.EncounterID",
                        "ProviderID" :  "source.ProviderID",
                        "DeptID" :  "source.DeptID",
                        "ServiceDate" :  "source.ServiceDate",
                        "ClaimDate" :  "source.ClaimDate",
                        "PayorID" :  "source.PayorID",
                        "ClaimAmount" :  "source.ClaimAmount",
                        "PaidAmount" :  "source.PaidAmount",
                        "ClaimStatus" :  "source.ClaimStatus",
                        "PayorType" :  "source.PayorType",
                        "Deductible" :  "source.Deductible",
                        "Coinsurance" :  "source.Coinsurance",
                        "Copay" :  "source.Copay",
                        "InsertDate" :  "source.InsertDate",
                        "ModifiedDate" :  "source.ModifiedDate",
                        "Inserted_date_bronze": current_timestamp(), 
                        "Updated_date_bronze": current_timestamp()  
                    }
                    ).execute()


# COMMAND ----------

# MAGIC %md
# MAGIC select * from devcatalog.bronze.claims

# COMMAND ----------

# MAGIC %md
# MAGIC ALTER TABLE devcatalog.bronze.claims 
# MAGIC ALTER COLUMN ClaimAmount DECIMAL(18,2);
# MAGIC     
# MAGIC
# MAGIC
