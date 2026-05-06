# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG devcatalog;
# MAGIC USE SCHEMA BRONZE;

# COMMAND ----------

transactions_schema = "TransactionID String,  EncounterID   String,  PatientID    String,  ProviderID   String,  DeptID     String,  VisitDate    DATE,  ServiceDate   DATE,  PaidDate    DATE,VisitType String,Amount DECIMAL(18,2),AmountType String,PaidAmount DECIMAL(18,2),  ClaimID     String,  PayorID     String,ProcedureCode INT,ICDCode String,LineOfBusiness String,MedicaidID String,MedicareID String,InsertDate   TIMESTAMP,ModifiedDate  TIMESTAMP"

df_trans = spark.read.schema(transactions_schema).option("header", "true").csv("abfss://outputexternal@databrciksadlsstgaccount.dfs.core.windows.net/OutputFolder/AzureSQLDB/TRANSACTIONS")

# COMMAND ----------

# MAGIC %md
# MAGIC ALTER TABLE TRANSACTIONS ADD COLUMN Updated_date_bronze timestamp;
# MAGIC     
# MAGIC ALTER TABLE TRANSACTIONS ADD COLUMN Inserted_date_bronze timestamp;

# COMMAND ----------

from delta.tables import DeltaTable

delta_df = DeltaTable.forName(spark, "devcatalog.bronze.transactions")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

delta_df.alias("target").merge(
    source = df_trans.alias("source"),
    condition = "target.transactionID = source.transactionID"
).whenMatchedUpdate(set = 
                    {
                        "EncounterID" :  "source.EncounterID",
                        "PatientID" :  "source.PatientID",
                        "ProviderID" :  "source.ProviderID",
                        "DeptID" :  "source.DeptID",
                        "VisitDate" :  "source.VisitDate",
                        "ServiceDate" :  "source.ServiceDate",
                        "PaidDate" :  "source.PaidDate",
                        "VisitType" :  "source.VisitType",
                        "Amount" :  "source.Amount",
                        "AmountType" :  "source.AmountType",
                        "PaidAmount" :  "source.PaidAmount",
                        "ClaimID" :  "source.ClaimID",
                        "PayorID" :  "source.PayorID",
                        "ProcedureCode" :  "source.ProcedureCode",
                        "ICDCode" :  "source.ICDCode",
                        "LineOfBusiness" :  "source.LineOfBusiness",
                        "MedicaidID" :  "source.MedicaidID",
                        "MedicareID" :  "source.MedicareID",
                        "InsertDate" :  "source.InsertDate",
                        "ModifiedDate" :  "source.ModifiedDate",
                        "Updated_date_bronze": current_timestamp()  
                    }
                    ).whenNotMatchedInsert(values = 
                    {
                        "EncounterID" :  "source.EncounterID",
                        "PatientID" :  "source.PatientID",
                        "ProviderID" :  "source.ProviderID",
                        "DeptID" :  "source.DeptID",
                        "VisitDate" :  "source.VisitDate",
                        "ServiceDate" :  "source.ServiceDate",
                        "PaidDate" :  "source.PaidDate",
                        "VisitType" :  "source.VisitType",
                        "Amount" :  "source.Amount",
                        "AmountType" :  "source.AmountType",
                        "PaidAmount" :  "source.PaidAmount",
                        "ClaimID" :  "source.ClaimID",
                        "PayorID" :  "source.PayorID",
                        "ProcedureCode" :  "source.ProcedureCode",
                        "ICDCode" :  "source.ICDCode",
                        "LineOfBusiness" :  "source.LineOfBusiness",
                        "MedicaidID" :  "source.MedicaidID",
                        "MedicareID" :  "source.MedicareID",
                        "InsertDate" :  "source.InsertDate",
                        "ModifiedDate" :  "source.ModifiedDate",
                        "Inserted_date_bronze": current_timestamp(), 
                        "Updated_date_bronze": current_timestamp()  
                    }
                    ).execute()


# COMMAND ----------

# MAGIC %md
# MAGIC df_trans = spark.read.option("header", "true").option("inferschema","true").csv("abfss://outputexternal@databrciksadlsstgaccount.dfs.core.windows.net/OutputFolder/AzureSQLDB/TRANSACTIONS")
# MAGIC df_trans.display()

# COMMAND ----------

# MAGIC %md
# MAGIC from pyspark.sql.functions import to_date, col, to_timestamp
# MAGIC df_trans_update = df_trans.withColumn("ServiceDate",to_date(col("ServiceDate"))).\
# MAGIC withColumn("VisitDate",to_date(col("VisitDate"))).\
# MAGIC     withColumn("PaidDate",to_date(col("PaidDate"))).\
# MAGIC         withColumn("InsertDate",to_date(col("InsertDate"))).\
# MAGIC         withColumn("ModifiedDate",to_date(col("ModifiedDate")))
# MAGIC
# MAGIC df_trans_update.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("TRANSACTIONS")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from devcatalog.bronze.transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists claims
# MAGIC (
# MAGIC   ClaimID String,TransactionID String, PatientID    String, EncounterID   String, ProviderID   String, DeptID     String, ServiceDate   DATE, ClaimDate    DATE, PayorID     String,ClaimAmount DECIMAL(18,2),PaidAmount DECIMAL(18,2),ClaimStatus String,PayorType String,Deductible DECIMAL(18,2),Coinsurance DECIMAL(18,2),Copay DECIMAL(18,2), InsertDate   TIMESTAMP, ModifiedDate  TIMESTAMP, Inserted_date_bronze TIMESTAMP, Updated_date_bronze TIMESTAMP
# MAGIC )

# COMMAND ----------

claims_schema = "ClaimID String,TransactionID String, PatientID    String, EncounterID   String, ProviderID   String, DeptID     String, ServiceDate   DATE, ClaimDate    DATE, PayorID     String,ClaimAmount DECIMAL(18,2),PaidAmount DECIMAL(18,2),ClaimStatus String,PayorType String,Deductible DECIMAL(18,2),Coinsurance DECIMAL(18,2),Copay DECIMAL(18,2), InsertDate   TIMESTAMP, ModifiedDate  TIMESTAMP"

df_claims = spark.read.schema(claims_schema).option("header", "true").csv("abfss://outputexternal@databrciksadlsstgaccount.dfs.core.windows.net/OutputFolder/AzureSQLDB/CLAIMS")

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

