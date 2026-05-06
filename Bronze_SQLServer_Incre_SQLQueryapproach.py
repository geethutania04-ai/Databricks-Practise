# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog devcatalog;
# MAGIC use schema bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table encounters
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table Encounters
# MAGIC (
# MAGIC EncounterID string,
# MAGIC PatientID string ,
# MAGIC EncounterDate DATE,
# MAGIC EncounterType string,
# MAGIC ProviderID string ,
# MAGIC DepartmentID string ,
# MAGIC ProcedureCode INT,
# MAGIC InsertedDate DATE,
# MAGIC ModifiedDate DATE,
# MAGIC Inserted_date_bronze timestamp,
# MAGIC Updated_date_bronze timestamp
# MAGIC )

# COMMAND ----------

encounters_schema = "EncounterID STRING, PatientID STRING, EncounterDate DATE, EncounterType STRING, ProviderID STRING, DepartmentID STRING, ProcedureCode INT, InsertedDate DATE, ModifiedDate DATE, Inserted_date_bronze TIMESTAMP, Updated_date_bronze TIMESTAMP"

df_enc = spark.read.schema(encounters_schema).option("header", "true").csv("abfss://outputexternal@databrciksadlsstgaccount.dfs.core.windows.net/OutputFolder/SQLServer/ENCOUNTERS")

df_enc.createOrReplaceTempView("encounters_view_source")


# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO devcatalog.bronze.encounters AS Target
# MAGIC USING encounters_view_source AS Source
# MAGIC ON (Target.encounterid = Source.encounterid)
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET Target.PatientID = Source.PatientID,
# MAGIC                 Target.EncounterDate  = Source.EncounterDate ,
# MAGIC                 Target.EncounterType = Source.EncounterType,
# MAGIC                 Target.ProviderID = Source.ProviderID,
# MAGIC                 Target.DepartmentID = Source.DepartmentID,
# MAGIC                 Target.ProcedureCode = Source.ProcedureCode,
# MAGIC                 Target.InsertedDate = Source.InsertedDate,
# MAGIC                 Target.ModifiedDate = Source.ModifiedDate,
# MAGIC                 Target.Updated_date_bronze= current_timestamp
# MAGIC WHEN NOT MATCHED BY TARGET THEN
# MAGIC     INSERT (EncounterID, PatientID, EncounterDate, EncounterType, ProviderID, DepartmentID, ProcedureCode, InsertedDate, ModifiedDate, Inserted_date_bronze, Updated_date_bronze)
# MAGIC     VALUES (Source.EncounterID, Source.PatientID,Source.EncounterDate,Source.EncounterType,Source.ProviderID, Source.DepartmentID, Source.ProcedureCode, Source.InsertedDate, Source.ModifiedDate, current_timestamp(), current_timestamp());

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from encounters;
