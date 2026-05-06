# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog devcatalog;
# MAGIC use schema bronze;

# COMMAND ----------

df_DEP = spark.read.option("header", "true").option("inferschema","true").csv("abfss://outputexternal@databrciksadlsstgaccount.dfs.core.windows.net/OutputFolder/SQLServer/DEPARTMENTS")
display(df_DEP)

# COMMAND ----------

df_DEP.write.mode("overwrite").saveAsTable("DEPARTMENTS")

# COMMAND ----------

# MAGIC %md
# MAGIC SELECT * FROM departments

# COMMAND ----------

df_PROV = spark.read.option("header", "true").option("inferschema","true").csv("abfss://outputexternal@databrciksadlsstgaccount.dfs.core.windows.net/OutputFolder/SQLServer/PROVIDERS")
df_PROV.write.mode("overwrite").saveAsTable("PROVIDERS")

# COMMAND ----------

# MAGIC %md
# MAGIC SELECT * FROM PROVIDERS

# COMMAND ----------

# MAGIC %md
# MAGIC df_pat = spark.read.option("header", "true").option("mergeschema","true").csv("abfss://outputexternal@databrciksadlsstgaccount.dfs.core.windows.net/OutputFolder/SQLServer/PATIENTS")
# MAGIC df_pat_update = df_pat.withColumn("DOB",to_date(col("DOB"))).withColumn("ModifiedDate",to_timestamp(col("ModifiedDate")))
# MAGIC df_pat_update.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("PATIENTS")

# COMMAND ----------

# MAGIC %md
# MAGIC select count(*) from patients;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not exists PATIENTS(
# MAGIC   PatientID String,FirstName String,LastName String,MiddleName String,SSN String, PhoneNumber String,Gender String,DOB DATE, Address    String,  ModifiedDate DATE, Inserted_date_bronze TIMESTAMP, Updated_date_bronze TIMESTAMP
# MAGIC )

# COMMAND ----------

patients_schema = "PatientID String,FirstName String,LastName String,MiddleName String,SSN String, PhoneNumber String,Gender String,DOB DATE, Address    String,  ModifiedDate DATE, Inserted_date_bronze TIMESTAMP, Updated_date_bronze TIMESTAMP"

df_pat = spark.read.schema(patients_schema).option("header", "true").csv("abfss://outputexternal@databrciksadlsstgaccount.dfs.core.windows.net/OutputFolder/SQLServer/PATIENTS")
df_pat.printSchema()

# COMMAND ----------

from delta.tables import DeltaTable

delta_df = DeltaTable.forName(spark, "devcatalog.bronze.patients")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

delta_df.alias("target").merge(
    source = df_pat.alias("source"),
    condition = "target.patientID = source.patientID"
).whenMatchedUpdate(set = 
                    {
                        "FirstName": "source.FirstName",
                        "LastName": "source.LastName",
                        "MiddleName": "source.MiddleName",
                        "SSN": "source.SSN",
                        "PhoneNumber": "source.PhoneNumber",
                        "Gender": "source.Gender",
                        "DOB": "source.DOB",
                        "Address": "source.Address",
                        "ModifiedDate": "source.ModifiedDate",
                        "Updated_date_bronze": current_timestamp()
                    }
                    ).whenNotMatchedInsert(values = 
                    {
                        "patientID": "source.patientID",
                        "FirstName": "source.FirstName",
                        "LastName": "source.LastName",
                        "MiddleName": "source.MiddleName",
                        "SSN": "source.SSN",
                        "PhoneNumber": "source.PhoneNumber",
                        "Gender": "source.Gender",
                        "DOB": "source.DOB",
                        "Address": "source.Address",
                        "ModifiedDate": "source.ModifiedDate",
                        "Inserted_date_bronze": current_timestamp(), 
                        "Updated_date_bronze": current_timestamp()
                    }
                    ).execute()


# COMMAND ----------

# MAGIC %sql
# MAGIC Create table if not exists Encounters
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

from delta.tables import DeltaTable


delta_df = DeltaTable.forName(spark, "devcatalog.bronze.encounters")
from pyspark.sql.functions import current_timestamp


delta_df.alias("target").merge(
    source = df_enc.alias("source"),
    condition = "target.EncounterID = source.EncounterID"
).whenMatchedUpdate(set = 
                    {
                        "PatientID": "source.PatientID",
                        "EncounterDate": "source.EncounterDate",
                        "EncounterType": "source.EncounterType",
                        "ProviderID": "source.ProviderID",
                        "DepartmentID": "source.DepartmentID",                        
                        "ProcedureCode": "source.ProcedureCode",
                        "InsertedDate": "source.InsertedDate",
                        "ModifiedDate": "source.ModifiedDate",                
                        "Updated_date_bronze": current_timestamp()
                    }
                    ).whenNotMatchedInsert(values = 
                    {
                        "EncounterID": "source.EncounterID",
                        "PatientID": "source.PatientID",
                        "EncounterDate":"source.EncounterDate",
                        "EncounterType": "source.EncounterType",
                        "ProviderID": "source.ProviderID",
                        "DepartmentID": "source.DepartmentID",                        
                        "ProcedureCode": "source.ProcedureCode",
                        "InsertedDate": "source.InsertedDate",
                        "ModifiedDate": "source.ModifiedDate",
                        "Inserted_date_bronze": current_timestamp(), 
                        "Updated_date_bronze": current_timestamp()
                    }
                    ).execute()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from devcatalog.bronze.encounters
