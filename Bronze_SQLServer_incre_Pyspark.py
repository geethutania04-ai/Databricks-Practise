# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog devcatalog;
# MAGIC use schema bronze;

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

delta_df = DeltaTable.forName(spark, "devcatalog.bronze.encounters")

# COMMAND ----------

# MAGIC %md
# MAGIC patients_schema = "PatientID String,FirstName String,LastName String,MiddleName String,SSN String, PhoneNumber String,Gender String,DOB DATE, Address    String,  ModifiedDate DATE, Inserted_date_bronze TIMESTAMP, Updated_date_bronze TIMESTAMP"
# MAGIC
# MAGIC #df_pat = spark.read.schema(patients_schema).option("header", "true").csv("abfss://outputexternal@databrciksadlsstgaccount.dfs.core.windows.net/OutputFolder/SQLServer/PATIENTS")
# MAGIC
# MAGIC df_pat = spark.read.option("header", "true").option("inferschema","true").csv("abfss://outputexternal@databrciksadlsstgaccount.dfs.core.windows.net/OutputFolder/SQLServer/PATIENTS")
# MAGIC df_pat.printSchema()

# COMMAND ----------

df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .csv("abfss://outputexternal@databrciksadlsstgaccount.dfs.core.windows.net/OutputFolder/SQLServer/PATIENTS")


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

# MAGIC %md
# MAGIC select * from patients
