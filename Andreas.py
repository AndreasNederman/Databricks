# Databricks notebook source
blob_account_name = "lakenederman"
blob_container_name = "taxi"
blob_relative_path = "yellow.csv"
blob_sas_token = r"?sv=2020-02-10&ss=bfqt&srt=sco&sp=rwdlacupx&se=2021-03-17T15:21:55Z&st=2021-02-17T07:21:55Z&spr=https&sig=H7yLKj6ycKOB88ZME4%2F%2FKwSsAbehTAxukWoIf6FtFxA%3D"


# COMMAND ----------

wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path)
spark.conf.set('fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name), blob_sas_token)
print('Remote blob path: ' + wasbs_path)


# COMMAND ----------

taxidf = spark.read.format("csv")\
.option("header", "true")\
.option("inferSchema", "true")\
.load(wasbs_path)

print('Register the DataFrame as a SQL temporary view: source')
taxidf.createOrReplaceTempView('source')

# COMMAND ----------


taxidf.count()

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM source LIMIT 10

# COMMAND ----------

taxidf.show(5)


# COMMAND ----------

taxidf.printSchema()


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS taxidataset;
# MAGIC 
# MAGIC -- if you want to give more details
# MAGIC --CREATE DATABASE IF NOT EXISTS customer_db COMMENT 'This is customer database' LOCATION '/user'
# MAGIC --    WITH DBPROPERTIES (ID=001, Name='John');

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE EXTENDED taxidataset;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER DATABASE taxidataset;

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE taxidataset.nyctaxi (
# MAGIC vendorid INT,
# MAGIC tpep_pickup_datetime STRING,
# MAGIC tpep_dropoff_datetime STRING,
# MAGIC passenger_count INT,
# MAGIC trip_distance double,
# MAGIC ratecodeid INT,
# MAGIC store_and_fwd_flag string,
# MAGIC pulocationID INT,
# MAGIC dolocationID INT,
# MAGIC payment_type INT,
# MAGIC fare_amount double,
# MAGIC extra double,
# MAGIC mta_tax double,
# MAGIC tip_amount double,
# MAGIC tolls_amount double,
# MAGIC improvement_surcharge double,
# MAGIC total_amount double,
# MAGIC congestion_surcharge double)
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE taxidataset.nyctaxi

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT * FROM source

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO TABLE taxidataset.nyctaxi SELECT * FROM source;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM taxidataset.nyctaxi LIMIT 10

# COMMAND ----------

