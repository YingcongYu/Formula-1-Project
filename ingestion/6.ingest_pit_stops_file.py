# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pit_stops.json file

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date', '2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run ../includes/configurations

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the pit_stops.json file

# COMMAND ----------

#pre-set schema
pit_stops_schema = 'raceId INT, driverId INT, stop INT, lap INT, time STRING, duration STRING, milliseconds INT'

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option('multiLine', True) \
.json(f'{raw_folder_path}/{v_file_date}/pit_stops.json')

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns & Add ingestion date and data source

# COMMAND ----------

from pyspark.sql.functions import lit
pit_stops_final = add_ingestion_date(pit_stops_df).withColumnRenamed('raceId', 'race_id') \
                                                  .withColumnRenamed('driverId', 'driver_id') \
                                                  .withColumn('data_source', lit(v_data_source)) \
                                                  .withColumn('file_date', lit(v_file_date))

display(pit_stops_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write data to datalake as parquet

# COMMAND ----------

# overwrite_partition(pit_stops_final, 'f1_processed', 'pit_stops', 'race_id')

# COMMAND ----------

merge_condition = 'target.driver_id = source.driver_id AND target.stop = source.stop AND target.race_id = source.race_id'
merge_delta_data(pit_stops_final, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.pit_stops;

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(*)
# MAGIC FROM f1_processed.pit_stops
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;