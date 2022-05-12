# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times folder

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
# MAGIC ##### Step 1 - Read the csv files

# COMMAND ----------

#pre-set schema
lap_times_schema = 'raceId INT, driverId INT, lap INT, position INT, time STRING, milliseconds INT'

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f'{raw_folder_path}/{v_file_date}/lap_times/lap_times_split*.csv')

display(lap_times_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns & Add ingestion date and data source

# COMMAND ----------

from pyspark.sql.functions import lit
lap_times_final = add_ingestion_date(lap_times_df).withColumnRenamed('raceId', 'race_id') \
                                                  .withColumnRenamed('driverId', 'driver_id') \
                                                  .withColumn('data_source', lit(v_data_source)) \
                                                  .withColumn('file_date', lit(v_file_date))

display(lap_times_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write data to datalake as parquet

# COMMAND ----------

# overwrite_partition(lap_times_final, 'f1_processed', 'lap_times', 'race_id')

# COMMAND ----------

merge_condition = 'target.driver_id = source.driver_id AND target.lap = source.lap AND target.race_id = source.race_id'
merge_delta_data(lap_times_final, 'f1_processed', 'lap_times', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.lap_times;

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(*)
# MAGIC FROM f1_processed.lap_times
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;