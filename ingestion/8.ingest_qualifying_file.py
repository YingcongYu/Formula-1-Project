# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying folder

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
# MAGIC ##### Step 1 - Read the json files

# COMMAND ----------

#pre-set schema
qualifying_schema = 'qualifyId INT, raceId INT, driverId INT, constructorId INT, number INT, position INT, q1 STRING, q2 STRING, q3 STRING'

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option('multiLine', True) \
.json(f'{raw_folder_path}/{v_file_date}/qualifying')

display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns & Add ingestion date and data source

# COMMAND ----------

from pyspark.sql.functions import lit
qualifying_final = add_ingestion_date(qualifying_df).withColumnRenamed('qualifyId', 'qualify_id') \
                                                    .withColumnRenamed('raceId', 'race_id') \
                                                    .withColumnRenamed('driverId', 'driver_id') \
                                                    .withColumnRenamed('constructorId', 'constructor_id') \
                                                    .withColumn('data_source', lit(v_data_source)) \
                                                    .withColumn('file_date', lit(v_file_date))

display(qualifying_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write data to datalake as parquet

# COMMAND ----------

# overwrite_partition(qualifying_final, 'f1_processed', 'qualifying', 'race_id')

# COMMAND ----------

merge_condition = 'target.qualify_id = source.qualify_id AND target.race_id = source.race_id'
merge_delta_data(qualifying_final, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.qualifying;

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(*)
# MAGIC FROM f1_processed.qualifying
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;