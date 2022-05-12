# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest results.json file

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
# MAGIC ##### Step 1 - Read the results.json file

# COMMAND ----------

#pre-set schema
results_schema = 'resultId INT, \
                  raceId INT, \
                  driverId INT, \
                  constructorId INT, \
                  number INT, \
                  grid INT, \
                  position INT, \
                  positionText STRING, \
                  positionOrder INT, \
                  points FLOAT, \
                  laps INT, \
                  time STRING, \
                  milliseconds INT, \
                  fastestLap INT, \
                  rank INT, \
                  fastestLapTime STRING, \
                  fastestLapSpeed FLOAT, \
                  statusId INT'

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json(f'{raw_folder_path}/{v_file_date}/results.json')

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Drop unwanted columns

# COMMAND ----------

results_dropped = results_df.drop('statusId')

display(results_dropped)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename columns & Add ingestion date and data source

# COMMAND ----------

from pyspark.sql.functions import lit
results_final = add_ingestion_date(results_dropped).withColumnRenamed('resultId', 'result_id') \
                                                   .withColumnRenamed('raceId', 'race_id') \
                                                   .withColumnRenamed('driverId', 'driver_id') \
                                                   .withColumnRenamed('constructorId', 'constructor_id') \
                                                   .withColumnRenamed('positionText', 'position_text') \
                                                   .withColumnRenamed('positionOrder', 'position_order') \
                                                   .withColumnRenamed('fastestLap', 'fastest_lap') \
                                                   .withColumnRenamed('fastestLapTime', 'fastest_lap_time') \
                                                   .withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed') \
                                                   .withColumn('data_source', lit(v_data_source)) \
                                                   .withColumn('file_date', lit(v_file_date))

display(results_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write data to datalake as parquet

# COMMAND ----------

# for race_id_list in results_final.select('race_id').distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists('f1_processed.results')):
#         spark.sql(f'ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})')
        
# results_final.write.mode('append').partitionBy('race_id').format('delta').saveAsTable('f1_processed.results')

# COMMAND ----------

merge_condition = 'target.result_id = source.result_id AND target.race_id = source.race_id'
merge_delta_data(results_final, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.results;

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(*)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;