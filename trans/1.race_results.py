# Databricks notebook source
dbutils.widgets.text('p_file_date', '2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read all data

# COMMAND ----------

# MAGIC %run ../includes/configurations

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

races_df = spark.read.format('delta').load(f'{processed_folder_path}/races')
circuits_df = spark.read.format('delta').load(f'{processed_folder_path}/circuits')
drivers_df = spark.read.format('delta').load(f'{processed_folder_path}/drivers')
constructors_df = spark.read.format('delta').load(f'{processed_folder_path}/constructors')
results_df = spark.read.format('delta').load(f'{processed_folder_path}/results').filter(f'file_date = "{v_file_date}"')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename and select required columns

# COMMAND ----------

races_pre_join = races_df.withColumnRenamed('race_timestamp', 'race_date') \
                         .withColumnRenamed('name', 'race_name') \
                         .select('race_id', 'race_year', 'race_name', 'race_date', 'circuit_id')

display(races_pre_join)

# COMMAND ----------

circuits_pre_join = circuits_df.withColumnRenamed('location', 'circuit_location') \
                               .select('circuit_id', 'circuit_location')

display(circuits_pre_join)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join races table and circuits table

# COMMAND ----------

races_circuits = circuits_pre_join.join(races_pre_join, races_pre_join.circuit_id == circuits_pre_join.circuit_id) \
                                  .select('race_id', 'race_year', 'race_name', 'race_date', 'circuit_location')

display(races_circuits)

# COMMAND ----------

drivers_pre_join = drivers_df.withColumnRenamed('name', 'driver_name') \
                             .withColumnRenamed('number', 'driver_number') \
                             .withColumnRenamed('nationality', 'driver_nationality') \
                             .select('driver_id', 'driver_name', 'driver_number', 'driver_nationality')

display(drivers_pre_join)

# COMMAND ----------

constructors_pre_join = constructors_df.withColumnRenamed('name', 'team') \
                                       .select('constructor_id', 'team')

display(constructors_pre_join)

# COMMAND ----------

results_pre_join = results_df.withColumnRenamed('time', 'race_time') \
                             .withColumnRenamed('race_id', 'result_race_id') \
                             .withColumnRenamed('driver_id', 'result_driver_id') \
                             .withColumnRenamed('fastest_lap', 'fastest_lap') \
                             .withColumnRenamed('file_date', 'result_file_date') \
                             .select('result_driver_id', 'constructor_id', 'result_race_id', 'grid', 'fastest_lap', 'race_time', 'points', 'position', 'result_file_date')

display(results_pre_join)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join all tables together and select only the required columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
race_results = results_pre_join.join(drivers_pre_join, results_pre_join.result_driver_id == drivers_pre_join.driver_id) \
                               .join(constructors_pre_join, results_pre_join.constructor_id == constructors_pre_join.constructor_id) \
                               .join(races_circuits, results_pre_join.result_race_id == races_circuits.race_id) \
                               .select('race_id', 'race_year', 'race_name', 'race_date', 'circuit_location', 'driver_id', 'driver_name', 'driver_number', 'driver_nationality', 'team', 'grid', 'fastest_lap', 'race_time', 'points', 'position', 'result_file_date') \
                               .withColumn('created_date', current_timestamp()) \
                               .withColumnRenamed('result_file_date', 'file_date')

display(race_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save the data as parquet

# COMMAND ----------

# overwrite_partition(race_results, 'f1_presentation', 'race_results', 'race_id')

# COMMAND ----------

merge_condition = 'target.driver_id = source.driver_id AND target.race_id = source.race_id'
merge_delta_data(race_results, 'f1_presentation', 'race_results', presentation_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.race_results;