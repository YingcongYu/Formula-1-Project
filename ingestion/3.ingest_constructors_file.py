# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

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
# MAGIC ##### Step 1 - Read the constructors.json file

# COMMAND ----------

#pre-set schema
constructors_schema = 'constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING'

# COMMAND ----------

constructors_df = spark.read \
.schema(constructors_schema) \
.json(f'{raw_folder_path}/{v_file_date}/constructors.json')

display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Drop unwanted columns

# COMMAND ----------

constructors_dropped = constructors_df.drop('url')

display(constructors_dropped)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename columns & Add ingestion date and data source

# COMMAND ----------

from pyspark.sql.functions import lit
constructors_final = add_ingestion_date(constructors_dropped).withColumnRenamed('constructorId', 'constructor_id') \
                                                             .withColumnRenamed('constructorRef', 'constructor_ref') \
                                                             .withColumn('data_source', lit(v_data_source)) \
.withColumn('file_date', lit(v_file_date))

display(constructors_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write data to datalake as parquet

# COMMAND ----------

constructors_final.write.mode('overwrite').format('delta').saveAsTable('f1_processed.constructors')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors;

# COMMAND ----------

dbutils.notebook.exit('Success')