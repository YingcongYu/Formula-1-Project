# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest drivers.json file

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
# MAGIC ##### Step 1 - Read the drivers.json file

# COMMAND ----------

#pre-set schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
name_schema = StructType(fields = [StructField('forename', StringType(), True), 
                                   StructField('surname', StringType(), True)
])

drivers_schema = StructType(fields = [StructField('driverId', IntegerType(), False),
                                      StructField('driverRef', StringType(), True),
                                      StructField('number', IntegerType(), True),
                                      StructField('code', StringType(), True),
                                      StructField('name', name_schema),
                                      StructField('dob', DateType(), True),
                                      StructField('nationality', StringType(), True),
                                      StructField('url', StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read \
.schema(drivers_schema) \
.json(f'{raw_folder_path}/{v_file_date}/drivers.json')

display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns & Add ingestion date and data source

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit
drivers_with_column = add_ingestion_date(drivers_df).withColumnRenamed('driverId', 'driver_id') \
                                                    .withColumnRenamed('driverRef', 'driver_ref') \
                                                    .withColumn('name', concat(col('name.forename'), lit(' '), col('name.surname'))) \
                                                    .withColumn('data_source', lit(v_data_source)) \
.withColumn('file_date', lit(v_file_date))

display(drivers_with_column)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop unwanted columns

# COMMAND ----------

drivers_final = drivers_with_column.drop('url')

display(drivers_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write data to datalake as parquet

# COMMAND ----------

drivers_final.write.mode('overwrite').format('delta').saveAsTable('f1_processed.drivers')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers;

# COMMAND ----------

dbutils.notebook.exit('Success')