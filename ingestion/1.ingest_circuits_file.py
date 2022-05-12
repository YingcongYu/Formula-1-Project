# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

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
# MAGIC ##### Step 1 - Read the csv file using Spark DataFrame Reader

# COMMAND ----------

#look for file locations - step 1
display(dbutils.fs.mounts())

# COMMAND ----------

#look for file locations - step 2
display(dbutils.fs.ls(raw_folder_path))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

#pre-set schema to avoid an addtional spark job on inferring schema and save resources & time
circuits_schema = StructType(fields = [StructField('circuitId', IntegerType(), False),
                                       StructField('circuitRef', StringType(), True),
                                       StructField('name', StringType(), True),
                                       StructField('location', StringType(), True),
                                       StructField('country', StringType(), True),
                                       StructField('lat', DoubleType(), True),
                                       StructField('lng', DoubleType(), True),
                                       StructField('alt', IntegerType(), True),
                                       StructField('url', StringType(), True)
    
])

# COMMAND ----------

circuits_df = spark.read \
.option('header', True) \
.schema(circuits_schema) \
.csv(f'{raw_folder_path}/{v_file_date}/circuits.csv')

# COMMAND ----------

#display() can show the dataframe without truncate, but show() with
display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select & Rename only the required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_required = circuits_df.select(col('circuitId').alias('circuit_id') \
                                      , col('circuitRef').alias('circuit_ref') \
                                      , col('name') \
                                      , col('location') \
                                      , col('country') \
                                      , col('lat').alias('latitude') \
                                      , col('lng').alias('longtitude') \
                                      , col('alt').alias('altitude'))

# COMMAND ----------

display(circuits_required)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Add ingestion date and data source to the dataframe

# COMMAND ----------

from pyspark.sql.functions import lit
circuits_final = add_ingestion_date(circuits_required).withColumn('data_source', lit(v_data_source)) \
.withColumn('file_date', lit(v_file_date))

# COMMAND ----------

display(circuits_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write data to datalake as parquet

# COMMAND ----------

circuits_final.write.mode('overwrite').format('delta').saveAsTable('f1_processed.circuits')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits;

# COMMAND ----------

dbutils.notebook.exit('Success')