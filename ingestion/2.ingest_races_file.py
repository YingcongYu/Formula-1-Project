# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

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
# MAGIC ##### Step 1 - Read the csv file

# COMMAND ----------

#pre-set schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
races_schema = StructType(fields = [StructField('raceId', IntegerType(), False),
                                    StructField('year', IntegerType(), True),
                                    StructField('round', IntegerType(), True),
                                    StructField('circuitId', IntegerType(), True),
                                    StructField('name', StringType(), True),
                                    StructField('date', StringType(), True),
                                    StructField('time', StringType(), True),
                                    StructField('url', StringType(), True)
])

# COMMAND ----------

races_df = spark.read \
.option('header', True) \
.schema(races_schema) \
.csv(f'{raw_folder_path}/{v_file_date}/races.csv')

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Combine column date and time

# COMMAND ----------

from pyspark.sql.functions import col, lit, to_timestamp, concat
races_combined = races_df.withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))
#display() cannot show the ' ' between date and time, so use show()
races_combined.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Select & Rename only the required columns

# COMMAND ----------

races_required = races_combined.select(col('raceId').alias('race_id') \
                                       ,col('year').alias('race_year') \
                                       ,col('round') \
                                       ,col('circuitId').alias('circuit_id') \
                                       ,col('name') \
                                       ,col('race_timestamp')
)

display(races_required)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Add ingestion date and data source to the dataframe

# COMMAND ----------

races_final = add_ingestion_date(races_required).withColumn('data_source', lit(v_data_source)) \
.withColumn('file_date', lit(v_file_date))

display(races_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Write data to datalake as parquet

# COMMAND ----------

races_final.write.mode('overwrite').format('delta').saveAsTable('f1_processed.races')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races;

# COMMAND ----------

dbutils.notebook.exit('Success')