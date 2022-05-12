# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):
    output_df = input_df.withColumn('ingestion_date', current_timestamp())
    return output_df

# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):
    column_names = input_df.schema.names
    column_names.remove(partition_column)
    column_names.append(partition_column)
    output_df = input_df.select(column_names)
    return output_df

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
    output_df = re_arrange_partition_column(input_df, partition_column)
    spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
    if (spark._jsparkSession.catalog().tableExists(f'{db_name}.{table_name}')):
        output_df.write.mode('overwrite').insertInto(f'{db_name}.{table_name}')
    else:
        output_df.write.mode('overwrite').partitionBy(partition_column).format('parquet').saveAsTable(f'{db_name}.{table_name}')

# COMMAND ----------

def df_column_to_list(input_df, column_name):
    output = input_df.select(column_name).distinct().collect()
    column_list = [row[column_name] for row in output]
    return column_list

# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column):
    spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
    from delta.tables import DeltaTable
    if (spark._jsparkSession.catalog().tableExists(f'{db_name}.{table_name}')):
        deltaTable = DeltaTable.forPath(spark, f'{folder_path}/{table_name}')
        deltaTable.alias('target').merge(input_df.alias('source'), merge_condition) \
                                  .whenMatchedUpdateAll() \
                                  .whenNotMatchedInsertAll() \
                                  .execute()
    else:
        input_df.write.mode('overwrite').partitionBy(partition_column).format('delta').saveAsTable(f'{db_name}.{table_name}')