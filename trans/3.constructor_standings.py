# Databricks notebook source
dbutils.widgets.text('p_file_date', '2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run ../includes/configurations

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

race_results_list = spark.read.format('delta').load(f'{presentation_folder_path}/race_results') \
.filter(f'file_date = "{v_file_date}"')

race_year_list = df_column_to_list(race_results_list, 'race_year')

# COMMAND ----------

from pyspark.sql.functions import *
race_results = spark.read.format('delta').load(f'{presentation_folder_path}/race_results') \
.filter(col('race_year').isin(race_year_list))

display(race_results)

# COMMAND ----------

constructor_standings = race_results.groupBy('race_year', 'team') \
                               .agg(sum('points').alias('total_points'), count(when(col('position') == 1, True)).alias('wins'))

display(constructor_standings)

# COMMAND ----------

from pyspark.sql.window import Window
constructor_rank_spec = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))
final_df = constructor_standings.withColumn('rank', rank().over(constructor_rank_spec))

display(final_df.filter('race_year = 2020'))

# COMMAND ----------

# overwrite_partition(final_df, 'f1_presentation', 'constructor_standings', 'race_year')

# COMMAND ----------

merge_condition = 'target.team = source.team AND target.race_year = source.race_year'
merge_delta_data(final_df, 'f1_presentation', 'constructor_standings', presentation_folder_path, merge_condition, 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.constructor_standings;