# Databricks notebook source
# File location and type
file_location = "/FileStore/tables/test_task_dataset_summer_products.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# Create a view or table

temp_table_name = "test_task_dataset_summer_products_csv"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `test_task_dataset_summer_products_csv`

# COMMAND ----------

permanent_table_name = "summer_products"

df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   avg(price),
# MAGIC   (sum(rating_five_count) / sum(rating_count)) * 100 as five_percentage,
# MAGIC   origin_country
# MAGIC from
# MAGIC   summer_products
# MAGIC group by
# MAGIC   origin_country
# MAGIC order by
# MAGIC   origin_country

# COMMAND ----------

from pyspark.sql import functions as F

res_df = df.select('price','origin_country','rating_five_count','rating_count').groupby('origin_country')\
      .agg(F.avg('price'), F.sum('rating_five_count').alias('sum_rating_five_count'), F.sum('rating_count').alias('sum_rating_count') )\
      .withColumn('five_percentage', F.col('sum_rating_five_count')/F.col('sum_rating_count')*100)\
      .orderBy('origin_country').drop('sum_rating_five_count').drop('sum_rating_count')

display(res_df)

# COMMAND ----------


