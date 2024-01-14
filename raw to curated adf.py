# Databricks notebook source
# MAGIC %md
# MAGIC ### Data transformations 01

# COMMAND ----------

table_name = []

for i in dbutils.fs.ls('mnt/raw/dbo/'):
    table_name.append(i.name.split('/')[0])
#changes
#adsfkjdskf

# COMMAND ----------

table_name

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, date_format
from pyspark.sql.types import TimestampType

for i in table_name:
	path = '/mnt/raw/dbo/' + i + '/' + i + '.parquet'
	df = spark.read.format('parquet').load(path)
	column = df.columns

	for col in column:
		if "created_at" in col or "Created_at" in col:
			df = df.withColumn(col, date_format(from_utc_timestamp(df[col].cast(TimestampType()), "UTC"), "yyyy-MM-dd"))
	output_path = '/mnt/curated/dbo/' +i +'/'
	df.write.format('delta').mode("overwrite").save(output_path)
