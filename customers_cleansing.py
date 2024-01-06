# Databricks notebook source
# Set your Azure Storage account details
storage_account_name = "storagedemoproject1"
storage_account_key = "u155sn/6P9XCMH0q1xNw6VkQrgBJSu3cXxcaSSwd2TG7AL2r9Ejvb9usdB3Juxw1dUPSY0Kwl3im+AStxslwfQ=="
container_name = "raw"
file_path = "raw/customers"

# Create a connection string
connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={storage_account_key};EndpointSuffix=core.windows.net"

# Mount Azure Storage as a DBFS (Databricks File System) mount point
dbutils.fs.mount(
    source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
    mount_point = "/mnt/pro",
    extra_configs = {"fs.azure.account.key." + storage_account_name + ".blob.core.windows.net": storage_account_key}
)

# COMMAND ----------

path = "/mnt/pro/customers"
df = spark.read.parquet(path)
df = df.orderBy("patient_id")
df.display()

# COMMAND ----------

from pyspark.sql import col
df.filter(col("first_name").islower())
df.dropDuplicates()
df.display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

df.createOrReplaceTempView("customers")

# COMMAND ----------

df = df.dropDuplicates()
df.display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import initcap,upper
df = df.withColumn("first_name", initcap("first_name")).withColumn("last_name", initcap("last_name")).withColumn("gender", initcap("gender")).withColumn("address", upper("address"))
#df.display()

# COMMAND ----------

df = df.drop_duplicates()
#df.display()

# COMMAND ----------

storage_account_name = "storagedemoproject1"
storage_account_key = "u155sn/6P9XCMH0q1xNw6VkQrgBJSu3cXxcaSSwd2TG7AL2r9Ejvb9usdB3Juxw1dUPSY0Kwl3im+AStxslwfQ=="
container_name = "consumption"
spark.conf.set("fs.azure.account.key.{0}.dfs.core.windows.net".format(storage_account_name), storage_account_key)

target_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/customer_parquet"


# Write the DataFrame to Parquet format in the specified container and path
df.write.parquet(target_path, mode="overwrite")



# COMMAND ----------

# Example ADLS Gen2 account name
account_name = "storagedemoproject1"

# container name
container_name = "consumption"

# Delta table path within the container
delta_table_path = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/customers_delta"

df.write.format("delta").mode("append").save(delta_table_path)


# COMMAND ----------


