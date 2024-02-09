# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %sql
# MAGIC create
# MAGIC or replace table `uat-sandpit-datahub-bronze`.anoj.partition_testing (
# MAGIC   id bigint,
# MAGIC   name string,
# MAGIC   category bigint,
# MAGIC   random_value double,
# MAGIC   odd_or_even string
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC create
# MAGIC or replace table `uat-sandpit-datahub-bronze`.anoj.partition_testing_partitioned (
# MAGIC   id bigint,
# MAGIC   name string,
# MAGIC   category bigint,
# MAGIC   random_value double,
# MAGIC   odd_or_even string
# MAGIC )
# MAGIC partitioned by (odd_or_even)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `uat-sandpit-datahub-bronze`.anoj.partition_testing

# COMMAND ----------

# Generate sample data (you can replace this with your own data)
num_rows = 10000000  # Adjust the number of rows as needed
data = [(i, f"Name_{i}", i % 5) for i in range(num_rows)]

# Create a DataFrame
columns = ["id", "name", "category"]
df = spark.createDataFrame(data, columns)

# Add some random values to a new column
df = df.withColumn("random_value", F.rand())

# COMMAND ----------

df = df.withColumn('odd_or_even', F.when(F.col('id') % 2 == 0, 'even').otherwise('odd'))

# COMMAND ----------

# df.display()
# df.filter(df.odd_or_even == 'even').display()

# COMMAND ----------

(df
 .write
 .mode('overwrite')
 .saveAsTable('`uat-sandpit-datahub-bronze`.anoj.partition_testing')
 )

# COMMAND ----------

(df
 .write
 .mode('overwrite')
 .saveAsTable('`uat-sandpit-datahub-bronze`.anoj.partition_testing_partitioned')
 )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `uat-sandpit-datahub-bronze`.anoj.partition_testing
# MAGIC where odd_or_even == 'even'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `uat-sandpit-datahub-bronze`.anoj.partition_testing_partitioned
# MAGIC where odd_or_even == 'even'

# COMMAND ----------

# %sql
# create
# or replace table `uat-sandpit-datahub-bronze`.anoj.partition_testing_partitioned 
# partitioned by (odd_or_even) as
# select
#   *
# from
#   `uat-sandpit-datahub-bronze`.anoj.partition_testing
