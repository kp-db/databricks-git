# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC This notebook will show you how to create and query a table or DataFrame on AWS S3.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 1: Data location and type
# MAGIC 
# MAGIC There are two ways in Databricks to read from S3. You can either read data using [an IAM Role](https://docs.databricks.com/user-guide/cloud-configurations/aws/iam-roles.html) or read data using Access Keys.
# MAGIC 
# MAGIC **We recommend leveraging IAM Roles in Databricks in order to specify which cluster can access which buckets. Keys can show up in logs and table metadata and are therefore fundamentally insecure.** If you do use keys, you'll have to escape the `/` in yourkeys with `%2F`.
# MAGIC 
# MAGIC 
# MAGIC Now to get started, we'll need to set the location and type of the file. We'll do this using [widgets](https://docs.databricks.com/user-guide/notebooks/widgets.html). Widgets allow us to parameterize the exectuion of this entire notebook. First we'll create them, then we'll be able to reference them throughout the notebook. 

# COMMAND ----------

dbutils.widgets.text("file_location", "s3a:/example/location", "Upload Location")
dbutils.widgets.dropdown("file_type", "csv", ["csv", 'parquet', 'json'])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 2: Reading the data
# MAGIC 
# MAGIC Now that we specified our file metadata, we can create a DataFrame. You'll notice that we use an *option* to specify that we'd like to infer the schema from the file. We can also explicitly set this to a particular schema if we have one already.
# MAGIC 
# MAGIC First, let's create a DataFrame in Python.

# COMMAND ----------

df = spark.read.format(dbutils.widgets.get("file_type")).option("inferSchema", "true").load(dbutils.widgets.get("file_location"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 3: Querying the data
# MAGIC 
# MAGIC Now that we created our DataFrame. We can query it. For instance, you can select some particular columns to select and display within Databricks.

# COMMAND ----------

display(df.select("EXAMPLE_COLUMN"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 4: (Optional) Create a view or table
# MAGIC 
# MAGIC If you'd like to be able to use query this data as a table, it is simple to register it as a *view* or a table.

# COMMAND ----------

df.createOrReplaceTempView("YOUR_TEMP_VIEW_NAME")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC We can query this using Spark SQL. For instance, we can perform a simple aggregation. Notice how we can use `%sql` in order to query the view from SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT EXAMPLE_GROUP, SUM(EXAMPLE_AGG) FROM YOUR_TEMP_VIEW_NAME GROUP BY EXAMPLE_GROUP

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table. You can also create a table from the DataFrame.

# COMMAND ----------

df.write.format("parquet").saveAsTable("MY_PERMANENT_TABLE_NAME")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This table will persist across cluster restarts as well as allow various users across different notebooks to query this data.