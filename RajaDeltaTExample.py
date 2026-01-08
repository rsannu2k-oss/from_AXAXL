# Databricks notebook source

# Load a file into a dataframe
#df = spark.read.load('/data/mydata.csv', format='csv', header=True)
df = spark.read.load('dbfs:/FileStore/test/RSample/ProductsDelta.csv', format='csv', header=True)

# Save the dataframe as a delta table
#delta_table_path = "/delta/mydata"
delta_table_path = "dbfs:/FileStore/test/RSample/ProductsDelta.csv"

df.write.format("delta").save(delta_table_path)

#You can replace an existing Delta Lake table with the contents of a dataframe by using the overwrite mode, as shown here:
new_df.write.format("delta").mode("overwrite").save(delta_table_path)

#You can also add rows from a dataframe to an existing table by using the append mode:
new_rows_df.write.format("delta").mode("append").save(delta_table_path)



# COMMAND ----------

#Making conditional updates

from delta.tables import *
from pyspark.sql.functions import *

# Create a deltaTable object
deltaTable = DeltaTable.forPath(spark, delta_table_path)

# Update the table (reduce price of accessories by 10%)
deltaTable.update(
    condition = "Category == 'Accessories'",
    set = { "Price": "Price * 0.9" })


# COMMAND ----------

#Querying a previous version of a table
df = spark.read.format("delta").option("versionAsOf", 0).load(delta_table_path)


# COMMAND ----------

#Alternatively, you can specify a timestamp by using the timestampAsOf option:
df = spark.read.format("delta").option("timestampAsOf", '2022-01-01').load(delta_table_path)


# COMMAND ----------


#Creating catalog tables
##Creating a catalog table from a dataframe

# Save a dataframe as a managed table
df.write.format("delta").saveAsTable("MyManagedTable")

## specify a path option to save as an external table
df.write.format("delta").option("path", "/mydata").saveAsTable("MyExternalTable")


# COMMAND ----------

#Creating a catalog table using SQL
##
spark.sql("CREATE TABLE MyExternalTable USING DELTA LOCATION '/mydata'")

%sql

CREATE TABLE MyExternalTable
USING DELTA
LOCATION '/mydata'


# COMMAND ----------

#Defining the table schema

%sql

CREATE TABLE ManagedSalesOrders
(
    Orderid INT NOT NULL,
    OrderDate TIMESTAMP NOT NULL,
    CustomerName STRING,
    SalesTotal FLOAT NOT NULL
)
USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC --from delta.tables import *
# MAGIC
# MAGIC use dw_xle
# MAGIC
# MAGIC

# COMMAND ----------

#Using the DeltaTableBuilder API

from delta.tables import *

DeltaTable.create(spark) \
  .tableName("ManagedProducts") \
  #.tableName("default.ManagedProducts") \
  .addColumn("Productid", "INT") \
  .addColumn("ProductName", "STRING") \
  .addColumn("Category", "STRING") \
  .addColumn("Price", "FLOAT") \
  .execute()


# COMMAND ----------

#Using catalog tables
%sql

SELECT orderid, salestotal
FROM ManagedSalesOrders
