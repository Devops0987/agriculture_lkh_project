# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql.functions import  col, lit

# COMMAND ----------

dbutils.widgets.text("catalog", "agriculture_lkh_dev", "Catalog")
dbutils.widgets.text("source_data", "db", "Source data")
dbutils.widgets.text("ingestion_date", "", "Ingestion date")



# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog")
source_data = dbutils.widgets.get("source_data")
ingestion_date = dbutils.widgets.get("ingestion_date")


# COMMAND ----------

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.bronze")
spark.sql(f"USE SCHEMA bronze")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.bronze.metadata")


# COMMAND ----------

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .option("mode", "PERMISSIVE") # Options: PERMISSIVE, DROPMALFORMED, FAILFAST
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .load(f"{source_data}*.csv") 
)

# COMMAND ----------

df = (
    df.withColumnRenamed("Farm_Area(acres)", "Farm_Area_acres")
    .withColumnRenamed("Fertilizer_Used(tons)", "Fertilizer_Used_tons")
    .withColumnRenamed("Pesticide_Used(kg)", "Pesticide_Used_kg")
    .withColumnRenamed("Yield(tons)", "Yield_tons")
    .withColumnRenamed("Water_Usage(cubic meters)", "Water_Usage_cubic_meters")
    .withColumn("ingestion_date", lit(ingestion_date).cast("timestamp"))
    .withColumn("source_file", col("_metadata.file_name"))
)

# COMMAND ----------

df.show()

# COMMAND ----------

df.write.mode("append").option("delta.columnMapping.mode", "name").option("mergeSchema", "true").saveAsTable(f"{catalog_name}.bronze.farm_raw")

# COMMAND ----------

bronze_df = spark.read.table(f"{catalog_name}.bronze.farm_raw")
bronze_df.show()

# COMMAND ----------

bronze_df.count()

# COMMAND ----------

print("test")

# COMMAND ----------

