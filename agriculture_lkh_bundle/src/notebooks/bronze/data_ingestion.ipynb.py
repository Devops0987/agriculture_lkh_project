# Databricks notebook source
from pyspark.sql.functions import  col,lit

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

chk_point = f"/Volumes/{catalog_name}/bronze/metadata/checkpoint/"
schema_path = f"/Volumes/{catalog_name}/bronze/metadata/"


# COMMAND ----------

df = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format","csv")
    .option("cloudFiles.schemaLocation",schema_path)
    .option("cloudFiles.schemaEvolutionMode","addNewColumns")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("header", "true")
    .load(source_data)
)



# COMMAND ----------

df = (
    df.withColumnRenamed("Farm_Area(acres)", "Farm_Area_acres")
      .withColumnRenamed("Fertilizer_Used(tons)", "Fertilizer_Used_tons")
      .withColumnRenamed("Pesticide_Used(kg)", "Pesticide_Used_kg")
      .withColumnRenamed("Yield(tons)", "Yield_tons")
      .withColumnRenamed("Water_Usage(cubic meters)", "Water_Usage_cubic_meters")
)

# COMMAND ----------

df = (
    df.withColumn("ingestion_date", lit(ingestion_date).cast("timestamp"))
    .withColumn("source_file", col("_metadata.file_name"))
)


# COMMAND ----------

query = (
df.writeStream
  .format("delta")
  .option("delta.columnMapping.mode", "name")
  .option("checkpointLocation", chk_point)
  .option("mergeSchema", "true")
  .trigger(availableNow=True)
  .toTable(f"{catalog_name}.bronze.farm_raw")
  )

query.awaitTermination()


# COMMAND ----------

bronze_df = spark.read.table(f"{catalog_name}.bronze.farm_raw").filter(
    f"ingestion_date = '{ingestion_date}'"
)
bronze_df.count()

# COMMAND ----------

