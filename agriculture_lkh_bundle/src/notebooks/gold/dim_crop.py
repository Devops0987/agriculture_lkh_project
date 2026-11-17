# Databricks notebook source
dbutils.widgets.text("catalog", "agriculture_lkh_dev", "Catalog")
catalog_name = dbutils.widgets.get("catalog")

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.gold")
spark.sql(f"USE SCHEMA gold")

# COMMAND ----------

silver_crop_df = (
    spark.read.table(f"{catalog_name}.silver.farm_info")
    .filter("is_current = True AND is_quarantine = False")
    .select("crop_type")
    .distinct()
)

# COMMAND ----------

silver_crop_df.count()

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog_name}.gold.Dim_crop (
    crop_key BIGINT GENERATED ALWAYS AS IDENTITY,
    crop_type STRING
)
  
          
""")

# COMMAND ----------

silver_crop_df.write.mode("append").saveAsTable(f"{catalog_name}.gold.Dim_crop")

# COMMAND ----------

