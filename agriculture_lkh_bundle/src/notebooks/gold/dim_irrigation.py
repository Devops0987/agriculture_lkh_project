# Databricks notebook source
dbutils.widgets.text("catalog", "agriculture_lkh_dev", "Catalog")
catalog_name = dbutils.widgets.get("catalog")

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.gold")
spark.sql(f"USE SCHEMA gold")

# COMMAND ----------

silver_irrigation_df = (
    spark.read.table(f"{catalog_name}.silver.farm_info")
    .filter("is_current = True AND is_quarantine = False")
    .select("irrigation_type")
    .distinct()
)

# COMMAND ----------

silver_irrigation_df.count()

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.gold.Dim_irrigation (
    irrigation_key BIGINT GENERATED ALWAYS AS IDENTITY,
    irrigation_type string

)
  
          
""")

# COMMAND ----------

silver_irrigation_df.write.mode("append").saveAsTable(f"{catalog_name}.gold.Dim_irrigation")

# COMMAND ----------

