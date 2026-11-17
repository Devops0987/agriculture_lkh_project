# Databricks notebook source
dbutils.widgets.text("catalog", "agriculture_lkh_dev", "Catalog")
catalog_name = dbutils.widgets.get("catalog")

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.gold")
spark.sql(f"USE SCHEMA gold")

# COMMAND ----------

silver_df = (
    spark.read.table(f"{catalog_name}.silver.farm_info")
    .filter("is_current = True AND is_quarantine = False")
    .select("season")
    .distinct()
)

# COMMAND ----------

silver_df.count()

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.gold.Dim_season(
    season_key BIGINT GENERATED ALWAYS AS IDENTITY,
    season string

)
  
          
""")

# COMMAND ----------

silver_df.write.mode("append").saveAsTable(f"{catalog_name}.gold.Dim_season")

# COMMAND ----------

