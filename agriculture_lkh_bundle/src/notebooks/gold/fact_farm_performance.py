# Databricks notebook source
from pyspark.sql.functions import col, lit


# COMMAND ----------

dbutils.widgets.text("catalog", "agriculture_lkh_dev", "Catalog")
dbutils.widgets.text("ingestion_date", "", "Ingestion date")

catalog_name = dbutils.widgets.get("catalog")
ingestion_date = dbutils.widgets.get("ingestion_date")


# COMMAND ----------

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.gold")
spark.sql(f"USE SCHEMA gold")

# COMMAND ----------

silver_df = (
    spark.read.table(f"{catalog_name}.silver.farm_info")
    .filter(
        (col("is_current") == True) &
        (col("is_quarantine") == False) 
    )
)

# COMMAND ----------

silver_df.show()

# COMMAND ----------

silver_df.count()

# COMMAND ----------

dim_crop_df = spark.read.table(f"{catalog_name}.gold.Dim_crop")
dim_irrigation_df = spark.read.table(f"{catalog_name}.gold.Dim_irrigation")
dim_season_df = spark.read.table(f"{catalog_name}.gold.Dim_season")
dim_soil_df = spark.read.table(f"{catalog_name}.gold.Dim_soil")

# COMMAND ----------

fact_df = (
    silver_df
    .join(dim_crop_df, silver_df.crop_type == dim_crop_df.crop_type, "left")
    .join(dim_irrigation_df, silver_df.irrigation_type == dim_irrigation_df.irrigation_type, "left")
    .join(dim_season_df, silver_df.season == dim_season_df.season, "left")
    .join(dim_soil_df, silver_df.soil_type == dim_soil_df.soil_type, "left")
    .select(
        "farm_id", 
        "crop_key", 
        "irrigation_key", 
        "season_key",
        "soil_key", 
        "farm_area_acres", 
        "fertilizer_used_tons", 
        "pesticide_used_kg", 
        "yield_tons",      
        "water_usage_cubic_meters" 
    ))
display(fact_df)

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.gold.Fact_farm_performance(
    farm_id STRING,
    crop_key BIGINT,
    irrigation_key BIGINT,
    season_key BIGINT,
    soil_key BIGINT,
    farm_area_acres DECIMAL(10,2),
    fertilizer_used_tons DECIMAL(10,2),
    pesticide_used_kg DECIMAL(10,2),
    yield_tons DECIMAL(10,2),
    water_usage_cubic_meters DECIMAL(10,2)

    

)
 
          
""")



# COMMAND ----------

fact_df.write.mode("overwrite").saveAsTable(f"{catalog_name}.gold.Fact_farm_performance")

# COMMAND ----------

fact_df.count()

# COMMAND ----------

