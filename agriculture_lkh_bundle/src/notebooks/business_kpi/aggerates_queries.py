# Databricks notebook source
dbutils.widgets.text("catalog", "agriculture_lkh_dev", "Catalog")
catalog_name = dbutils.widgets.get("catalog")


# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.gold.total_yield_by_crop AS
SELECT
    c.crop_type,
    ROUND(SUM(p.yield_tons), 2) AS total_yield_tons
FROM {catalog_name}.gold.Fact_farm_performance p
JOIN {catalog_name}.gold.Dim_crop c
    ON p.crop_key = c.crop_key
GROUP BY c.crop_type
ORDER BY total_yield_tons DESC;""")

spark.sql(f"""
          
          select * from {catalog_name}.gold.total_yield_by_crop 
          """).show()

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.gold.total_yield_by_crop_season AS
SELECT
    s.season,
    c.crop_type,
    ROUND(SUM(p.yield_tons), 2) AS total_yield_tons
FROM {catalog_name}.gold.Fact_farm_performance  p
JOIN {catalog_name}.gold.Dim_crop c
    ON p.crop_key = c.crop_key
JOIN {catalog_name}.gold.Dim_season s
    ON p.season_key = s.season_key
GROUP BY
    s.season,
    c.crop_type
ORDER BY
    s.season,
    total_yield_tons DESC;
          """)

spark.sql(f"""
          
          select * from {catalog_name}.gold.total_yield_by_crop_season 
          """).show()


# COMMAND ----------

# water usages by irrigation type
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.gold.total_water_usage_by_irrigation AS
SELECT
    ROUND(SUM(p.water_usage_cubic_meters), 2) AS total_water_usage,
    -- ROUND(AVG(p.water_usage_cubic_meters), 2) AS avg_water_usage,
    i.irrigation_type 
FROM {catalog_name}.gold.Fact_farm_performance  p
JOIN {catalog_name}.gold.Dim_irrigation i
    ON p.irrigation_key = i.irrigation_key
GROUP BY i.irrigation_type
ORDER BY total_water_usage DESC;      
""")
spark.sql(f"""
          
          select * from {catalog_name}.gold.total_water_usage_by_irrigation 
          """).show()


# COMMAND ----------

# water usage by crop

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.gold.total_water_usage_by_crop as
SELECT
    c.crop_type,
    ROUND(SUM(p.water_usage_cubic_meters), 2) AS total_water_usage
    -- ROUND(SUM(p.yield_tons), 2) AS total_yield,
    -- ROUND(
    --     SUM(p.water_usage_cubic_meters) /
    --     NULLIF(SUM(p.yield_tons ), 0),
    --     4
    -- ) AS total_water_per_ton_yield
FROM {catalog_name}.gold.Fact_farm_performance  p
JOIN {catalog_name}.gold.Dim_crop c
    ON p.crop_key = c.crop_key
GROUP BY c.crop_type
ORDER BY total_water_usage DESC;          

""")
spark.sql(f"""
          
          select * from {catalog_name}.gold.total_water_usage_by_crop 
""").show()

# COMMAND ----------

#pesticides by crop
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.gold.pesticides_by_crop as
SELECT
    c.crop_type,
    ROUND(SUM(p.pesticide_used_kg), 2) AS total_pesticide_used_kg
FROM {catalog_name}.gold.Fact_farm_performance  p
JOIN {catalog_name}.gold.Dim_crop c
    ON p.crop_key = c.crop_key
GROUP BY c.crop_type
ORDER BY total_pesticide_used_kg DESC;
""")

spark.sql(f"""
          
          select * from {catalog_name}.gold.pesticides_by_crop 
""").show()

# COMMAND ----------

#fertilizer by crop
spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.gold.fertilizer_by_crop as
SELECT
    c.crop_type,
    ROUND(SUM(p.pesticide_used_kg), 2) AS total_fertilizer_used_tons
FROM {catalog_name}.gold.Fact_farm_performance  p
JOIN {catalog_name}.gold.Dim_crop c
    ON p.crop_key = c.crop_key
GROUP BY c.crop_type
ORDER BY total_fertilizer_used_tons DESC;
""")

spark.sql(f"""
          
          select * from {catalog_name}.gold.fertilizer_by_crop 
""").show()