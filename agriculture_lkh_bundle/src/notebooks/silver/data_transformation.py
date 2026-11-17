# Databricks notebook source
from pyspark.sql.functions import col, lit
from pyspark.sql.functions import ucase, trim, initcap, regexp_replace, when
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


# COMMAND ----------

dbutils.widgets.text("catalog", "agriculture_lkh_dev", "Catalog")
dbutils.widgets.text("ingestion_date", "", "Ingestion date")


# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog")
ingestion_date = dbutils.widgets.get("ingestion_date")

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.silver")
spark.sql(f"USE SCHEMA silver")

# COMMAND ----------


df = spark.read.table(f"{catalog_name}.bronze.farm_raw").filter(
    col("ingestion_date") == lit(ingestion_date).cast("timestamp")
)

# COMMAND ----------

df.count()


# COMMAND ----------

# MAGIC %md
# MAGIC Rename Col

# COMMAND ----------

rename_col_df = (
    df.withColumnRenamed("Farm_ID", "farm_id")
    .withColumnRenamed("Crop_Type", "crop_type")
    .withColumnRenamed("Farm_Area_acres", "farm_area_acres")
    .withColumnRenamed("Irrigation_Type", "irrigation_type")
    .withColumnRenamed("Fertilizer_Used_tons", "fertilizer_used_tons")
    .withColumnRenamed("Pesticide_Used_kg", "pesticide_used_kg")
    .withColumnRenamed("Yield_tons", "yield_tons")
    .withColumnRenamed("Soil_Type", "soil_type")
    .withColumnRenamed("Season", "season")
    .withColumnRenamed("Water_Usage_cubic_meters", "water_usage_cubic_meters")
)

# COMMAND ----------

rename_col_df.schema.names

# COMMAND ----------

# MAGIC %md
# MAGIC Standardise Text

# COMMAND ----------

standardise_text_df = (
    rename_col_df.withColumn(
        "farm_id",
        when(col("farm_id").isNotNull(), ucase(trim("farm_id"))).otherwise(None),
    )
    .withColumn(
        "crop_type",
        when(
            col("crop_type").isNotNull(),
            initcap(trim(regexp_replace(col("crop_type"), r"[^A-Za-z ]", ""))),
        ).otherwise(None),
    )
    .withColumn(
        "irrigation_type",
        when(
            col("irrigation_type").isNotNull(),
            initcap(trim(regexp_replace(col("irrigation_type"), r"[^A-Za-z ]", ""))),
        ).otherwise(None),
    )
    .withColumn(
        "soil_type",
        when(
            col("soil_type").isNotNull(),
            initcap(trim(regexp_replace(col("soil_type"), r"[^A-Za-z ]", ""))),
        ).otherwise(None),
    )
    .withColumn(
        "season",
        when(
            col("season").isNotNull(),
            initcap(trim(regexp_replace(col("season"), r"[^A-Za-z ]", ""))),
        ).otherwise(None),
    )
)

# COMMAND ----------

standardise_text_df.show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC Data Quality Check

# COMMAND ----------

dq_checked_df = standardise_text_df.withColumn(
    "is_quarantine",
    when(
        (col("farm_id").isNotNull() & (col("farm_id") != ""))
        & (col("farm_area_acres") > 0)
        & (col("fertilizer_used_tons") >= 0)
        & (col("pesticide_used_kg") >= 0)
        & (col("yield_tons") > 0)
        & (col("water_usage_cubic_meters") > 0)
        & (col("_rescued_data").isNull()),
        lit(False),
    ).otherwise(lit(True)),
)

# COMMAND ----------

dq_checked_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC DeDuplication

# COMMAND ----------

w = Window.partitionBy("farm_id").orderBy(col("ingestion_date").desc())

deduped_df = (
    dq_checked_df.withColumn("_rn", row_number().over(w))
    .filter("_rn = 1")
    .drop("_rn")
)
deduped_df.count()

# COMMAND ----------

spark.sql(f"""
          
    CREATE TABLE IF NOT EXISTS {catalog_name}.silver.farm_info (
    farm_id                   STRING    NOT NULL,                   
    -- business key
    crop_type                 STRING,
    farm_area_acres           DECIMAL(10,2),
    irrigation_type           STRING,
    fertilizer_used_tons      DECIMAL(10,2),
    pesticide_used_kg         DECIMAL(10,2),
    yield_tons                DECIMAL(10,2),
    soil_type                 STRING,
    season                    STRING,
    water_usage_cubic_meters  DECIMAL(10,2),
    is_quarantine             BOOLEAN,
    -- SCD Type 2 columns
    is_current                BOOLEAN   NOT NULL,
    effective_from            TIMESTAMP NOT NULL,
    effective_to              TIMESTAMP,   -- NULL while the row is current

    -- lineage from bronze
    ingestion_date            TIMESTAMP,
    source_file               STRING
)

""")

# COMMAND ----------

silver_df = spark.read.table(f"{catalog_name}.silver.farm_info")
silver_df.count()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

(
    deduped_df.alias("s")
    .mergeInto(
        f"{catalog_name}.silver.farm_info",
        (
            col("s.farm_id")
            == col(f"{catalog_name}.silver.farm_info.farm_id")
        )
        & (col(f"{catalog_name}.silver.farm_info.is_current") == True),
    )
    .whenMatched()
    .update({"is_current": lit(False), "effective_to": current_timestamp()})
    .whenNotMatched()
    .insert(
        {
            "farm_id": col("s.farm_id"),
            "crop_type": col("s.crop_type"),
            "farm_area_acres": col("s.farm_area_acres"),
            "irrigation_type": col("s.irrigation_type"),
            "fertilizer_used_tons": col("s.fertilizer_used_tons"),
            "pesticide_used_kg": col("s.pesticide_used_kg"),
            "yield_tons": col("s.yield_tons"),
            "soil_type": col("s.soil_type"),
            "season": col("s.season"),
            "water_usage_cubic_meters": col("s.water_usage_cubic_meters"),
            "is_quarantine": col("s.is_quarantine"),
            "is_current": lit(True),
            "effective_from": current_timestamp(),
            "effective_to": lit(None).cast("timestamp"),
            "ingestion_date": col("s.ingestion_date"),
            "source_file": col("s.source_file"),
        }
    )
    .merge()
)

# COMMAND ----------

silver_df.count()