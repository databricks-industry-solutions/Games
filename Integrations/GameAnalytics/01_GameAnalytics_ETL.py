# Databricks notebook source
import dlt
from pyspark.sql import functions as F 
# from pyspark.sql.window import Window
# from pyspark.sql.types import LongType, FloatType, StringType, TimestampType, BooleanType, StructType, StructField, DoubleType, DateType, IntegerType

# COMMAND ----------

@dlt.table(
  name="raw",
  # comment=user_comment,
  # table_properties={"quality": "bronze"},
  # schema=user_schema
  # table_properties={'delta.feature.variantType-dev':'supported'},
  )
# @dlt.expect("Correct schema", "_rescued_data IS NULL")
def raw():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "text")
      # .option("cloudFiles.schemaHints", "value VARIANT")
      # .schema("value variant")
      .load("/Volumes/users/randy_akerman/game_analytics/raw/")
      .withColumn("source_metadata", F.col("_metadata"))
      )

# COMMAND ----------


