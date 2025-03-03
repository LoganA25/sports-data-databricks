# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col

# Define the expected schema
expected_schema = StructType([
    StructField("PlayerName", StringType(), True),
    StructField("PlayerId", IntegerType(), True),
    StructField("Pos", StringType(), True),
    StructField("Team", StringType(), True),
    StructField("PlayerOpponent", StringType(), True),
    StructField("PassingYDS", IntegerType(), True),
    StructField("PassingInt", IntegerType(), True),
    StructField("PassingTD", IntegerType(), True),
    StructField("RushingPassingYDS", IntegerType(), True),
    StructField("ReceivingRec", IntegerType(), True),
    StructField("ReceivingYDS", IntegerType(), True),
    StructField("ReceivingTD", IntegerType(), True),
    StructField("RetTD", IntegerType(), True),
    StructField("FumTD", IntegerType(), True),
    StructField("2PT", IntegerType(), True),
    StructField("Fum", IntegerType(), True),
    StructField("PlayerWeekProjectedPts", DoubleType(), True),
    StructField("Rank", IntegerType(), True),
    StructField("TotalPoints", DoubleType(), True),
    StructField("ProjectionDiff", DoubleType(), True),
    StructField("week", IntegerType(), True),
    StructField("year", IntegerType(), True),
    StructField("PatMade", IntegerType(), True),
    StructField("PatMissed", IntegerType(), True),
    StructField("FgMade_0-19", IntegerType(), True),
    StructField("FgMade_20-29", IntegerType(), True),
    StructField("FgMade_30-39", IntegerType(), True),
    StructField("FgMade_40-49", IntegerType(), True),
    StructField("FgMade_50", IntegerType(), True),
    StructField("FgMiss_0-19", IntegerType(), True),
    StructField("FgMiss_20-29", IntegerType(), True),
    StructField("FgMiss_30-39", IntegerType(), True),
    StructField("TacklesTot", IntegerType(), True),
    StructField("TacklesAst", IntegerType(), True),
    StructField("TacklesSck", IntegerType(), True),
    StructField("TacklesTfl", IntegerType(), True),
    StructField("TurnoverInt", IntegerType(), True),
    StructField("TurnoverFrcFum", IntegerType(), True),
    StructField("TurnoverFumRec", IntegerType(), True),
    StructField("ScoreIntTd", IntegerType(), True),
    StructField("ScoreFumTd", IntegerType(), True),
    StructField("ScoreBlkTd", IntegerType(), True),
    StructField("ScoreSaf", IntegerType(), True),
    StructField("ScoreDef2ptRet", IntegerType(), True),
    StructField("Blk", IntegerType(), True),
    StructField("PDef", IntegerType(), True),
    StructField("QBHit", IntegerType(), True),
    StructField("ReturnIntYds", IntegerType(), True),
    StructField("ReturnFumYds", IntegerType(), True),
    StructField("RushingYDS", IntegerType(), True),
    StructField("RushingTD", IntegerType(), True),
    StructField("ProjectedRank", IntegerType(), True)
])

# Read the CSV file
csv_df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(expected_schema) \
    .load("/Volumes/tabular/dataexpert/la_player_stats/")

# Cast columns to the expected data types
for field in expected_schema.fields:
    csv_df = csv_df.withColumn(field.name, col(field.name).cast(IntegerType()) if field.name == 'PlayerId' else col(field.name).cast(field.dataType))

# Reorder columns to match the expected schema
csv_df = csv_df.select([field.name for field in expected_schema.fields])

# Validate the schema
def validate_schema(df, expected_schema):
    actual_schema = df.schema
    if actual_schema != expected_schema:
        raise ValueError(f"Schema mismatch detected! Got {actual_schema}, expected {expected_schema}.")

validate_schema(csv_df, expected_schema)

# Clean and filter the DataFrame
validated_df = csv_df \
    .dropna(subset=["PlayerName", "year", "week"]) \
    .filter(col("week") > 0)

# Write the DataFrame to Delta tables with schema merging
validated_df.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .saveAsTable("tabular.dataexpert.la_nfl_historic_stats")

validated_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("tabular.dataexpert.la_nfl_weekly_stats")

# COMMAND ----------

