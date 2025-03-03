# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import col

expected_schema = StructType([
    StructField("Player", StringType(), True),
    StructField("Player_ID", StringType(), True),
    StructField("Game_Date", DateType(), True),
    StructField("Season_Year", IntegerType(), True),
    StructField("Week", IntegerType(), True),
    StructField("Home_Team", StringType(), True),
    StructField("Away_Team", StringType(), True),
    StructField("Bookmaker", StringType(), True),
    StructField("Bet_Type", StringType(), True),
    StructField("Bet", StringType(), True),
    StructField("Point", DoubleType(), True),
    StructField("Odds", DoubleType(), True)
])

json_df = spark.read.format("json") \
    .option("multiLine", "true") \
    .load("/Volumes/tabular/dataexpert/la_player_odds/")

cleaned_stats_df = json_df.withColumn("Game_Date", col("Game_Date").cast("date")) \
    .withColumn("Season_Year", col("Season_Year").cast("int")) \
    .withColumn("Week", col("Week").cast("int")) \
    .withColumn("Home_Team", col("Home_Team").cast("string")) \
    .withColumn("Away_Team", col("Away_Team").cast("string")) \
    .withColumn("Bookmaker", col("Bookmaker").cast("string")) \
    .withColumn("Bet_Type", col("Bet_Type").cast("string")) \
    .withColumn("Player", col("Player").cast("string")) \
    .withColumn("Player_ID", col("Player_ID").cast("string")) \
    .withColumn("Bet", col("Bet").cast("string")) \
    .withColumn("Point", col("Point").cast("double")) \
    .withColumn("Odds", col("Odds").cast("double"))

# Reorder columns to match the expected schema
cleaned_stats_df = cleaned_stats_df.select(
    "Player", "Player_ID", "Game_Date", "Season_Year", "Week", "Home_Team", 
    "Away_Team", "Bookmaker", "Bet_Type", "Bet", "Point", "Odds"
)

def validate_schema(df, expected_schema):
    actual_schema = df.schema
    if actual_schema != expected_schema:
        raise ValueError(f"Schema mismatch detected! Got {actual_schema}, expected {expected_schema}.")

validate_schema(cleaned_stats_df, expected_schema)

cleaned_stats_df.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .saveAsTable("tabular.dataexpert.la_betting_odds")

cleaned_stats_df.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .saveAsTable("tabular.dataexpert.la_nfl_weekly_odds")