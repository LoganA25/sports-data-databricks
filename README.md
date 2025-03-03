Capstone Project for DataExpert bootcamp

NFL Betting & Fantasy Data Analysis
Introduction
This project was originally intended to stream NFL data in real-time, but due to the end of the season, I pivoted to ingesting and analyzing historical data. The goal was to capture betting odds (player props), fantasy stats, and player performance metrics to identify trends and inefficiencies.

## Design
![image](https://github.com/user-attachments/assets/504b2139-6211-4349-b7e9-b6caba738a62)


## Workflows/Pipeline
I wasn't able to do a streaming portion due to the season being over, so the prediction tables are on standby. The historic data is ready for full use! Here's an example of the pipeline and how it runs!

![image](https://github.com/user-attachments/assets/35b97b35-dd18-4da9-87b4-e42e444a5335)
![image](https://github.com/user-attachments/assets/0a03380a-3100-4dff-8eef-2b3ded11b66e)

You can set the schedule on the pipeline or the workflow. I have it in the workflow so it kicks off when the file is dropped inside of the volume. 

![image](https://github.com/user-attachments/assets/0ac5b35e-5fa8-42ae-8e5b-09976d0f1f2f)

# NFL Betting & Fantasy Data Analysis

This project was originally intended to stream NFL data in real-time, but due to the end of the season, I pivoted to ingesting and analyzing historical data. The goal was to capture betting odds (player props), fantasy stats, and player performance metrics to identify trends and inefficiencies.

While working with the API, excessive testing led to the expiration of my API tokens for player props data. As a workaround, I retrieved a large JSON dataset and focused on parsing and analyzing it. This dataset can be leveraged for future implementations, including real-time streaming. As for the stats I found used historic data from csvs.

## Project Scope & Data Sources
This project aggregates data from multiple sources to ensure diversity in formats and structures:
Betting Odds (Player Props): Originally intended for real-time ingestion from APIs (e.g., FanDuel, DraftKings) but obtained from a JSON dump due to API limitations.
Fantasy Sports Projections: Historical and current player performance data, including expected fantasy points, injury status, and positional matchups.
Player Stats: Actual player performance data to compare against projections and betting odds.

## Tech Stack
- Python: Used for data ingestion, transformation, and analysis within Databricks.
- Databricks: Unified platform for processing and analyzing large datasets.
- Delta Live Tables (DLT): Automates ingestion, transformation, and pipeline execution.
- Delta Lake: ACID-compliant storage for structured and optimized querying.
- Databricks Workflows: Managed ETL jobs without external orchestration tools.
- Databricks Dashboarding: Built-in visualization tool for analyzing trends and insights.

I've never used a "big data" tool and having free access to a flagship like databricks I wanted to use it to its fullest extent. Boy did I underestimate it.
## First Look at Data Sources
### Player Stats
This one wasn't too bad and I wanted a free source for this one so I went with scraped data.

**Scraped NFL Player Stats**

https://github.com/hvpkod/NFL-Data/tree/main/NFL-data-Players

Results for one position(QB)

`PlayerName` – STRING NOT NULL  
* Name of the player.  

`PlayerId` – STRING NOT NULL  
* Unique identifier for the player.  

`Pos` – STRING NOT NULL  
* Position of the player (e.g., `"QB"` for Quarterback).  

`Team` – STRING NOT NULL  
* The team that the player currently plays for.  

`PlayerOpponent` – STRING NOT NULL  
* The opposing team for the given game.  

`PassingYDS` – INT NULL  
* Total passing yards recorded by the player in the game.  

`PassingTD` – INT NULL  
* Number of passing touchdowns thrown by the player.  

`PassingInt` – INT NULL  
* Number of interceptions thrown by the player.  

`RushingYDS` – INT NULL  
* Total rushing yards recorded by the player in the game.  

`RushingTD` – INT NULL  
* Number of rushing touchdowns scored by the player.  

`ReceivingRec` – INT NULL  
* Number of receptions made by the player (not common for QBs but included for completeness).  

`ReceivingYDS` – INT NULL  
* Total receiving yards recorded by the player (not common for QBs but included for completeness).  

`ReceivingTD` – INT NULL  
* Number of receiving touchdowns scored by the player (not common for QBs but included for completeness).  

`RetTD` – INT NULL  
* Number of return touchdowns (e.g., kick return, punt return) scored by the player.  

`FumTD` – INT NULL  
* Number of fumbles recovered and returned for a touchdown by the player.  

`2PT` – INT NULL  
* Number of successful two-point conversions made by the player.  

`Fum` – INT NULL  
* Number of times the player fumbled the ball.  

`PlayerWeekProjectedPts` – DOUBLE NULL  
* The projected fantasy points for the player in the given week.  

`ProjectedRank` – INT NULL  
* The projected ranking of the player compared to others in their position.  

`Rank` – INT NULL  
* The actual ranking of the player for the given week based on performance.  

`TotalPoints` – DOUBLE NULL  
* The total fantasy points earned by the player for the game.  

`ProjectionDiff` – DOUBLE NULL  
* The difference between the player's projected fantasy points and actual fantasy points earned.  


**the-odds-api**

endpoint: https://api.the-odds-api.com/v4/historical/sports/basketball_nba/events/da359da99aa27e97d38f2df709343998/odds?apiKey=YOUR_API_KEY&date=2023-11-29T22:45:00Z&regions=us&markets=player_points,h2h_q1

```{
    "timestamp": "2023-11-29T22:40:39Z",
    "previous_timestamp": "2023-11-29T22:35:39Z",
    "next_timestamp": "2023-11-29T22:45:40Z",
    "data": {
        "id": "da359da99aa27e97d38f2df709343998",
        "sport_key": "basketball_nba",
        "sport_title": "NBA",
        "commence_time": "2023-11-30T00:10:00Z",
        "home_team": "Detroit Pistons",
        "away_team": "Los Angeles Lakers",
        "bookmakers": [
            {
                "key": "draftkings",
                "title": "DraftKings",
                "last_update": "2023-11-29T22:40:09Z",
                "markets": [
                    {
                        "key": "h2h_q1",
                        "last_update": "2023-11-29T22:40:55Z",
                        "outcomes": [
                            {
                                "name": "Detroit Pistons",
                                "price": 2.5
                            },
                            {
                                "name": "Los Angeles Lakers",
                                "price": 1.56
                            }
                        ]
                    },
                    {
                        "key": "player_points",
                        "last_update": "2023-11-29T22:40:55Z",
                        "outcomes": [
                            {
                                "name": "Over",
                                "description": "Anthony Davis",
                                "price": 1.83,
                                "point": 23.5
                            },
                            {
                                "name": "Under",
                                "description": "Anthony Davis",
                                "price": 1.91,
                                "point": 23.5
                            },
                            ...
```

**Odds Data Dictionary**

`timestamp` – TIMESTAMP NOT NULL 
* Timestamp when this data was retrieved.  
`previous_timestamp` – TIMESTAMP NULL 
* Previous recorded timestamp in the feed.  
`next_timestamp` – TIMESTAMP NULL 
* Next recorded timestamp in the feed.  
`id` – STRING NOT NULL 
* Unique identifier for the game event.  
`sport_key` – STRING NOT NULL 
* Sport category (e.g., `"basketball_nba"`).  
`sport_title` – STRING NOT NULL 
* Official name of the sport (e.g., `"NBA"`).  
`commence_time` – TIMESTAMP NOT NULL 
* Scheduled start time of the game.  
`home_team` – STRING NOT NULL 
* Name of the home team.  
`away_team` – STRING NOT NULL 
* Name of the away team.  
`key` – STRING NOT NULL 
* Unique identifier for the bookmaker (e.g., `"draftkings"`).  
`title` – STRING NOT NULL 
* Full name of the bookmaker (e.g., `"DraftKings"`).  
`last_update` – TIMESTAMP NOT NULL 
* Last time odds were updated by this bookmaker.  
`key` – STRING NOT NULL 
* Market type (e.g., `"h2h_q1"`, `"player_points"`).  
`last_update` – TIMESTAMP NOT NULL 
* Last time this market’s odds were updated.  
`name` – STRING NOT NULL 
* Outcome name (e.g., `"Detroit Pistons"`, `"Over"`).  
`price` – DOUBLE NOT NULL 
* Betting odds for this outcome.  
`description` – STRING NULL 
* Additional details about the bet (e.g., `"Anthony Davis"`).  
`point` – DOUBLE NULL 
* Statistical threshold for the bet (e.g., `23.5` for player points).

## Data Quality
For schema validation I defined the schema and did a check against it. 

![image](https://github.com/user-attachments/assets/7b56f3a3-6dec-4780-9936-a677c14259a2)
![image](https://github.com/user-attachments/assets/aafe6b59-273a-4d73-9d11-f462462c427b)


## Workflow/pipeline

**Betting Data**

This is structured similarly to the player stats data,\
both scripts have sensors setup through the workflow to pickup new files\
that are added to the listed volume. This could easily go right into a streaming project once the season starts back up. 

```from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
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
```

## Dashboards
I decided to use databricks dashboards for the seamless integration. The visuals are a little limited, but for what I needed it was great!\
I have a fantasy and betting dashboard, this can be used for decisions to help with who to select for your roster or betting purposes.

Fantasy/Stats:
![image](https://github.com/user-attachments/assets/7b27b44e-24b0-4bdd-96c3-358dcc754c61)

Betting Odds(2024):
![image](https://github.com/user-attachments/assets/3e03f8dc-576d-4ac5-9b82-e8d002466ede)

## Thoughts
- I wish I would have chosen better data sources, but I guess that's what we're here for! Where would the world be without data engineers!
- I've learned that hands on with anything is the way I learn best.

## Conclusion 
Working on this project was rewarding and challenging. I never though I'd be staring at databricks for so long.\
I underestimated the complexity of the tool, nonetheless I was able to build an end to end project all with databricks!
No matter what tool you run across, fundamentals are where it's at. With that said tools are an important piece to success.\
I do plan on branching this out in the future I do however think I'll be moving it to another stack due to costs and wants.
I hope you enjoyed my project, feel free to reach out and collaborate! I look forward to hearing your feedback, feel free\
to reach out! 

Checkout my website for a deeper dive and other projects!
Website: https://loganallen.dev/
Email: loganallendev@gmail.com



