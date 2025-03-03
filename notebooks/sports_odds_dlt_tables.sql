-- Databricks notebook source
CREATE OR REFRESH LIVE TABLE tabular.dataexpert.standardized_odds
TBLPROPERTIES ("quality" = "silver")
AS
SELECT 
    Player,
    Season_Year AS year,
    Week AS week,
    Bet_Type,
    Bookmaker as betting_platform,
    Point AS Line,
    Odds,
    Home_Team,
    Away_Team
FROM tabular.dataexpert.la_nfl_historic_odds;

CREATE OR REFRESH LIVE TABLE tabular.dataexpert.standardized_actuals
TBLPROPERTIES ("quality" = "silver")
AS
SELECT 
    PlayerName,
    year,
    week,
    Team,
    PassingTD,
    RushingYDS,
    ReceivingYDS,
    ReceivingRec
FROM tabular.dataexpert.la_nfl_historic_actual_stats;

CREATE OR REFRESH MATERIALIZED VIEW tabular.dataexpert.la_historic_odds
AS
SELECT 
    o.year,
    o.week,
    o.Player AS player,
    o.Bet_Type,
    o.Line AS line,
    o.Odds AS odds,
    o.betting_platform,
    a.team,
    CASE 
        WHEN o.Bet_Type = 'player_pass_yds' THEN a.PassingTD
        WHEN o.Bet_Type = 'player_rush_yds' THEN a.RushingYDS
        WHEN o.Bet_Type = 'player_reception_yds' THEN a.ReceivingYDS
        WHEN o.Bet_Type = 'player_reception' THEN a.ReceivingRec
        WHEN o.Bet_Type = 'player_pass_tds' THEN a.PassingTD
        ELSE NULL
    END AS actual_stat,
    ROUND(
        CASE 
            WHEN odds >= 2.00 THEN (odds - 1) * 100 
            WHEN odds < 2.00 THEN -100 / (odds - 1)  
        END, 0
    ) AS moneyline_odds,
    ROUND((1 / odds) * 100, 2) AS implied_probability, 
    CASE 
        WHEN (
            (o.Bet_Type = 'player_pass_yds' AND a.PassingTD > o.Line) OR
            (o.Bet_Type = 'player_rush_yds' AND a.RushingYDS > o.Line) OR
            (o.Bet_Type = 'player_reception_yds' AND a.ReceivingYDS > o.Line) OR
            (o.Bet_Type = 'player_reception' AND a.ReceivingRec > o.Line) OR
            (o.Bet_Type = 'player_pass_tds' AND a.PassingTD > o.Line)
        ) THEN 'Over'
        ELSE 'Under'
    END AS bet_result
FROM LIVE.standardized_odds o                          
LEFT JOIN LIVE.standardized_actuals a
ON o.Player = a.PlayerName 
AND o.year = a.year 
AND o.week = a.week
WHERE o.Player IS NOT NULL
AND o.year IS NOT NULL
AND o.week IS NOT NULL
AND o.week > 0;
