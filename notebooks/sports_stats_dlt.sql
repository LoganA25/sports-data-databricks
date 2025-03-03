-- Databricks notebook source
CREATE OR REFRESH LIVE TABLE tabular.dataexpert.standardized_stats
TBLPROPERTIES ("quality" = "gold")
AS
SELECT 
    *
FROM tabular.dataexpert.la_nfl_historic_stats;