# Databricks notebook source
# MAGIC %md
# MAGIC # EIA Electricity Demand - Bronze Layer Ingestion
# MAGIC
# MAGIC Ingests daily electricity demand data from EIA API for multiple regions

# COMMAND ----------

import requests
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Get your free API key at: https://www.eia.gov/opendata/register.php
EIA_API_KEY = dbutils.secrets.get(scope="eia", key="api_key")  # Or hardcode for testing

# Major US electricity regions (Balancing Authorities)
REGIONS = [
    "PJM",      # Mid-Atlantic (13 states)
    "CISO",     # California
    "MISO",     # Midwest
    "ERCO",     # Texas (ERCOT)
    "SWPP",     # Southwest Power Pool
    "NYIS",     # New York
    "ISNE",     # New England
    "BPAT",     # Bonneville Power (Pacific Northwest)
    "FPL",      # Florida Power & Light
    "TVA"       # Tennessee Valley Authority
]

# Date range
START_DATE = "2020-01-01"
END_DATE = datetime.now().strftime("%Y-%m-%d")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch Data from EIA API

# COMMAND ----------

def fetch_eia_electricity_demand(api_key, region, start_date, end_date):
    """
    Fetch daily electricity demand data from EIA API for a specific region
    """
    url = "https://api.eia.gov/v2/electricity/rto/daily-region-data/data/"
    
    params = {
        "api_key": api_key,
        "frequency": "daily",
        "data[0]": "value",
        "facets[respondent][]": region,
        "facets[type][]": "D",  # D = Demand
        "start": start_date,
        "end": end_date,
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
        "offset": 0,
        "length": 5000
    }
    
    all_data = []
    
    while True:
        response = requests.get(url, params=params)
        
        if response.status_code != 200:
            print(f"Error fetching data for {region}: {response.status_code}")
            print(response.text)
            break
            
        json_data = response.json()
        
        if 'response' not in json_data or 'data' not in json_data['response']:
            print(f"No data found for {region}")
            break
            
        data = json_data['response']['data']
        all_data.extend(data)
        
        # Check if there's more data (pagination)
        total = int(json_data['response']['total'])
        if len(all_data) >= total:
            break
            
        params['offset'] += 5000
        
    print(f"Fetched {len(all_data)} records for {region}")
    return all_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest All Regions

# COMMAND ----------

all_region_data = []

for region in REGIONS:
    print(f"Fetching data for {region}...")
    region_data = fetch_eia_electricity_demand(EIA_API_KEY, region, START_DATE, END_DATE)
    all_region_data.extend(region_data)
    print(f"Total records so far: {len(all_region_data)}\n")

print(f"Total records fetched: {len(all_region_data)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert to DataFrame and Write to Bronze

# COMMAND ----------

# Define schema
schema = StructType([
    StructField("period", StringType(), True),  # Keep as string for parsing
    StructField("respondent", StringType(), True),
    StructField("respondent-name", StringType(), True),
    StructField("type", StringType(), True),
    StructField("type-name", StringType(), True),
    StructField("value", StringType(), True),  # Keep as string, convert later
    StructField("value-units", StringType(), True)
])

# Create DataFrame
df = spark.createDataFrame(all_region_data, schema=schema)

# Add audit columns and convert types
df_bronze = (
    df
    # Convert period from string to date (format: YYYYMMDD or YYYYDDD)
    .withColumn("date", 
        F.when(F.length("period") == 8, F.to_date("period", "yyyyMMdd"))
         .when(F.length("period") == 10, F.to_date("period", "yyyy-MM-dd"))
         .otherwise(F.to_date("period"))
    )
    .withColumn("demand_mwh", F.col("value").cast("double"))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source", F.lit("eia_api"))
    .select(
        "date",
        F.col("respondent").alias("region_code"),
        F.col("respondent-name").alias("region_name"),
        F.col("type").alias("data_type"),
        F.col("type-name").alias("data_type_name"),
        "demand_mwh",
        F.col("value-units").alias("units"),
        "_ingested_at",
        "_source"
    )
)

# Show sample
print("Sample data:")
df_bronze.show(10)

print(f"\nTotal records: {df_bronze.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze Table

# COMMAND ----------

# Write to bronze layer
(
    df_bronze.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("noaa_data.bronze.eia_electricity_demand_raw")
)

print("✓ Data written to: noaa_data.bronze.eia_electricity_demand_raw")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify the data
# MAGIC SELECT 
# MAGIC      region_name,
# MAGIC      COUNT(*) as record_count,
# MAGIC      MIN(date) as earliest_date,
# MAGIC      MAX(date) as latest_date,
# MAGIC      ROUND(AVG(demand_mwh), 0) as avg_demand_mwh
# MAGIC FROM noaa_data.bronze.eia_electricity_demand_raw
# MAGIC GROUP BY region_name
# MAGIC ORDER BY region_name;
