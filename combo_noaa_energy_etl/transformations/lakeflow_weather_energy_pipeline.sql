-- Lakeflow Declarative Pipeline: NOAA Weather + EIA Energy Data
-- Combined medallion architecture for weather and electricity demand analysis
-- 
-- Pipeline Configuration:
-- - Target catalog: noaa_data
-- - Creates tables in bronze, silver, and gold schemas

-- ============================================================================
-- WEATHER DATA PIPELINE (BRONZE → SILVER → GOLD)
-- ============================================================================

-- BRONZE: Raw Weather Data
CREATE OR REFRESH LIVE TABLE noaa_data.bronze.bronze_noaa_ghcn_daily_raw
COMMENT "Raw NOAA GHCN daily weather data from Delta Share"
TBLPROPERTIES ("quality" = "bronze")
AS SELECT 
    *,
    CURRENT_TIMESTAMP() as _ingested_at,
    'delta_share_rearc' as _source
FROM rearc_daily_weather_observations_noaa.esg_noaa_ghcn.noaa_ghcn_daily;

-- SILVER: Clean Weather Observations
CREATE OR REFRESH LIVE TABLE noaa_data.silver.silver_daily_observations (
    CONSTRAINT valid_date EXPECT (date >= '2020-01-01' AND date <= CURRENT_DATE()),
    CONSTRAINT valid_temp_max_range EXPECT (temp_max_c IS NULL OR (temp_max_c >= -80 AND temp_max_c <= 60)) ON VIOLATION DROP ROW,
    CONSTRAINT valid_temp_min_range EXPECT (temp_min_c IS NULL OR (temp_min_c >= -90 AND temp_min_c <= 50)) ON VIOLATION DROP ROW,
    CONSTRAINT temp_max_gte_min EXPECT (temp_max_c IS NULL OR temp_min_c IS NULL OR temp_max_c >= temp_min_c) ON VIOLATION DROP ROW
)
COMMENT "Cleaned daily weather observations with unit conversions"
TBLPROPERTIES ("quality" = "silver")
AS SELECT 
    station,
    date,
    name as station_name,
    latitude,
    longitude,
    elevation,
    CASE WHEN temp_max IS NOT NULL THEN temp_max / 10.0 ELSE NULL END as temp_max_c,
    CASE WHEN temp_min IS NOT NULL THEN temp_min / 10.0 ELSE NULL END as temp_min_c,
    CASE WHEN precipitation IS NOT NULL THEN precipitation / 10.0 ELSE NULL END as precipitation_mm,
    CASE WHEN snowfall IS NOT NULL THEN snowfall / 10.0 ELSE NULL END as snowfall_cm,
    CASE WHEN snow_depth IS NOT NULL THEN snow_depth ELSE NULL END as snow_depth_mm,
    temp_max_attrs,
    temp_min_attrs,
    precipitation_attrs,
    snowfall_attrs,
    snow_depth_attrs,
    (CASE WHEN temp_max IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN temp_min IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN precipitation IS NOT NULL THEN 1 ELSE 0 END) as data_quality_score,
    group_ as station_group,
    _ingested_at,
    CURRENT_TIMESTAMP() as _processed_at
FROM noaa_data.bronze.bronze_noaa_ghcn_daily_raw;

-- ============================================================================
-- ENERGY DATA PIPELINE (BRONZE → SILVER → GOLD)
-- ============================================================================

-- Note: Bronze EIA data is loaded via the eia_bronze_ingestion.py notebook
-- This pipeline assumes that table exists

-- SILVER: Clean Energy Demand
CREATE OR REFRESH LIVE TABLE noaa_data.silver.silver_electricity_demand (
    CONSTRAINT valid_demand EXPECT (demand_mwh > 0 AND demand_mwh < 1000000) ON VIOLATION DROP ROW,
    CONSTRAINT valid_date EXPECT (date >= '2000-01-01' AND date <= CURRENT_DATE())
)
COMMENT "Cleaned daily electricity demand by region"
TBLPROPERTIES ("quality" = "silver")
AS SELECT 
    date,
    region_code,
    region_name,
    demand_mwh,
    units,
    _ingested_at,
    CURRENT_TIMESTAMP() as _processed_at
FROM noaa_data.bronze.eia_electricity_demand_raw;

-- ============================================================================
-- GOLD LAYER: WEATHER-ONLY TABLES
-- ============================================================================

-- Monthly Weather Summary
CREATE OR REFRESH LIVE TABLE noaa_data.gold.gold_monthly_weather_summary
COMMENT "Monthly weather summary aggregated by station"
TBLPROPERTIES ("quality" = "gold")
AS SELECT 
    station,
    MAX(station_name) as station_name,
    MAX(latitude) as latitude,
    MAX(longitude) as longitude,
    YEAR(date) as year,
    MONTH(date) as month,
    DATE_TRUNC('month', date) as month_date,
    ROUND(AVG(temp_max_c), 1) as avg_temp_max_c,
    ROUND(AVG(temp_min_c), 1) as avg_temp_min_c,
    ROUND(AVG((temp_max_c + temp_min_c) / 2), 1) as avg_temp_c,
    ROUND(MAX(temp_max_c), 1) as max_temp_c,
    ROUND(MIN(temp_min_c), 1) as min_temp_c,
    ROUND(SUM(precipitation_mm), 1) as total_precipitation_mm,
    COUNT(CASE WHEN precipitation_mm > 0 THEN 1 END) as days_with_precipitation,
    ROUND(MAX(precipitation_mm), 1) as max_daily_precipitation_mm,
    ROUND(SUM(snowfall_cm), 1) as total_snowfall_cm,
    COUNT(CASE WHEN snowfall_cm > 0 THEN 1 END) as days_with_snowfall,
    COUNT(*) as days_reported,
    COUNT(CASE WHEN temp_max_c IS NOT NULL THEN 1 END) as days_with_temp_max,
    COUNT(CASE WHEN temp_min_c IS NOT NULL THEN 1 END) as days_with_temp_min,
    COUNT(CASE WHEN precipitation_mm IS NOT NULL THEN 1 END) as days_with_precip,
    CURRENT_TIMESTAMP() as _created_at
FROM noaa_data.silver.silver_daily_observations
GROUP BY station, YEAR(date), MONTH(date), DATE_TRUNC('month', date);

-- Recent Weather Dashboard
CREATE OR REFRESH LIVE TABLE noaa_data.gold.gold_recent_weather_dashboard
COMMENT "Recent weather data (last 30 days) for operational dashboards"
TBLPROPERTIES ("quality" = "gold")
AS SELECT 
    station,
    station_name,
    latitude,
    longitude,
    date,
    temp_max_c,
    temp_min_c,
    ROUND((temp_max_c + temp_min_c) / 2, 1) as temp_avg_c,
    precipitation_mm,
    snowfall_cm,
    snow_depth_mm,
    ROUND(temp_max_c * 9/5 + 32, 1) as temp_max_f,
    ROUND(temp_min_c * 9/5 + 32, 1) as temp_min_f,
    ROUND((temp_max_c + temp_min_c) / 2 * 9/5 + 32, 1) as temp_avg_f,
    ROUND(precipitation_mm / 25.4, 2) as precipitation_inches,
    data_quality_score,
    CURRENT_TIMESTAMP() as _created_at
FROM noaa_data.silver.silver_daily_observations
WHERE date >= CURRENT_DATE() - INTERVAL '30' DAY
  AND date <= CURRENT_DATE();

-- ============================================================================
-- GOLD LAYER: ENERGY-ONLY TABLES
-- ============================================================================

-- Monthly Energy Demand Summary
CREATE OR REFRESH LIVE TABLE noaa_data.gold.gold_monthly_energy_demand
COMMENT "Monthly electricity demand aggregated by region"
TBLPROPERTIES ("quality" = "gold")
AS SELECT 
    region_code,
    region_name,
    YEAR(date) as year,
    MONTH(date) as month,
    DATE_TRUNC('month', date) as month_date,
    ROUND(AVG(demand_mwh), 0) as avg_daily_demand_mwh,
    ROUND(MIN(demand_mwh), 0) as min_daily_demand_mwh,
    ROUND(MAX(demand_mwh), 0) as max_daily_demand_mwh,
    ROUND(SUM(demand_mwh), 0) as total_monthly_demand_mwh,
    COUNT(*) as days_reported,
    CURRENT_TIMESTAMP() as _created_at
FROM noaa_data.silver.silver_electricity_demand
GROUP BY region_code, region_name, YEAR(date), MONTH(date), DATE_TRUNC('month', date);

-- ============================================================================
-- GOLD LAYER: COMBINED WEATHER + ENERGY ANALYSIS
-- ============================================================================

-- Regional Weather Aggregations (for joining with energy)
CREATE OR REFRESH LIVE TABLE noaa_data.gold.gold_regional_daily_weather
COMMENT "Daily weather aggregated by US region for energy correlation analysis"
TBLPROPERTIES ("quality" = "gold")
AS
WITH regional_mapping AS (
    SELECT 
        station,
        station_name,
        latitude,
        longitude,
        date,
        temp_max_c,
        temp_min_c,
        (temp_max_c + temp_min_c) / 2 as temp_avg_c,
        precipitation_mm,
        -- Map weather stations to energy regions based on lat/lon
        CASE
            -- PJM: Mid-Atlantic (roughly 37-42°N, -83 to -74°W)
            WHEN latitude BETWEEN 37 AND 42 AND longitude BETWEEN -83 AND -74 THEN 'PJM'
            -- CISO: California (32-42°N, -125 to -114°W)
            WHEN latitude BETWEEN 32 AND 42 AND longitude BETWEEN -125 AND -114 THEN 'CISO'
            -- ERCO: Texas (26-37°N, -107 to -93°W)
            WHEN latitude BETWEEN 26 AND 37 AND longitude BETWEEN -107 AND -93 THEN 'ERCO'
            -- MISO: Midwest (37-49°N, -105 to -82°W) excluding PJM area
            WHEN latitude BETWEEN 37 AND 49 AND longitude BETWEEN -105 AND -82 THEN 'MISO'
            -- NYIS: New York (40.5-45°N, -80 to -71°W)
            WHEN latitude BETWEEN 40.5 AND 45 AND longitude BETWEEN -80 AND -71 THEN 'NYIS'
            -- ISNE: New England (41-48°N, -74 to -66°W)
            WHEN latitude BETWEEN 41 AND 48 AND longitude BETWEEN -74 AND -66 THEN 'ISNE'
            -- SWPP: Southwest (31-41°N, -109 to -94°W)
            WHEN latitude BETWEEN 31 AND 41 AND longitude BETWEEN -109 AND -94 THEN 'SWPP'
            -- BPAT: Pacific NW (42-49°N, -125 to -110°W)
            WHEN latitude BETWEEN 42 AND 49 AND longitude BETWEEN -125 AND -110 THEN 'BPAT'
            -- FPL: Florida (24-31°N, -88 to -80°W)
            WHEN latitude BETWEEN 24 AND 31 AND longitude BETWEEN -88 AND -80 THEN 'FPL'
            -- TVA: Tennessee Valley (33-37°N, -91 to -81°W)
            WHEN latitude BETWEEN 33 AND 37 AND longitude BETWEEN -91 AND -81 THEN 'TVA'
            ELSE 'OTHER'
        END as energy_region
    FROM noaa_data.silver.silver_daily_observations
    WHERE date >= '2020-01-01'  -- Match EIA data range
      AND (temp_max_c + temp_min_c) / 2 IS NOT NULL
)
SELECT 
    date,
    energy_region,
    COUNT(DISTINCT station) as station_count,
    ROUND(AVG(temp_avg_c), 1) as avg_temp_c,
    ROUND(MIN(temp_min_c), 1) as min_temp_c,
    ROUND(MAX(temp_max_c), 1) as max_temp_c,
    ROUND(AVG(precipitation_mm), 2) as avg_precipitation_mm,
    -- Calculate degree days (base 18°C / 65°F)
    ROUND(AVG(CASE WHEN temp_avg_c < 18 THEN 18 - temp_avg_c ELSE 0 END), 1) as heating_degree_days,
    ROUND(AVG(CASE WHEN temp_avg_c > 18 THEN temp_avg_c - 18 ELSE 0 END), 1) as cooling_degree_days,
    CURRENT_TIMESTAMP() as _created_at
FROM regional_mapping
WHERE energy_region != 'OTHER'
GROUP BY date, energy_region;

-- Weather + Energy Correlation Analysis
CREATE OR REFRESH LIVE TABLE noaa_data.gold.gold_weather_energy_correlation
COMMENT "Daily weather and energy demand correlation by region"
TBLPROPERTIES ("quality" = "gold")
AS SELECT 
    w.date,
    w.energy_region as region,
    w.station_count,
    w.avg_temp_c,
    w.min_temp_c,
    w.max_temp_c,
    w.avg_precipitation_mm,
    w.heating_degree_days,
    w.cooling_degree_days,
    e.demand_mwh,
    -- Temperature in Fahrenheit for context
    ROUND(w.avg_temp_c * 9/5 + 32, 1) as avg_temp_f,
    -- Demand per degree day (efficiency metric)
    CASE 
        WHEN w.heating_degree_days > 0 THEN ROUND(e.demand_mwh / w.heating_degree_days, 0)
        ELSE NULL 
    END as demand_per_hdd,
    CASE 
        WHEN w.cooling_degree_days > 0 THEN ROUND(e.demand_mwh / w.cooling_degree_days, 0)
        ELSE NULL 
    END as demand_per_cdd,
    CURRENT_TIMESTAMP() as _created_at
FROM noaa_data.gold.gold_regional_daily_weather w
INNER JOIN noaa_data.silver.silver_electricity_demand e
    ON w.date = e.date
    AND w.energy_region = e.region_code
WHERE w.station_count >= 5;  -- Ensure sufficient weather station coverage