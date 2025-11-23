-- Lakeflow Declarative Pipeline: NOAA Weather Medallion Architecture
-- Save this as a SQL file and create a Lakeflow pipeline pointing to it
--  Change? 
-- Pipeline Configuration:
-- - Target catalog: noaa_data
-- - This pipeline will create tables in:
--   - noaa_data.bronze.* (bronze layer)
--   - noaa_data.silver.* (silver layer)  
--   - noaa_data.gold.* (gold layer)

-- ============================================================================
-- BRONZE LAYER: Raw Ingestion
-- ============================================================================

CREATE OR REFRESH LIVE TABLE noaa_data.bronze.bronze_noaa_ghcn_daily_raw
COMMENT "Raw NOAA GHCN daily weather data from Delta Share"
TBLPROPERTIES ("quality" = "bronze")
AS SELECT 
    *,
    CURRENT_TIMESTAMP() as _ingested_at,
    'delta_share_rearc' as _source
FROM rearc_daily_weather_observations_noaa.esg_noaa_ghcn.noaa_ghcn_daily;

-- ============================================================================
-- SILVER LAYER: Cleansed and Standardized
-- ============================================================================

CREATE OR REFRESH LIVE TABLE noaa_data.silver.silver_daily_observations (
    CONSTRAINT valid_date EXPECT (date >= '1900-01-01' AND date <= CURRENT_DATE()),
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
    
    -- Convert temps from tenths of degrees C to degrees C
    CASE WHEN temp_max IS NOT NULL THEN temp_max / 10.0 ELSE NULL END as temp_max_c,
    CASE WHEN temp_min IS NOT NULL THEN temp_min / 10.0 ELSE NULL END as temp_min_c,
    
    -- Convert precipitation from tenths of mm to mm
    CASE WHEN precipitation IS NOT NULL THEN precipitation / 10.0 ELSE NULL END as precipitation_mm,
    
    -- Convert snowfall from mm to cm
    CASE WHEN snowfall IS NOT NULL THEN snowfall / 10.0 ELSE NULL END as snowfall_cm,
    CASE WHEN snow_depth IS NOT NULL THEN snow_depth ELSE NULL END as snow_depth_mm,
    
    -- Quality attributes
    temp_max_attrs,
    temp_min_attrs,
    precipitation_attrs,
    snowfall_attrs,
    snow_depth_attrs,
    
    -- Data quality score
    (CASE WHEN temp_max IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN temp_min IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN precipitation IS NOT NULL THEN 1 ELSE 0 END) as data_quality_score,
    
    -- Metadata
    group_ as station_group,
    _ingested_at,
    CURRENT_TIMESTAMP() as _processed_at
    
FROM noaa_data.bronze.bronze_noaa_ghcn_daily_raw;

-- ============================================================================
-- GOLD LAYER: Monthly Aggregations
-- ============================================================================

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
    
    -- Temperature stats
    ROUND(AVG(temp_max_c), 1) as avg_temp_max_c,
    ROUND(AVG(temp_min_c), 1) as avg_temp_min_c,
    ROUND(AVG((temp_max_c + temp_min_c) / 2), 1) as avg_temp_c,
    ROUND(MAX(temp_max_c), 1) as max_temp_c,
    ROUND(MIN(temp_min_c), 1) as min_temp_c,
    
    -- Precipitation stats
    ROUND(SUM(precipitation_mm), 1) as total_precipitation_mm,
    COUNT(CASE WHEN precipitation_mm > 0 THEN 1 END) as days_with_precipitation,
    ROUND(MAX(precipitation_mm), 1) as max_daily_precipitation_mm,
    
    -- Snow stats
    ROUND(SUM(snowfall_cm), 1) as total_snowfall_cm,
    COUNT(CASE WHEN snowfall_cm > 0 THEN 1 END) as days_with_snowfall,
    
    -- Data completeness
    COUNT(*) as days_reported,
    COUNT(CASE WHEN temp_max_c IS NOT NULL THEN 1 END) as days_with_temp_max,
    COUNT(CASE WHEN temp_min_c IS NOT NULL THEN 1 END) as days_with_temp_min,
    COUNT(CASE WHEN precipitation_mm IS NOT NULL THEN 1 END) as days_with_precip,
    
    CURRENT_TIMESTAMP() as _created_at

FROM noaa_data.silver.silver_daily_observations
GROUP BY station, YEAR(date), MONTH(date), DATE_TRUNC('month', date);

-- ============================================================================
-- GOLD LAYER: Recent Weather Dashboard (Last 30 Days)
-- ============================================================================

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
    
    -- Convert to Fahrenheit for US audience
    ROUND(temp_max_c * 9/5 + 32, 1) as temp_max_f,
    ROUND(temp_min_c * 9/5 + 32, 1) as temp_min_f,
    ROUND((temp_max_c + temp_min_c) / 2 * 9/5 + 32, 1) as temp_avg_f,
    ROUND(precipitation_mm / 25.4, 2) as precipitation_inches,
    
    data_quality_score,
    CURRENT_TIMESTAMP() as _created_at
    
FROM noaa_data.silver.silver_daily_observations
WHERE date >= CURRENT_DATE() - INTERVAL '30' DAY
  AND date <= CURRENT_DATE();