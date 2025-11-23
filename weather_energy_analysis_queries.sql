-- Weather + Energy Analysis Queries
-- Run these against the gold layer tables to explore correlations

-- ============================================================================
-- 1. Temperature vs Demand Correlation by Region
-- ============================================================================

SELECT 
    region,
    CORR(avg_temp_c, demand_mwh) as temp_demand_correlation,
    CORR(heating_degree_days, demand_mwh) as hdd_demand_correlation,
    CORR(cooling_degree_days, demand_mwh) as cdd_demand_correlation,
    COUNT(*) as days_analyzed
FROM noaa_data.gold.gold_weather_energy_correlation
WHERE date >= '2023-01-01'
GROUP BY region
ORDER BY region;

-- ============================================================================
-- 2. Peak Demand Days by Temperature
-- ============================================================================

SELECT 
    region,
    date,
    avg_temp_f,
    demand_mwh,
    heating_degree_days,
    cooling_degree_days,
    CASE 
        WHEN avg_temp_f < 32 THEN 'Extreme Cold'
        WHEN avg_temp_f < 50 THEN 'Cold'
        WHEN avg_temp_f > 90 THEN 'Extreme Heat'
        WHEN avg_temp_f > 75 THEN 'Hot'
        ELSE 'Moderate'
    END as temp_category
FROM noaa_data.gold.gold_weather_energy_correlation
WHERE date >= '2023-01-01'
ORDER BY demand_mwh DESC
LIMIT 50;

-- ============================================================================
-- 3. Seasonal Demand Patterns
-- ============================================================================

SELECT 
    region,
    YEAR(date) as year,
    QUARTER(date) as quarter,
    AVG(avg_temp_f) as avg_temp_f,
    AVG(demand_mwh) as avg_demand_mwh,
    MIN(demand_mwh) as min_demand_mwh,
    MAX(demand_mwh) as max_demand_mwh,
    SUM(heating_degree_days) as total_hdd,
    SUM(cooling_degree_days) as total_cdd
FROM noaa_data.gold.gold_weather_energy_correlation
WHERE date >= '2022-01-01'
GROUP BY region, YEAR(date), QUARTER(date)
ORDER BY region, year, quarter;

-- ============================================================================
-- 4. Heating vs Cooling Dominant Regions
-- ============================================================================

SELECT 
    region,
    SUM(heating_degree_days) as annual_hdd,
    SUM(cooling_degree_days) as annual_cdd,
    ROUND(SUM(heating_degree_days) / (SUM(heating_degree_days) + SUM(cooling_degree_days)) * 100, 1) as pct_heating,
    ROUND(SUM(cooling_degree_days) / (SUM(heating_degree_days) + SUM(cooling_degree_days)) * 100, 1) as pct_cooling,
    AVG(demand_mwh) as avg_demand_mwh
FROM noaa_data.gold.gold_weather_energy_correlation
WHERE date BETWEEN '2023-01-01' AND '2023-12-31'
GROUP BY region
ORDER BY pct_heating DESC;

-- ============================================================================
-- 5. Weather Sensitivity Analysis
-- ============================================================================

-- How much does demand increase per degree of temperature change?
WITH daily_changes AS (
    SELECT 
        region,
        date,
        avg_temp_c,
        demand_mwh,
        LAG(avg_temp_c) OVER (PARTITION BY region ORDER BY date) as prev_temp,
        LAG(demand_mwh) OVER (PARTITION BY region ORDER BY date) as prev_demand
    FROM noaa_data.gold.gold_weather_energy_correlation
    WHERE date >= '2023-01-01'
)
SELECT 
    region,
    ROUND(AVG(CASE 
        WHEN ABS(avg_temp_c - prev_temp) > 0 
        THEN (demand_mwh - prev_demand) / (avg_temp_c - prev_temp)
        ELSE NULL 
    END), 0) as avg_demand_change_per_degree_c,
    COUNT(*) as days_analyzed
FROM daily_changes
WHERE prev_temp IS NOT NULL
  AND ABS(avg_temp_c - prev_temp) BETWEEN 1 AND 10  -- Filter extreme changes
GROUP BY region
ORDER BY region;

-- ============================================================================
-- 6. Extreme Weather Event Impact
-- ============================================================================

-- Compare demand during extreme weather vs normal conditions
WITH categorized_days AS (
    SELECT 
        region,
        date,
        avg_temp_f,
        demand_mwh,
        CASE 
            WHEN avg_temp_f < 20 THEN 'Extreme Cold'
            WHEN avg_temp_f > 95 THEN 'Extreme Heat'
            ELSE 'Normal'
        END as weather_category
    FROM noaa_data.gold.gold_weather_energy_correlation
    WHERE date >= '2023-01-01'
)
SELECT 
    region,
    weather_category,
    COUNT(*) as days,
    ROUND(AVG(demand_mwh), 0) as avg_demand_mwh,
    ROUND(MAX(demand_mwh), 0) as max_demand_mwh,
    ROUND(AVG(demand_mwh) - AVG(AVG(demand_mwh)) OVER (PARTITION BY region), 0) as demand_vs_regional_avg
FROM categorized_days
GROUP BY region, weather_category
ORDER BY region, weather_category;

-- ============================================================================
-- 7. Precipitation Impact on Demand
-- ============================================================================

SELECT 
    region,
    CASE 
        WHEN avg_precipitation_mm = 0 THEN 'No Rain'
        WHEN avg_precipitation_mm < 5 THEN 'Light Rain'
        WHEN avg_precipitation_mm < 20 THEN 'Moderate Rain'
        ELSE 'Heavy Rain'
    END as precip_category,
    COUNT(*) as days,
    ROUND(AVG(demand_mwh), 0) as avg_demand_mwh,
    ROUND(AVG(avg_temp_c), 1) as avg_temp_c
FROM noaa_data.gold.gold_weather_energy_correlation
WHERE date >= '2023-01-01'
GROUP BY region, 
    CASE 
        WHEN avg_precipitation_mm = 0 THEN 'No Rain'
        WHEN avg_precipitation_mm < 5 THEN 'Light Rain'
        WHEN avg_precipitation_mm < 20 THEN 'Moderate Rain'
        ELSE 'Heavy Rain'
    END
ORDER BY region, precip_category;

-- ============================================================================
-- 8. Year-over-Year Comparison
-- ============================================================================

SELECT 
    region,
    YEAR(date) as year,
    ROUND(AVG(avg_temp_c), 1) as avg_temp_c,
    ROUND(AVG(demand_mwh), 0) as avg_demand_mwh,
    ROUND(SUM(heating_degree_days), 0) as total_hdd,
    ROUND(SUM(cooling_degree_days), 0) as total_cdd
FROM noaa_data.gold.gold_weather_energy_correlation
WHERE date >= '2020-01-01'
GROUP BY region, YEAR(date)
ORDER BY region, year;

-- ============================================================================
-- 9. Demand Forecasting Features
-- ============================================================================

-- Create a feature set for ML demand forecasting
SELECT 
    date,
    region,
    avg_temp_c,
    avg_temp_f,
    heating_degree_days,
    cooling_degree_days,
    avg_precipitation_mm,
    demand_mwh as target_demand,
    -- Lag features (previous day)
    LAG(demand_mwh, 1) OVER (PARTITION BY region ORDER BY date) as demand_lag1,
    LAG(avg_temp_c, 1) OVER (PARTITION BY region ORDER BY date) as temp_lag1,
    -- Rolling averages (7-day)
    AVG(demand_mwh) OVER (PARTITION BY region ORDER BY date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) as demand_avg_7d,
    AVG(avg_temp_c) OVER (PARTITION BY region ORDER BY date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING) as temp_avg_7d,
    -- Day of week
    DAYOFWEEK(date) as day_of_week,
    -- Month
    MONTH(date) as month,
    -- Is weekend
    CASE WHEN DAYOFWEEK(date) IN (1, 7) THEN 1 ELSE 0 END as is_weekend
FROM noaa_data.gold.gold_weather_energy_correlation
WHERE date >= '2023-01-01'
ORDER BY region, date;

-- ============================================================================
-- 10. Regional Efficiency Comparison
-- ============================================================================

-- Which regions are most efficient (lowest demand per degree day)?
SELECT 
    region,
    ROUND(AVG(demand_per_hdd), 0) as avg_demand_per_hdd,
    ROUND(AVG(demand_per_cdd), 0) as avg_demand_per_cdd,
    ROUND(AVG(demand_mwh), 0) as avg_total_demand,
    COUNT(*) as days_analyzed
FROM noaa_data.gold.gold_weather_energy_correlation
WHERE date >= '2023-01-01'
  AND (demand_per_hdd IS NOT NULL OR demand_per_cdd IS NOT NULL)
GROUP BY region
ORDER BY region;
