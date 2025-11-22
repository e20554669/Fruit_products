USE `tjr103-team02`;

-- 建立 v_crop_daily VIEW
CREATE VIEW `v_crop_daily` AS
WITH 
-- ============================================
-- CTE 1: 計算每日每個 crop_id 的平均價格
-- ============================================
crop_price AS (
    SELECT 
        `date`,
        crop_id,
        ROUND(AVG(avg_price), 2) AS crop_price_per_kg
    FROM volume
    GROUP BY `date`, crop_id
),

-- ============================================
-- CTE 2: 決定每個作物的代表海拔
-- ============================================
crop_altitude AS (
    SELECT DISTINCT 
        crop_id,
        altitude AS crop_altitude
    FROM area_production
),

-- ============================================
-- CTE 3: 城市權重
-- ============================================

area_prod AS(
	SELECT 
		`year`
        ,`crop_id`
        ,`city_id`
        ,`planted_area`
        ,`production`
    FROM area_production
    UNION ALL -- 使用union all將當年度資訊以去年度資訊做為替代
	SELECT
		YEAR(CURDATE()) AS `year`
        ,`crop_id`
        ,`city_id`
        ,`planted_area`
        ,`production`
    FROM area_production
    WHERE `year` = YEAR(CURDATE()) - 1
),
-- 計算權重
city_weights AS (
    SELECT
        `year`,
        `crop_id`,
        `city_id`,
        `planted_area` / SUM(`planted_area`) OVER (PARTITION BY `year`, `crop_id`) AS area_weight,
        `production`   / SUM(`production`)   OVER (PARTITION BY `year`, `crop_id`) AS prod_weight
    FROM area_prod
),

-- ============================================
-- CTE 4: 天氣資料 pivot (High/Low 分開)
-- ============================================
weather_pivot AS (
    SELECT 
        `date`,
        city_id,
        MAX(CASE WHEN altitude = 'High' THEN station_pressure END) AS station_pressure_High,
        MAX(CASE WHEN altitude = 'Low' THEN station_pressure END) AS station_pressure_Low,
        MAX(CASE WHEN altitude = 'High' THEN air_temperature END) AS air_temperature_High,
        MAX(CASE WHEN altitude = 'Low' THEN air_temperature END) AS air_temperature_Low,
        MAX(CASE WHEN altitude = 'High' THEN relative_humidity END) AS relative_humidity_High,
        MAX(CASE WHEN altitude = 'Low' THEN relative_humidity END) AS relative_humidity_Low,
        MAX(CASE WHEN altitude = 'High' THEN wind_speed END) AS wind_speed_High,
        MAX(CASE WHEN altitude = 'Low' THEN wind_speed END) AS wind_speed_Low,
        MAX(CASE WHEN altitude = 'High' THEN precipitation END) AS precipitation_High,
        MAX(CASE WHEN altitude = 'Low' THEN precipitation END) AS precipitation_Low
    FROM weather
    GROUP BY `date`, city_id
),

-- ============================================
-- CTE 5: 每日颱風資訊
-- ============================================
typhoon_daily AS (
    SELECT 
        `date`,
        MAX(is_typhoon) AS is_typhoon,
        MAX(CASE WHEN typhoon_name IS NOT NULL AND TRIM(typhoon_name) != '' 
            THEN typhoon_name END) AS typhoon_name
    FROM weather
    GROUP BY `date`
),

-- ============================================
-- CTE 6: 從交易量取得所有 (date, crop_id) 組合
-- ============================================
date_crop_pairs AS (
    SELECT DISTINCT
        `date`,
        crop_id,
        YEAR(`date`) AS `year`
    FROM volume
),

-- ============================================
-- CTE 7: 合併權重和天氣，選擇對應海拔的天氣值
-- ============================================
merged_data AS (
    SELECT 
        dcp.`date`,
        dcp.crop_id,
        ca.crop_altitude,
        cw.city_id,
        cw.area_weight,
        cw.prod_weight,
        -- 根據作物海拔選擇天氣值
        -- station_pressure
        CASE 
            WHEN ca.crop_altitude = 'High' THEN 
                COALESCE(wp.station_pressure_High, wp.station_pressure_Low)
            ELSE 
                COALESCE(wp.station_pressure_Low, wp.station_pressure_High)
        END AS station_pressure,
        -- air_temperature
        CASE 
            WHEN ca.crop_altitude = 'High' THEN 
                COALESCE(wp.air_temperature_High, wp.air_temperature_Low)
            ELSE 
                COALESCE(wp.air_temperature_Low, wp.air_temperature_High)
        END AS air_temperature,
        -- relative_humidity
        CASE 
            WHEN ca.crop_altitude = 'High' THEN 
                COALESCE(wp.relative_humidity_High, wp.relative_humidity_Low)
            ELSE 
                COALESCE(wp.relative_humidity_Low, wp.relative_humidity_High)
        END AS relative_humidity,
        -- wind_speed
        CASE 
            WHEN ca.crop_altitude = 'High' THEN 
                COALESCE(wp.wind_speed_High, wp.wind_speed_Low)
            ELSE 
                COALESCE(wp.wind_speed_Low, wp.wind_speed_High)
        END AS wind_speed,
        -- precipitation
        CASE 
            WHEN ca.crop_altitude = 'High' THEN 
                COALESCE(wp.precipitation_High, wp.precipitation_Low)
            ELSE 
                COALESCE(wp.precipitation_Low, wp.precipitation_High)
        END AS precipitation
    FROM date_crop_pairs dcp
    LEFT JOIN crop_altitude ca ON dcp.crop_id = ca.crop_id
    LEFT JOIN city_weights cw 
        ON dcp.`year` = cw.`year` AND dcp.crop_id = cw.crop_id
    LEFT JOIN weather_pivot wp 
        ON dcp.`date` = wp.`date` AND cw.city_id = wp.city_id
),

-- ============================================
-- CTE 8: 計算加權平均天氣
-- ============================================
weighted_weather AS (
    SELECT 
        `date`,
        crop_id,
        -- 以面積作為加權的氣象資訊
        ROUND(
            SUM(CASE WHEN station_pressure IS NOT NULL THEN area_weight * station_pressure ELSE 0 END) /
            NULLIF(SUM(CASE WHEN station_pressure IS NOT NULL THEN area_weight ELSE 0 END), 0)
        , 2) AS station_pressure_area,
        ROUND(
            SUM(CASE WHEN air_temperature IS NOT NULL THEN area_weight * air_temperature ELSE 0 END) /
            NULLIF(SUM(CASE WHEN air_temperature IS NOT NULL THEN area_weight ELSE 0 END), 0)
        , 2) AS air_temperature_area,
        ROUND(
            SUM(CASE WHEN relative_humidity IS NOT NULL THEN area_weight * relative_humidity ELSE 0 END) /
            NULLIF(SUM(CASE WHEN relative_humidity IS NOT NULL THEN area_weight ELSE 0 END), 0)
        , 2) AS relative_humidity_area,
        ROUND(
            SUM(CASE WHEN wind_speed IS NOT NULL THEN area_weight * wind_speed ELSE 0 END) /
            NULLIF(SUM(CASE WHEN wind_speed IS NOT NULL THEN area_weight ELSE 0 END), 0)
        , 2) AS wind_speed_area,
        ROUND(
            SUM(CASE WHEN precipitation IS NOT NULL THEN area_weight * precipitation ELSE 0 END) /
            NULLIF(SUM(CASE WHEN precipitation IS NOT NULL THEN area_weight ELSE 0 END), 0)
        , 2) AS precipitation_area,
        -- 以產量作為加權的氣象資訊
        ROUND(
            SUM(CASE WHEN station_pressure IS NOT NULL THEN prod_weight * station_pressure ELSE 0 END) /
            NULLIF(SUM(CASE WHEN station_pressure IS NOT NULL THEN prod_weight ELSE 0 END), 0)
        , 2) AS station_pressure_prod,
        ROUND(
            SUM(CASE WHEN air_temperature IS NOT NULL THEN prod_weight * air_temperature ELSE 0 END) /
            NULLIF(SUM(CASE WHEN air_temperature IS NOT NULL THEN prod_weight ELSE 0 END), 0)
        , 2) AS air_temperature_prod,
        ROUND(
            SUM(CASE WHEN relative_humidity IS NOT NULL THEN prod_weight * relative_humidity ELSE 0 END) /
            NULLIF(SUM(CASE WHEN relative_humidity IS NOT NULL THEN prod_weight ELSE 0 END), 0)
        , 2) AS relative_humidity_prod,
        ROUND(
            SUM(CASE WHEN wind_speed IS NOT NULL THEN prod_weight * wind_speed ELSE 0 END) /
            NULLIF(SUM(CASE WHEN wind_speed IS NOT NULL THEN prod_weight ELSE 0 END), 0)
        , 2) AS wind_speed_prod,
        ROUND(
            SUM(CASE WHEN precipitation IS NOT NULL THEN prod_weight * precipitation ELSE 0 END) /
            NULLIF(SUM(CASE WHEN precipitation IS NOT NULL THEN prod_weight ELSE 0 END), 0)
        , 2) AS precipitation_prod
    FROM merged_data
    GROUP BY `date`, crop_id
)

-- ============================================
-- 最終 SELECT: 組合所有資料
-- ============================================
SELECT 
    ww.`date`,
    ww.crop_id,
    cp.crop_price_per_kg,
    ww.station_pressure_area,
    ww.air_temperature_area,
    ww.relative_humidity_area,
    ww.wind_speed_area,
    ww.precipitation_area,
    ww.station_pressure_prod,
    ww.air_temperature_prod,
    ww.relative_humidity_prod,
    ww.wind_speed_prod,
    ww.precipitation_prod,
    COALESCE(td.is_typhoon, 0) AS is_typhoon,
    td.typhoon_name
FROM weighted_weather ww
LEFT JOIN crop_price cp 
    ON ww.`date` = cp.`date` AND ww.crop_id = cp.crop_id
LEFT JOIN typhoon_daily td 
    ON ww.`date` = td.`date`
ORDER BY ww.`date`, ww.crop_id;