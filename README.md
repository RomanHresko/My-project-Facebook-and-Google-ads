WITH non_null_gf AS 
(WITH gf AS (SELECT ad_date, url_parameters, campaign_id, 'facebook ads' AS media_source, spend, impressions, reach, clicks, leads, value
FROM facebook_ads_basic_daily
UNION ALL
SELECT ad_date, url_parameters, campaign_name AS campaign_id, 'google ads' AS media_source, spend,  impressions, reach, clicks, leads,  value
    FROM google_ads_basic_daily gabd)
    SELECT 
    ad_date,
    url_parameters,
    media_source,
   	COALESCE(spend, 0) AS spend,
   	COALESCE(impressions, 0) AS impressions,
   	COALESCE(reach, 0) AS reach,
    COALESCE(clicks, 0) AS clicks,
    COALESCE(leads, 0) AS leads,
    COALESCE(value, 0) AS value
    FROM gf)
    SELECT 
    ad_date, 
    COALESCE(NULLIF(lower(substring(url_parameters, 'utm_campaign=([^\&]+)')), 'nan'), '') AS utm_campaign,
    sum(spend) AS total_spend, 
    sum(impressions)  AS total_impressions, 
    sum(clicks) AS total_clicks, 
    sum(value) AS total_value,
    CASE WHEN sum(impressions) > 0 THEN ROUND(sum(clicks):: numeric/sum(impressions):: NUMERIC *100, 2) END AS ctr,
    CASE WHEN sum(clicks) > 0 THEN ROUND(sum(spend):: numeric/sum(clicks):: NUMERIC, 2) END AS cpc,
    CASE WHEN sum(impressions) > 0 THEN ROUND(sum(spend):: numeric/sum(impressions):: NUMERIC *1000, 2) END AS cpm,
    CASE WHEN sum(spend) > 0 THEN ROUND(sum(value):: numeric/sum(spend):: NUMERIC -1, 2) END AS ROMI
    FROM non_null_gf
    GROUP BY 1,2;



-- Кодування рядків в URL. В значеннях поля utm_parameters кириличні літери закодовані в URL-рядок. Декодування значення utm_campaign за допомогою створення тимчасової функції

CREATE OR REPLACE FUNCTION pg_temp.decode_url_part(p varchar) RETURNS varchar AS $$
SELECT
    convert_from(
        CAST(E'\\x' || string_agg(
            CASE 
                WHEN length(r.m[1]) = 1 THEN encode(convert_to(r.m[1], 'UTF8'), 'hex')
                ELSE substring(r.m[1] from 2 for 2)
            END, ''
        ) AS bytea),
        'UTF8'
    )
FROM regexp_matches($1, '&[0-9a-f][0-9a-f]|.', 'gi') AS r(m);
$$ LANGUAGE SQL IMMUTABLE STRICT;


--Робота з датами і часом та віконні функції

 WITH all_data_total AS(WITH all_data AS (WITH non_null_gf AS 
(WITH gf AS (SELECT ad_date, url_parameters, campaign_id, 'facebook ads' AS media_source, spend, impressions, reach, clicks, leads, value
FROM facebook_ads_basic_daily
UNION ALL
SELECT ad_date, url_parameters, campaign_name AS campaign_id, 'google ads' AS media_source, spend,  impressions, reach, clicks, leads,  value
    FROM google_ads_basic_daily gabd)
    SELECT 
    ad_date,
    url_parameters,
    media_source,
   	COALESCE(spend, 0) AS spend,
   	COALESCE(impressions, 0) AS impressions,
   	COALESCE(reach, 0) AS reach,
    COALESCE(clicks, 0) AS clicks,
    COALESCE(leads, 0) AS leads,
    COALESCE(value, 0) AS value
    FROM gf)
    SELECT 
    date_trunc('month', ad_date) AS ad_month, 
    COALESCE(NULLIF(lower(substring(decode_url_part(url_parameters), 'utm_campaign=([^\&]+)')), 'nan'), '') AS utm_campaign,
    sum(spend) AS total_spend, 
    sum(impressions)  AS total_impressions, 
    sum(clicks) AS total_clicks, 
    sum(value) AS total_value,
    CASE WHEN sum(impressions) > 0 THEN ROUND(sum(clicks):: numeric/sum(impressions):: NUMERIC *100, 2) END AS ctr,
    CASE WHEN sum(clicks) > 0 THEN ROUND(sum(spend):: numeric/sum(clicks):: NUMERIC, 2) END AS cpc,
    CASE WHEN sum(impressions) > 0 THEN ROUND(sum(spend):: numeric/sum(impressions):: NUMERIC *1000, 2) END AS cpm,
    CASE WHEN sum(spend) > 0 THEN ROUND(sum(value):: numeric/sum(spend):: NUMERIC -1, 2) END AS ROMI
    FROM non_null_gf
    GROUP BY 1,2)
    SELECT ad_month, 
    utm_campaign, 
    total_spend, 
    total_impressions,
    total_clicks, 
    total_value,    
    cpc,
    cpm,
    ctr,
    romi,
   lag(cpc, 1) OVER(PARTITION BY utm_campaign ORDER BY ad_month) AS previous_cpc,
   lag(cpm, 1) OVER(PARTITION BY utm_campaign ORDER BY ad_month) AS previous_cpm,
   lag(ctr, 1) OVER(PARTITION BY utm_campaign ORDER BY ad_month) AS previous_ctr,
   lag(romi, 1) OVER(PARTITION BY utm_campaign ORDER BY ad_month) AS previous_romi
   FROM all_data)
   SELECT ad_month, 
    utm_campaign, 
    total_spend, 
    total_impressions,
    total_clicks, 
    total_value,    
    cpc,
    cpm,
    ctr,
    romi,
    CASE WHEN previous_cpc > 0 THEN ROUND(cpc / previous_cpc *100, 2) END AS diff_previous_cpc,
 	CASE WHEN previous_cpm > 0 THEN ROUND(cpm / previous_cpm *100, 2) END AS diff_previous_cpm,
    CASE WHEN previous_ctr > 0 THEN ROUND(ctr / previous_ctr *100, 2) END AS diff_previous_ctr,
    CASE WHEN previous_romi > 0 THEN ROUND(romi / previous_romi *100, 2) END AS diff_previous_romi
    FROM all_data_total;
