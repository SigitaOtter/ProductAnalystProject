-----------------------------------------------------------------------------------------------
-- EXPLORATION AND VALIDATION

-- daily first purchases (if a user bought twice on same day, only first purchase is counted in)
SELECT
    PARSE_DATE('%Y%m%d', event_date) AS purchase_date,
    COUNT(DISTINCT user_pseudo_id) AS purchases
FROM `tc-da-1.turing_data_analytics.raw_events`
WHERE event_name = 'purchase'
GROUP BY purchase_date
ORDER BY purchase_date

-- user 1026932.0858862293 bought twice on 2020-12-01, only first purchase should be taken into account
SELECT
    user_pseudo_id,
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    COUNT(*) AS purchases
FROM `tc-da-1.turing_data_analytics.raw_events`
WHERE event_name = 'purchase' 
      AND user_pseudo_id = '1026932.0858862293' 
      AND PARSE_DATE('%Y%m%d', event_date) = '2020-12-01'
GROUP BY user_pseudo_id, event_date
ORDER BY user_pseudo_id, event_date

-- only event with timestamp 1606815536636655 should be concidered
SELECT
    user_pseudo_id,
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    event_timestamp
FROM `tc-da-1.turing_data_analytics.raw_events`
WHERE event_name = 'purchase' 
      AND user_pseudo_id = '1026932.0858862293' 
      AND PARSE_DATE('%Y%m%d', event_date) = '2020-12-01'
ORDER BY user_pseudo_id, event_date, event_timestamp

-- first daily purchase per user takes only needed event by timestamp
SELECT
    user_pseudo_id,
    PARSE_DATE('%Y%m%d', event_date) AS purchase_date,
    MIN(event_timestamp) AS purchase_timestamp
FROM `tc-da-1.turing_data_analytics.raw_events`
WHERE event_name = 'purchase'
      AND user_pseudo_id = '1026932.0858862293' 
      AND PARSE_DATE('%Y%m%d', event_date) = '2020-12-01'
GROUP BY user_pseudo_id, purchase_date 
ORDER BY purchase_date, purchase_timestamp, user_pseudo_id

-- does first touch timestamp reset every day?
SELECT
    user_pseudo_id,
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    event_timestamp,
    user_first_touch_timestamp
FROM `tc-da-1.turing_data_analytics.raw_events`
WHERE event_name = 'purchase' 
      AND user_pseudo_id = '1026932.0858862293' 
      AND (PARSE_DATE('%Y%m%d', event_date) = '2020-12-01' OR PARSE_DATE('%Y%m%d', event_date) = '2020-12-08')
ORDER BY user_pseudo_id, event_date, event_timestamp
-- no, first touch timestap is per user not per user per day and cannot be used for daily purcase dynamic analysis

-- first timestamp per day per user
SELECT
    user_pseudo_id,
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    MIN(event_timestamp) AS first_daily_timestamp
FROM `tc-da-1.turing_data_analytics.raw_events`
WHERE user_pseudo_id = '1026932.0858862293' 
      AND PARSE_DATE('%Y%m%d', event_date) = '2020-12-01'
GROUP BY user_pseudo_id, event_date 
ORDER BY event_date, first_daily_timestamp, user_pseudo_id

-- session start seems like a legit first event in the funnel
SELECT
    user_pseudo_id,
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    event_name
FROM `tc-da-1.turing_data_analytics.raw_events`
WHERE event_timestamp = 1606814706389678

-- 830 seconds (a little under 14 minutes seems legit timespan to finalize a purchase)
SELECT ROUND((1606815536636655-1606814706389678)/1000000,0)

-- on 2020-11-11 user 56920896.3476155826 had a session_start and purchase event at exactly same timestamp, should investigate if hti is legit
SELECT
    user_pseudo_id,
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    event_timestamp,
    event_name
FROM `tc-da-1.turing_data_analytics.raw_events`
WHERE user_pseudo_id LIKE '%56920896.3476155%' 
      AND PARSE_DATE('%Y%m%d', event_date) = '2020-11-11'


-- time to purchase - general
WITH
purchases AS (
    SELECT
        user_pseudo_id,
        PARSE_DATE('%Y%m%d', event_date) AS purchase_date,
        MIN(event_timestamp) AS purchase_timestamp
    FROM `tc-da-1.turing_data_analytics.raw_events`
    WHERE event_name = 'purchase'
    GROUP BY user_pseudo_id, purchase_date
),
first_events AS (
    SELECT
        user_pseudo_id,
        PARSE_DATE('%Y%m%d', event_date) AS event_date,
        MIN(event_timestamp) AS first_daily_timestamp
    FROM `tc-da-1.turing_data_analytics.raw_events`
    GROUP BY user_pseudo_id, event_date 
)

SELECT
    p.purchase_date,
    p.user_pseudo_id,
    FLOOR((p.purchase_timestamp - fe.first_daily_timestamp)/1000000) AS full_seconds_to_purchase,
    FLOOR((p.purchase_timestamp - fe.first_daily_timestamp)/1000000/60) AS full_minutes_to_purchase,
FROM purchases p
    LEFT JOIN first_events fe
        ON p.user_pseudo_id = fe.user_pseudo_id AND p.purchase_date = fe.event_date
ORDER BY p.purchase_date, p.user_pseudo_id

-----------------------------------------------------------------------------------------------


-----------------------------------------------------------------------------------------------
-- FINAL QUERY

-- time to purchase with additional attributes
WITH
purchases AS (
    SELECT
        user_pseudo_id,
        PARSE_DATE('%Y%m%d', event_date) AS purchase_date,
        MIN(event_timestamp) AS purchase_timestamp
    FROM `tc-da-1.turing_data_analytics.raw_events`
    WHERE event_name = 'purchase'
          AND total_item_quantity IS NOT NULL
    GROUP BY user_pseudo_id, purchase_date
),
purchased_days AS (
    SELECT
        user_pseudo_id,
        COUNT(*) AS days_with_purchases,
        MIN(purchase_date) AS first_purchase_date
    FROM purchases
    GROUP BY user_pseudo_id
),
enriched_purchases AS (
    SELECT
        p.user_pseudo_id,
        p.purchase_date,
        p.purchase_timestamp,
        r.category AS device_category,
        r.operating_system,
        r.mobile_brand_name,
        r.browser,
        r.browser_version,
        r.country,
        r.total_item_quantity,
        r.purchase_revenue_in_usd,
        ROUND(r.purchase_revenue_in_usd/r.total_item_quantity,2) AS avg_item_revenue_in_usd,
        CASE WHEN pd.days_with_purchases > 1 
		     THEN 'returning'
			 ELSE 'one_time'
			 END AS is_returning,
        CASE WHEN p.purchase_date = pd.first_purchase_date 
		     THEN 'first' 
			 ELSE 'repeat' 
			 END AS is_repeat_purchase
    FROM purchases p
        LEFT JOIN `tc-da-1.turing_data_analytics.raw_events` r
            ON p.user_pseudo_id = r.user_pseudo_id 
               AND r.event_timestamp = p.purchase_timestamp 
               AND r.event_name = 'purchase'
        LEFT JOIN purchased_days pd
            ON p.user_pseudo_id = pd.user_pseudo_id 
),
first_events AS (
    SELECT
        user_pseudo_id,
        PARSE_DATE('%Y%m%d', event_date) AS event_date,
        MIN(event_timestamp) AS first_daily_timestamp
    FROM `tc-da-1.turing_data_analytics.raw_events`
    GROUP BY user_pseudo_id, event_date 
)

SELECT
    ep.purchase_date,
    ep.user_pseudo_id,
    ep.device_category,
    ep.operating_system,
    ep.mobile_brand_name,
    ep.browser,
    ep.browser_version,
    ep.country,
    ep.total_item_quantity,
    ep.purchase_revenue_in_usd,
    ep.avg_item_revenue_in_usd,
    ep.is_returning,
    ep.is_repeat_purchase,
    FLOOR((ep.purchase_timestamp - fe.first_daily_timestamp)/1000000) AS full_seconds_to_purchase,
    FLOOR((ep.purchase_timestamp - fe.first_daily_timestamp)/1000000/60) AS full_minutes_to_purchase,
    FLOOR((ep.purchase_timestamp - fe.first_daily_timestamp)/1000000/60/60) AS full_hours_to_purchase
FROM enriched_purchases ep
    LEFT JOIN first_events fe
        ON ep.user_pseudo_id = fe.user_pseudo_id AND ep.purchase_date = fe.event_date
ORDER BY ep.purchase_date, ep.user_pseudo_id

