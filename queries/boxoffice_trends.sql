-- Box Office Performance Trends
-- Analyze revenue patterns and performance metrics

-- Weekly Decay Analysis
-- How quickly do films lose revenue week-over-week
SELECT
    week_number,
    COUNT(*) AS films_in_week,
    AVG(total_revenue) AS avg_weekly_revenue,
    AVG(wow_change) AS avg_week_over_week_change,
    MIN(wow_change) AS worst_drop,
    MAX(wow_change) AS best_hold
FROM
    hive.hyperion.box_office_weekly
WHERE
    week_number <= 12
GROUP BY
    week_number
ORDER BY
    week_number;


-- Weekend vs Weekday Revenue Split
SELECT
    s.name AS studio_name,
    SUM(bod.revenue) FILTER (WHERE bod.is_weekend) AS total_weekend_revenue,
    SUM(bod.revenue) FILTER (WHERE NOT bod.is_weekend) AS total_weekday_revenue,
    ROUND(
        100.0 * SUM(bod.revenue) FILTER (WHERE bod.is_weekend) /
        NULLIF(SUM(bod.revenue), 0), 2
    ) AS weekend_percentage
FROM
    hive.hyperion.box_office_daily bod
    JOIN hive.hyperion.films f ON bod.film_id = f.id
    JOIN hive.hyperion.studios s ON f.studio_id = s.id
GROUP BY
    s.name
ORDER BY
    weekend_percentage DESC;


-- Geographic Distribution
-- Which states contribute most to box office
SELECT
    state_code,
    SUM(revenue) AS total_revenue,
    COUNT(DISTINCT film_id) AS films_shown,
    AVG(revenue) AS avg_daily_revenue,
    ROUND(100.0 * SUM(revenue) / SUM(SUM(revenue)) OVER (), 2) AS market_share
FROM
    hive.hyperion.box_office_daily
GROUP BY
    state_code
ORDER BY
    total_revenue DESC;


-- Opening Weekend Performance
SELECT
    s.name AS studio_name,
    m.title,
    m.year,
    bot.opening_week_revenue,
    bot.total_revenue,
    ROUND(100.0 * bot.opening_week_revenue / NULLIF(bot.total_revenue, 0), 2) AS opening_pct_of_total,
    bot.weeks_in_release
FROM
    hive.hyperion.studios s
    JOIN hive.hyperion.films f ON s.id = f.studio_id
    JOIN hive.hyperion.media m ON f.media_id = m.id
    JOIN hive.hyperion.box_office_total bot ON f.id = bot.film_id
WHERE
    bot.opening_week_revenue IS NOT NULL
ORDER BY
    bot.opening_week_revenue DESC
LIMIT 25;


-- Long-Running Films
-- Films with extended theatrical runs
SELECT
    m.title,
    m.year,
    s.name AS studio_name,
    bot.weeks_in_release,
    bot.total_revenue,
    bot.opening_week_revenue,
    ROUND(bot.total_revenue / NULLIF(bot.opening_week_revenue, 0), 2) AS total_to_opening_ratio
FROM
    hive.hyperion.films f
    JOIN hive.hyperion.media m ON f.media_id = m.id
    JOIN hive.hyperion.studios s ON f.studio_id = s.id
    JOIN hive.hyperion.box_office_total bot ON f.id = bot.film_id
WHERE
    bot.weeks_in_release >= 10
ORDER BY
    bot.weeks_in_release DESC, bot.total_revenue DESC
LIMIT 20;


-- Yearly Box Office Trends
SELECT
    film_year AS year,
    COUNT(DISTINCT film_id) AS films,
    SUM(total_revenue) AS total_annual_revenue,
    AVG(total_revenue) AS avg_film_revenue,
    MAX(total_revenue) AS top_film_revenue
FROM
    hive.hyperion.box_office_total
WHERE
    film_year IS NOT NULL
GROUP BY
    film_year
ORDER BY
    film_year;
