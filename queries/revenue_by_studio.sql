-- Revenue by Studio
-- Aggregate box office revenue by studio with film counts

SELECT
    s.name AS studio_name,
    s.status AS studio_status,
    COUNT(DISTINCT f.id) AS film_count,
    SUM(bot.total_revenue) AS total_box_office,
    AVG(bot.total_revenue) AS avg_box_office_per_film,
    MAX(bot.total_revenue) AS highest_grossing,
    MIN(bot.total_revenue) AS lowest_grossing,
    SUM(bot.opening_week_revenue) AS total_opening_weekend,
    AVG(bot.opening_week_revenue) AS avg_opening_weekend
FROM
    hive.hyperion.studios s
    JOIN hive.hyperion.films f ON s.id = f.studio_id
    JOIN hive.hyperion.media m ON f.media_id = m.id
    LEFT JOIN hive.hyperion.box_office_total bot ON f.id = bot.film_id
GROUP BY
    s.name, s.status
ORDER BY
    total_box_office DESC NULLS LAST;


-- Revenue by Studio and Year
-- Time series of studio performance

SELECT
    s.name AS studio_name,
    m.year,
    COUNT(DISTINCT f.id) AS films_released,
    SUM(bot.total_revenue) AS annual_revenue,
    AVG(bot.total_revenue) AS avg_revenue_per_film
FROM
    hive.hyperion.studios s
    JOIN hive.hyperion.films f ON s.id = f.studio_id
    JOIN hive.hyperion.media m ON f.media_id = m.id
    LEFT JOIN hive.hyperion.box_office_total bot ON f.id = bot.film_id
GROUP BY
    s.name, m.year
ORDER BY
    s.name, m.year;


-- Top 10 Highest Grossing Films by Studio
SELECT
    s.name AS studio_name,
    m.title,
    m.year,
    bot.total_revenue,
    bot.opening_week_revenue,
    bot.weeks_in_release,
    RANK() OVER (PARTITION BY s.name ORDER BY bot.total_revenue DESC) AS studio_rank
FROM
    hive.hyperion.studios s
    JOIN hive.hyperion.films f ON s.id = f.studio_id
    JOIN hive.hyperion.media m ON f.media_id = m.id
    JOIN hive.hyperion.box_office_total bot ON f.id = bot.film_id
WHERE
    bot.total_revenue IS NOT NULL
QUALIFY
    studio_rank <= 10
ORDER BY
    s.name, studio_rank;
