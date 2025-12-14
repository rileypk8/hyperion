-- Voice Actor Statistics
-- Analyze voice talent across the Disney animated universe

-- Top Voice Actors by Appearance Count
SELECT
    t.name AS voice_actor,
    COUNT(DISTINCT ca.id) AS total_appearances,
    COUNT(DISTINCT ca.character_id) AS unique_characters,
    COUNT(DISTINCT m.id) AS films_appeared,
    COUNT(DISTINCT c.franchise) AS franchises,
    MIN(m.year) AS first_appearance,
    MAX(m.year) AS last_appearance,
    MAX(m.year) - MIN(m.year) AS career_span_years
FROM
    hive.hyperion.talent t
    JOIN hive.hyperion.character_appearances ca ON t.id = ca.talent_id
    JOIN hive.hyperion.characters c ON ca.character_id = c.id
    JOIN hive.hyperion.media m ON ca.media_id = m.id
GROUP BY
    t.name
HAVING
    COUNT(DISTINCT ca.id) >= 3
ORDER BY
    total_appearances DESC
LIMIT 50;


-- Voice Actors by Total Box Office
SELECT
    t.name AS voice_actor,
    COUNT(DISTINCT ca.id) AS appearances,
    SUM(bot.total_revenue) AS total_box_office,
    AVG(bot.total_revenue) AS avg_box_office_per_film,
    MAX(bot.total_revenue) AS highest_grossing_film
FROM
    hive.hyperion.talent t
    JOIN hive.hyperion.character_appearances ca ON t.id = ca.talent_id
    JOIN hive.hyperion.films f ON ca.media_id = f.media_id
    LEFT JOIN hive.hyperion.box_office_total bot ON f.id = bot.film_id
GROUP BY
    t.name
HAVING
    SUM(bot.total_revenue) IS NOT NULL
ORDER BY
    total_box_office DESC
LIMIT 30;


-- Voice Actor Studio Loyalty
-- Which actors work primarily with which studios
SELECT
    t.name AS voice_actor,
    s.name AS studio_name,
    COUNT(DISTINCT ca.id) AS appearances_at_studio,
    ROUND(100.0 * COUNT(DISTINCT ca.id) / SUM(COUNT(DISTINCT ca.id)) OVER (PARTITION BY t.name), 2) AS pct_of_career
FROM
    hive.hyperion.talent t
    JOIN hive.hyperion.character_appearances ca ON t.id = ca.talent_id
    JOIN hive.hyperion.films f ON ca.media_id = f.media_id
    JOIN hive.hyperion.studios s ON f.studio_id = s.id
GROUP BY
    t.name, s.name
HAVING
    COUNT(DISTINCT ca.id) >= 2
ORDER BY
    t.name, appearances_at_studio DESC;


-- Prolific Protagonists Voice Actors
-- Actors who frequently voice hero characters
SELECT
    t.name AS voice_actor,
    r.name AS role_type,
    COUNT(*) AS role_count,
    COUNT(DISTINCT c.franchise) AS franchises
FROM
    hive.hyperion.talent t
    JOIN hive.hyperion.character_appearances ca ON t.id = ca.talent_id
    JOIN hive.hyperion.characters c ON ca.character_id = c.id
    JOIN hive.hyperion.roles r ON ca.role_id = r.id
WHERE
    r.name IN ('protagonist', 'deuteragonist', 'hero')
GROUP BY
    t.name, r.name
HAVING
    COUNT(*) >= 2
ORDER BY
    role_count DESC
LIMIT 20;


-- Voice Actor Cross-Media Appearances
-- Actors who appear in both films and games (via franchise)
WITH actor_media_types AS (
    SELECT
        t.name AS voice_actor,
        m.media_type,
        COUNT(DISTINCT ca.id) AS appearances
    FROM
        hive.hyperion.talent t
        JOIN hive.hyperion.character_appearances ca ON t.id = ca.talent_id
        JOIN hive.hyperion.media m ON ca.media_id = m.id
    GROUP BY
        t.name, m.media_type
)
SELECT
    voice_actor,
    MAX(CASE WHEN media_type = 'film' THEN appearances END) AS film_appearances,
    MAX(CASE WHEN media_type = 'game' THEN appearances END) AS game_appearances,
    SUM(appearances) AS total_appearances
FROM
    actor_media_types
GROUP BY
    voice_actor
HAVING
    MAX(CASE WHEN media_type = 'film' THEN appearances END) > 0
    AND MAX(CASE WHEN media_type = 'game' THEN appearances END) > 0
ORDER BY
    total_appearances DESC;
