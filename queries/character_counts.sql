-- Character Counts by Franchise
-- Count characters and their attributes by franchise

SELECT
    c.franchise,
    COUNT(*) AS total_characters,
    COUNT(DISTINCT c.species) AS unique_species,
    SUM(CASE WHEN g.name = 'male' THEN 1 ELSE 0 END) AS male_count,
    SUM(CASE WHEN g.name = 'female' THEN 1 ELSE 0 END) AS female_count,
    SUM(CASE WHEN g.name = 'non-binary' THEN 1 ELSE 0 END) AS nonbinary_count,
    SUM(CASE WHEN r.name IN ('protagonist', 'deuteragonist', 'hero') THEN 1 ELSE 0 END) AS hero_count,
    SUM(CASE WHEN r.name IN ('villain', 'antagonist') THEN 1 ELSE 0 END) AS villain_count
FROM
    hive.hyperion.characters c
    LEFT JOIN hive.hyperion.genders g ON c.gender_id = g.id
    LEFT JOIN hive.hyperion.roles r ON c.role_id = r.id
GROUP BY
    c.franchise
ORDER BY
    total_characters DESC;


-- Character Counts by Studio
SELECT
    s.name AS studio_name,
    COUNT(DISTINCT c.id) AS total_characters,
    COUNT(DISTINCT c.franchise) AS franchises,
    COUNT(DISTINCT c.species) AS unique_species,
    ROUND(AVG(ca.appearance_count), 2) AS avg_appearances_per_char
FROM
    hive.hyperion.studios s
    JOIN hive.hyperion.films f ON s.id = f.studio_id
    JOIN hive.hyperion.character_appearances ca ON f.media_id = ca.media_id
    JOIN hive.hyperion.characters c ON ca.character_id = c.id
GROUP BY
    s.name
ORDER BY
    total_characters DESC;


-- Species Distribution
SELECT
    c.species,
    COUNT(*) AS character_count,
    COUNT(DISTINCT c.franchise) AS franchises_appeared,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM
    hive.hyperion.characters c
WHERE
    c.species IS NOT NULL
GROUP BY
    c.species
ORDER BY
    character_count DESC
LIMIT 20;


-- Gender Distribution Over Time
SELECT
    m.year,
    g.name AS gender,
    COUNT(*) AS character_count
FROM
    hive.hyperion.character_appearances ca
    JOIN hive.hyperion.characters c ON ca.character_id = c.id
    JOIN hive.hyperion.genders g ON c.gender_id = g.id
    JOIN hive.hyperion.media m ON ca.media_id = m.id
WHERE
    m.year IS NOT NULL
GROUP BY
    m.year, g.name
ORDER BY
    m.year, g.name;


-- Role Distribution by Gender
SELECT
    r.name AS role,
    g.name AS gender,
    COUNT(*) AS character_count
FROM
    hive.hyperion.characters c
    JOIN hive.hyperion.roles r ON c.role_id = r.id
    JOIN hive.hyperion.genders g ON c.gender_id = g.id
GROUP BY
    r.name, g.name
ORDER BY
    r.name, character_count DESC;
