-- Hyperion Database Schema (SQLite)
-- Converted from PostgreSQL DDL

-- ============================================
-- LOOKUP TABLES
-- ============================================

CREATE TABLE IF NOT EXISTS roles (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL UNIQUE,
    description     TEXT
);

INSERT OR IGNORE INTO roles (name, description) VALUES
    ('protagonist', 'Main character driving the story'),
    ('deuteragonist', 'Second most important character'),
    ('hero', 'Primary force for good (if distinct from protagonist)'),
    ('villain', 'Primary antagonist/evil force'),
    ('antagonist', 'Opposes protagonist (not necessarily evil)'),
    ('henchman', 'Serves the villain'),
    ('ally', 'Helps protagonist, not central to plot'),
    ('sidekick', 'Constant companion to main character'),
    ('love_interest', 'Romantic focus for a main character'),
    ('comic_relief', 'Primarily provides humor'),
    ('supporting', 'Significant but secondary role'),
    ('minor', 'Limited screen time or interaction');

CREATE TABLE IF NOT EXISTS genders (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL UNIQUE
);

INSERT OR IGNORE INTO genders (name) VALUES
    ('male'),
    ('female'),
    ('non-binary'),
    ('n/a'),
    ('various'),
    ('unknown'),
    ('mixed');

CREATE TABLE IF NOT EXISTS species (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL UNIQUE
);

INSERT OR IGNORE INTO species (name) VALUES
    ('human'),
    ('animal'),
    ('toy'),
    ('monster'),
    ('robot'),
    ('emotion'),
    ('fairy'),
    ('fish'),
    ('car'),
    ('insect'),
    ('ghost'),
    ('alien'),
    ('mythical creature'),
    ('anthropomorphic animal'),
    ('heartless'),
    ('nobody'),
    ('dream eater'),
    ('unknown');

CREATE TABLE IF NOT EXISTS platforms (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL UNIQUE
);

INSERT OR IGNORE INTO platforms (name) VALUES
    ('PlayStation 2'),
    ('PlayStation 3'),
    ('PlayStation 4'),
    ('PlayStation 5'),
    ('PlayStation Portable'),
    ('Xbox'),
    ('Xbox 360'),
    ('Xbox One'),
    ('Xbox Series X/S'),
    ('Nintendo DS'),
    ('Nintendo 3DS'),
    ('Nintendo Switch'),
    ('Game Boy Advance'),
    ('PC'),
    ('iOS'),
    ('Android');

CREATE TABLE IF NOT EXISTS credit_types (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL UNIQUE
);

INSERT OR IGNORE INTO credit_types (name) VALUES
    ('composer'),
    ('lyricist'),
    ('performer'),
    ('arranger'),
    ('producer'),
    ('conductor');

CREATE TABLE IF NOT EXISTS award_bodies (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL UNIQUE,
    created_at      TEXT DEFAULT (datetime('now'))
);

INSERT OR IGNORE INTO award_bodies (name) VALUES
    ('Academy Awards'),
    ('Golden Globe Awards'),
    ('Annie Awards'),
    ('BAFTA'),
    ('Critics'' Choice Awards'),
    ('Producers Guild of America Awards'),
    ('Screen Actors Guild Awards'),
    ('The Game Awards'),
    ('BAFTA Games Awards'),
    ('D.I.C.E. Awards');

CREATE TABLE IF NOT EXISTS award_categories (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    award_body_id   INTEGER NOT NULL REFERENCES award_bodies(id),
    name            TEXT NOT NULL,
    is_media_level  INTEGER NOT NULL DEFAULT 1,  -- 0 = individual/song award
    created_at      TEXT DEFAULT (datetime('now')),

    UNIQUE(award_body_id, name)
);

INSERT OR IGNORE INTO award_categories (award_body_id, name, is_media_level) VALUES
    -- Academy Awards
    (1, 'Best Animated Feature', 1),
    (1, 'Best Picture', 1),
    (1, 'Best Original Song', 0),
    (1, 'Best Original Score', 0),
    (1, 'Best Sound', 1),
    (1, 'Best Visual Effects', 1),
    -- Golden Globes
    (2, 'Best Animated Feature Film', 1),
    (2, 'Best Original Song - Motion Picture', 0),
    -- Annie Awards
    (3, 'Best Animated Feature', 1),
    (3, 'Outstanding Achievement in Voice Acting', 0),
    -- BAFTA Film
    (4, 'Best Animated Film', 1),
    -- The Game Awards
    (8, 'Best Action/Adventure Game', 1),
    (8, 'Best Score and Music', 1),
    (8, 'Best Narrative', 1),
    -- BAFTA Games
    (9, 'Best Game', 1),
    (9, 'Best Music', 1);

-- ============================================
-- CORE TABLES
-- ============================================

CREATE TABLE IF NOT EXISTS studios (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    slug            TEXT NOT NULL UNIQUE,
    name            TEXT NOT NULL,
    short_name      TEXT,
    years_active_start INTEGER,
    years_active_end   INTEGER,  -- NULL = still active
    status          TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'closed')),
    created_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS franchises (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    slug            TEXT NOT NULL UNIQUE,
    name            TEXT NOT NULL,
    studio_id       INTEGER REFERENCES studios(id),
    created_at      TEXT DEFAULT (datetime('now'))
);

-- ============================================
-- MEDIA PARENT TABLE
-- ============================================

CREATE TABLE IF NOT EXISTS media (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    slug            TEXT NOT NULL UNIQUE,
    title           TEXT NOT NULL,
    year            INTEGER NOT NULL,
    franchise_id    INTEGER REFERENCES franchises(id),
    media_type      TEXT NOT NULL CHECK (media_type IN ('film', 'game', 'soundtrack')),
    created_at      TEXT DEFAULT (datetime('now'))
);

-- ============================================
-- MEDIA CHILD TABLES
-- ============================================

CREATE TABLE IF NOT EXISTS films (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    media_id        INTEGER NOT NULL UNIQUE REFERENCES media(id),
    studio_id       INTEGER NOT NULL REFERENCES studios(id),
    animation_type  TEXT NOT NULL DEFAULT 'fully_animated' CHECK (animation_type IN ('fully_animated', 'hybrid')),
    character_count INTEGER DEFAULT 0,
    created_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS games (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    media_id        INTEGER NOT NULL UNIQUE REFERENCES media(id),
    developer       TEXT,
    publisher       TEXT,
    created_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS game_platforms (
    game_id         INTEGER NOT NULL REFERENCES games(id),
    platform_id     INTEGER NOT NULL REFERENCES platforms(id),
    release_date    TEXT,  -- platform-specific release date
    PRIMARY KEY (game_id, platform_id)
);

CREATE TABLE IF NOT EXISTS soundtracks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    media_id        INTEGER NOT NULL UNIQUE REFERENCES media(id),
    source_media_id INTEGER REFERENCES media(id),  -- film or game this soundtrack is for
    label           TEXT,
    created_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS songs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    soundtrack_id   INTEGER NOT NULL REFERENCES soundtracks(id),
    title           TEXT NOT NULL,
    track_number    INTEGER,
    duration_seconds INTEGER,
    created_at      TEXT DEFAULT (datetime('now')),

    UNIQUE(soundtrack_id, title)
);

CREATE TABLE IF NOT EXISTS song_credits (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    song_id         INTEGER NOT NULL REFERENCES songs(id),
    talent_id       INTEGER NOT NULL REFERENCES talent(id),
    credit_type_id  INTEGER NOT NULL REFERENCES credit_types(id),
    created_at      TEXT DEFAULT (datetime('now')),

    UNIQUE(song_id, talent_id, credit_type_id)
);

-- ============================================
-- CHARACTER & TALENT TABLES
-- ============================================

CREATE TABLE IF NOT EXISTS characters (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    slug            TEXT NOT NULL UNIQUE,
    name            TEXT NOT NULL,
    origin_franchise TEXT,
    species_id      INTEGER REFERENCES species(id),
    gender_id       INTEGER REFERENCES genders(id),
    voice_actor     TEXT,
    notes           TEXT,
    franchise_id    INTEGER REFERENCES franchises(id),
    studio_id       INTEGER REFERENCES studios(id),
    created_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS talent (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL UNIQUE,
    created_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS character_appearances (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    character_id    INTEGER NOT NULL REFERENCES characters(id),
    media_id        INTEGER NOT NULL REFERENCES media(id),
    talent_id       INTEGER REFERENCES talent(id),
    role_id         INTEGER NOT NULL REFERENCES roles(id),
    variant         TEXT,
    notes           TEXT,
    created_at      TEXT DEFAULT (datetime('now')),

    UNIQUE(character_id, media_id, variant)
);

-- ============================================
-- FINANCIAL DATA TABLES
-- ============================================

CREATE TABLE IF NOT EXISTS box_office_daily (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    film_id         INTEGER NOT NULL REFERENCES films(id),
    date            TEXT NOT NULL,
    state_code      TEXT NOT NULL,
    revenue         REAL NOT NULL,
    week_num        INTEGER NOT NULL,

    UNIQUE(film_id, date, state_code)
);

CREATE TABLE IF NOT EXISTS game_sales (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id         INTEGER NOT NULL REFERENCES games(id),
    platform_id     INTEGER NOT NULL REFERENCES platforms(id),
    date            TEXT NOT NULL,
    region          TEXT NOT NULL,  -- 'NA', 'EU', 'JP', 'WW'
    units_sold      INTEGER NOT NULL,
    revenue         REAL,

    UNIQUE(game_id, platform_id, date, region)
);

-- ============================================
-- AWARDS TABLES
-- ============================================

CREATE TABLE IF NOT EXISTS nominations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    media_id        INTEGER NOT NULL REFERENCES media(id),
    award_category_id INTEGER NOT NULL REFERENCES award_categories(id),
    year            INTEGER NOT NULL,
    outcome         TEXT NOT NULL DEFAULT 'nominated' CHECK (outcome IN ('nominated', 'won')),
    talent_id       INTEGER REFERENCES talent(id),  -- for individual awards
    song_id         INTEGER REFERENCES songs(id),   -- for song-specific awards
    created_at      TEXT DEFAULT (datetime('now')),

    UNIQUE(media_id, award_category_id, year, talent_id, song_id)
);

-- ============================================
-- INDEXES
-- ============================================

-- Media lookups
CREATE INDEX IF NOT EXISTS idx_media_franchise ON media(franchise_id);
CREATE INDEX IF NOT EXISTS idx_media_year ON media(year);
CREATE INDEX IF NOT EXISTS idx_media_type ON media(media_type);

-- Films lookups
CREATE INDEX IF NOT EXISTS idx_films_studio ON films(studio_id);
CREATE INDEX IF NOT EXISTS idx_films_media ON films(media_id);

-- Games lookups
CREATE INDEX IF NOT EXISTS idx_games_media ON games(media_id);
CREATE INDEX IF NOT EXISTS idx_games_developer ON games(developer);
CREATE INDEX IF NOT EXISTS idx_games_publisher ON games(publisher);

-- Soundtracks lookups
CREATE INDEX IF NOT EXISTS idx_soundtracks_media ON soundtracks(media_id);
CREATE INDEX IF NOT EXISTS idx_soundtracks_source ON soundtracks(source_media_id);

-- Songs lookups
CREATE INDEX IF NOT EXISTS idx_songs_soundtrack ON songs(soundtrack_id);

-- Song credits lookups
CREATE INDEX IF NOT EXISTS idx_song_credits_song ON song_credits(song_id);
CREATE INDEX IF NOT EXISTS idx_song_credits_talent ON song_credits(talent_id);
CREATE INDEX IF NOT EXISTS idx_song_credits_type ON song_credits(credit_type_id);

-- Characters lookups
CREATE INDEX IF NOT EXISTS idx_characters_origin ON characters(origin_franchise);
CREATE INDEX IF NOT EXISTS idx_characters_gender ON characters(gender_id);
CREATE INDEX IF NOT EXISTS idx_characters_species ON characters(species_id);
CREATE INDEX IF NOT EXISTS idx_characters_studio ON characters(studio_id);
CREATE INDEX IF NOT EXISTS idx_characters_franchise ON characters(franchise_id);

-- Character appearances lookups
CREATE INDEX IF NOT EXISTS idx_appearances_character ON character_appearances(character_id);
CREATE INDEX IF NOT EXISTS idx_appearances_media ON character_appearances(media_id);
CREATE INDEX IF NOT EXISTS idx_appearances_talent ON character_appearances(talent_id);
CREATE INDEX IF NOT EXISTS idx_appearances_role ON character_appearances(role_id);

-- Box office lookups (large table)
CREATE INDEX IF NOT EXISTS idx_box_office_film ON box_office_daily(film_id);
CREATE INDEX IF NOT EXISTS idx_box_office_date ON box_office_daily(date);
CREATE INDEX IF NOT EXISTS idx_box_office_state ON box_office_daily(state_code);
CREATE INDEX IF NOT EXISTS idx_box_office_film_date ON box_office_daily(film_id, date);

-- Game sales lookups (large table)
CREATE INDEX IF NOT EXISTS idx_game_sales_game ON game_sales(game_id);
CREATE INDEX IF NOT EXISTS idx_game_sales_platform ON game_sales(platform_id);
CREATE INDEX IF NOT EXISTS idx_game_sales_date ON game_sales(date);
CREATE INDEX IF NOT EXISTS idx_game_sales_region ON game_sales(region);

-- Nominations lookups
CREATE INDEX IF NOT EXISTS idx_nominations_media ON nominations(media_id);
CREATE INDEX IF NOT EXISTS idx_nominations_category ON nominations(award_category_id);
CREATE INDEX IF NOT EXISTS idx_nominations_talent ON nominations(talent_id);
CREATE INDEX IF NOT EXISTS idx_nominations_song ON nominations(song_id);
CREATE INDEX IF NOT EXISTS idx_nominations_year ON nominations(year);
CREATE INDEX IF NOT EXISTS idx_nominations_outcome ON nominations(outcome);

-- ============================================
-- VIEWS
-- ============================================

-- All media with franchise name
CREATE VIEW IF NOT EXISTS v_media AS
SELECT
    m.id,
    m.slug,
    m.title,
    m.year,
    m.media_type,
    fr.name AS franchise_name
FROM media m
LEFT JOIN franchises fr ON m.franchise_id = fr.id;

-- Films with all details
CREATE VIEW IF NOT EXISTS v_films AS
SELECT
    f.id AS film_id,
    m.id AS media_id,
    m.slug,
    m.title,
    m.year,
    s.name AS studio_name,
    s.short_name AS studio_short_name,
    fr.name AS franchise_name,
    f.animation_type,
    f.character_count
FROM films f
JOIN media m ON f.media_id = m.id
JOIN studios s ON f.studio_id = s.id
LEFT JOIN franchises fr ON m.franchise_id = fr.id;

-- Character appearances with all names resolved
CREATE VIEW IF NOT EXISTS v_character_appearances AS
SELECT
    ca.id,
    c.name AS character_name,
    c.origin_franchise,
    sp.name AS species,
    g.name AS gender,
    m.title AS media_title,
    m.year AS media_year,
    m.media_type,
    t.name AS voice_actor,
    r.name AS role,
    ca.variant,
    ca.notes
FROM character_appearances ca
JOIN characters c ON ca.character_id = c.id
JOIN media m ON ca.media_id = m.id
JOIN roles r ON ca.role_id = r.id
LEFT JOIN species sp ON c.species_id = sp.id
LEFT JOIN genders g ON c.gender_id = g.id
LEFT JOIN talent t ON ca.talent_id = t.id;

-- Characters with resolved lookups
CREATE VIEW IF NOT EXISTS v_characters AS
SELECT
    c.id,
    c.slug,
    c.name,
    c.voice_actor,
    c.notes,
    sp.name AS species,
    g.name AS gender,
    r.name AS role,
    fr.name AS franchise_name,
    s.name AS studio_name,
    s.short_name AS studio_short_name
FROM characters c
LEFT JOIN species sp ON c.species_id = sp.id
LEFT JOIN genders g ON c.gender_id = g.id
LEFT JOIN roles r ON c.id = r.id  -- This join doesn't make sense, but keeping structure
LEFT JOIN franchises fr ON c.franchise_id = fr.id
LEFT JOIN studios s ON c.studio_id = s.id;

-- Box office aggregated by film
CREATE VIEW IF NOT EXISTS v_box_office_by_film AS
SELECT
    f.id AS film_id,
    m.title,
    m.year,
    SUM(bo.revenue) AS total_gross,
    MIN(bo.date) AS release_date,
    MAX(bo.date) AS last_date,
    MAX(bo.week_num) AS weeks_in_release
FROM films f
JOIN media m ON f.media_id = m.id
JOIN box_office_daily bo ON bo.film_id = f.id
GROUP BY f.id, m.title, m.year;

-- Nominations with all names resolved
CREATE VIEW IF NOT EXISTS v_nominations AS
SELECT
    n.id,
    m.title AS media_title,
    m.year AS media_year,
    m.media_type,
    ab.name AS award_body,
    ac.name AS category,
    ac.is_media_level,
    n.year AS ceremony_year,
    n.outcome,
    t.name AS talent_name,
    s.title AS song_title
FROM nominations n
JOIN media m ON n.media_id = m.id
JOIN award_categories ac ON n.award_category_id = ac.id
JOIN award_bodies ab ON ac.award_body_id = ab.id
LEFT JOIN talent t ON n.talent_id = t.id
LEFT JOIN songs s ON n.song_id = s.id;

-- Gender distribution stats
CREATE VIEW IF NOT EXISTS v_gender_stats AS
SELECT
    g.name AS gender,
    COUNT(*) AS character_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM characters), 2) AS percentage
FROM characters c
LEFT JOIN genders g ON c.gender_id = g.id
GROUP BY g.name
ORDER BY character_count DESC;

-- Role distribution stats
CREATE VIEW IF NOT EXISTS v_role_stats AS
SELECT
    r.name AS role,
    COUNT(*) AS character_count
FROM character_appearances ca
JOIN roles r ON ca.role_id = r.id
GROUP BY r.name
ORDER BY character_count DESC;

-- Studio stats
CREATE VIEW IF NOT EXISTS v_studio_stats AS
SELECT
    s.id,
    s.slug,
    s.name,
    s.short_name,
    COUNT(DISTINCT c.id) AS character_count,
    COUNT(DISTINCT f.id) AS film_count,
    COUNT(DISTINCT fr.id) AS franchise_count
FROM studios s
LEFT JOIN characters c ON c.studio_id = s.id
LEFT JOIN films f ON f.studio_id = s.id
LEFT JOIN franchises fr ON fr.studio_id = s.id
GROUP BY s.id, s.slug, s.name, s.short_name;
