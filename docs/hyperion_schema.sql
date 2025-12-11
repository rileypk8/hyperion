-- Hyperion Database Schema
-- PostgreSQL DDL

-- ============================================
-- ENUM TYPES (only where truly fixed)
-- ============================================

CREATE TYPE studio_status AS ENUM ('active', 'closed');
CREATE TYPE animation_type AS ENUM ('fully_animated', 'hybrid');
CREATE TYPE media_type AS ENUM ('film', 'game', 'soundtrack');
CREATE TYPE award_outcome AS ENUM ('nominated', 'won');

-- ============================================
-- LOOKUP TABLES
-- ============================================

CREATE TABLE roles (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(50) NOT NULL UNIQUE,
    description     TEXT
);

INSERT INTO roles (name, description) VALUES
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

CREATE TABLE genders (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(50) NOT NULL UNIQUE
);

INSERT INTO genders (name) VALUES
    ('male'),
    ('female'),
    ('non-binary'),
    ('n/a'),
    ('various');

CREATE TABLE species (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(100) NOT NULL UNIQUE
);

INSERT INTO species (name) VALUES
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
    ('dream eater');

CREATE TABLE platforms (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(50) NOT NULL UNIQUE
);

INSERT INTO platforms (name) VALUES
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

CREATE TABLE credit_types (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(50) NOT NULL UNIQUE
);

INSERT INTO credit_types (name) VALUES
    ('composer'),
    ('lyricist'),
    ('performer'),
    ('arranger'),
    ('producer'),
    ('conductor');

CREATE TABLE award_bodies (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(100) NOT NULL UNIQUE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO award_bodies (name) VALUES
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

CREATE TABLE award_categories (
    id              SERIAL PRIMARY KEY,
    award_body_id   INT NOT NULL REFERENCES award_bodies(id),
    name            VARCHAR(150) NOT NULL,
    is_media_level  BOOLEAN NOT NULL DEFAULT true,  -- false = individual/song award
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(award_body_id, name)
);

INSERT INTO award_categories (award_body_id, name, is_media_level) VALUES
    -- Academy Awards
    (1, 'Best Animated Feature', true),
    (1, 'Best Picture', true),
    (1, 'Best Original Song', false),
    (1, 'Best Original Score', false),
    (1, 'Best Sound', true),
    (1, 'Best Visual Effects', true),
    -- Golden Globes
    (2, 'Best Animated Feature Film', true),
    (2, 'Best Original Song - Motion Picture', false),
    -- Annie Awards
    (3, 'Best Animated Feature', true),
    (3, 'Outstanding Achievement in Voice Acting', false),
    -- BAFTA Film
    (4, 'Best Animated Film', true),
    -- The Game Awards
    (8, 'Best Action/Adventure Game', true),
    (8, 'Best Score and Music', true),
    (8, 'Best Narrative', true),
    -- BAFTA Games
    (9, 'Best Game', true),
    (9, 'Best Music', true);

-- ============================================
-- CORE TABLES
-- ============================================

CREATE TABLE studios (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(100) NOT NULL UNIQUE,
    years_active_start INT NOT NULL,
    years_active_end   INT,  -- NULL = still active
    status          studio_status NOT NULL DEFAULT 'active',
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE franchises (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(100) NOT NULL UNIQUE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- MEDIA PARENT TABLE
-- ============================================

CREATE TABLE media (
    id              SERIAL PRIMARY KEY,
    title           VARCHAR(200) NOT NULL,
    year            INT NOT NULL,
    franchise_id    INT NOT NULL REFERENCES franchises(id),
    media_type      media_type NOT NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(title, year, media_type)  -- allows "Frozen" film and "Frozen" soundtrack same year
);

-- ============================================
-- MEDIA CHILD TABLES
-- ============================================

CREATE TABLE films (
    id              SERIAL PRIMARY KEY,
    media_id        INT NOT NULL UNIQUE REFERENCES media(id),
    studio_id       INT NOT NULL REFERENCES studios(id),
    animation_type  animation_type NOT NULL DEFAULT 'fully_animated',
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE games (
    id              SERIAL PRIMARY KEY,
    media_id        INT NOT NULL UNIQUE REFERENCES media(id),
    developer       VARCHAR(100),
    publisher       VARCHAR(100),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE game_platforms (
    game_id         INT NOT NULL REFERENCES games(id),
    platform_id     INT NOT NULL REFERENCES platforms(id),
    release_date    DATE,  -- platform-specific release date
    PRIMARY KEY (game_id, platform_id)
);

CREATE TABLE soundtracks (
    id              SERIAL PRIMARY KEY,
    media_id        INT NOT NULL UNIQUE REFERENCES media(id),
    source_media_id INT REFERENCES media(id),  -- film or game this soundtrack is for
    label           VARCHAR(100),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE songs (
    id              SERIAL PRIMARY KEY,
    soundtrack_id   INT NOT NULL REFERENCES soundtracks(id),
    title           VARCHAR(200) NOT NULL,
    track_number    INT,
    duration_seconds INT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(soundtrack_id, title)
);

CREATE TABLE song_credits (
    id              SERIAL PRIMARY KEY,
    song_id         INT NOT NULL REFERENCES songs(id),
    talent_id       INT NOT NULL REFERENCES talent(id),
    credit_type_id  INT NOT NULL REFERENCES credit_types(id),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(song_id, talent_id, credit_type_id)
);

-- ============================================
-- CHARACTER & TALENT TABLES
-- ============================================

CREATE TABLE characters (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(150) NOT NULL,
    origin_franchise VARCHAR(100) NOT NULL,
    species_id      INT REFERENCES species(id),
    gender_id       INT REFERENCES genders(id),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(name, origin_franchise)
);

CREATE TABLE talent (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(150) NOT NULL UNIQUE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE character_appearances (
    id              SERIAL PRIMARY KEY,
    character_id    INT NOT NULL REFERENCES characters(id),
    media_id        INT NOT NULL REFERENCES media(id),
    talent_id       INT REFERENCES talent(id),
    role_id         INT NOT NULL REFERENCES roles(id),
    variant         VARCHAR(50),
    notes           TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(character_id, media_id, variant)
);

-- ============================================
-- FINANCIAL DATA TABLES
-- ============================================

CREATE TABLE box_office_daily (
    id              SERIAL PRIMARY KEY,
    film_id         INT NOT NULL REFERENCES films(id),
    date            DATE NOT NULL,
    state_code      CHAR(2) NOT NULL,
    revenue         DECIMAL(14, 2) NOT NULL,
    week_num        INT NOT NULL,
    
    UNIQUE(film_id, date, state_code)
);

CREATE TABLE game_sales (
    id              SERIAL PRIMARY KEY,
    game_id         INT NOT NULL REFERENCES games(id),
    platform_id     INT NOT NULL REFERENCES platforms(id),
    date            DATE NOT NULL,
    region          VARCHAR(20) NOT NULL,  -- 'NA', 'EU', 'JP', 'WW'
    units_sold      INT NOT NULL,
    revenue         DECIMAL(14, 2),
    
    UNIQUE(game_id, platform_id, date, region)
);

-- ============================================
-- AWARDS TABLES
-- ============================================

CREATE TABLE nominations (
    id              SERIAL PRIMARY KEY,
    media_id        INT NOT NULL REFERENCES media(id),
    award_category_id INT NOT NULL REFERENCES award_categories(id),
    year            INT NOT NULL,
    outcome         award_outcome NOT NULL DEFAULT 'nominated',
    talent_id       INT REFERENCES talent(id),  -- for individual awards
    song_id         INT REFERENCES songs(id),   -- for song-specific awards
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(media_id, award_category_id, year, talent_id, song_id)
);

-- ============================================
-- INDEXES
-- ============================================

-- Media lookups
CREATE INDEX idx_media_franchise ON media(franchise_id);
CREATE INDEX idx_media_year ON media(year);
CREATE INDEX idx_media_type ON media(media_type);

-- Films lookups
CREATE INDEX idx_films_studio ON films(studio_id);
CREATE INDEX idx_films_media ON films(media_id);

-- Games lookups
CREATE INDEX idx_games_media ON games(media_id);
CREATE INDEX idx_games_developer ON games(developer);
CREATE INDEX idx_games_publisher ON games(publisher);

-- Soundtracks lookups
CREATE INDEX idx_soundtracks_media ON soundtracks(media_id);
CREATE INDEX idx_soundtracks_source ON soundtracks(source_media_id);

-- Songs lookups
CREATE INDEX idx_songs_soundtrack ON songs(soundtrack_id);

-- Song credits lookups
CREATE INDEX idx_song_credits_song ON song_credits(song_id);
CREATE INDEX idx_song_credits_talent ON song_credits(talent_id);
CREATE INDEX idx_song_credits_type ON song_credits(credit_type_id);

-- Characters lookups
CREATE INDEX idx_characters_origin ON characters(origin_franchise);
CREATE INDEX idx_characters_gender ON characters(gender_id);
CREATE INDEX idx_characters_species ON characters(species_id);

-- Character appearances lookups
CREATE INDEX idx_appearances_character ON character_appearances(character_id);
CREATE INDEX idx_appearances_media ON character_appearances(media_id);
CREATE INDEX idx_appearances_talent ON character_appearances(talent_id);
CREATE INDEX idx_appearances_role ON character_appearances(role_id);

-- Box office lookups (large table)
CREATE INDEX idx_box_office_film ON box_office_daily(film_id);
CREATE INDEX idx_box_office_date ON box_office_daily(date);
CREATE INDEX idx_box_office_state ON box_office_daily(state_code);
CREATE INDEX idx_box_office_film_date ON box_office_daily(film_id, date);

-- Game sales lookups (large table)
CREATE INDEX idx_game_sales_game ON game_sales(game_id);
CREATE INDEX idx_game_sales_platform ON game_sales(platform_id);
CREATE INDEX idx_game_sales_date ON game_sales(date);
CREATE INDEX idx_game_sales_region ON game_sales(region);

-- Nominations lookups
CREATE INDEX idx_nominations_media ON nominations(media_id);
CREATE INDEX idx_nominations_category ON nominations(award_category_id);
CREATE INDEX idx_nominations_talent ON nominations(talent_id);
CREATE INDEX idx_nominations_song ON nominations(song_id);
CREATE INDEX idx_nominations_year ON nominations(year);
CREATE INDEX idx_nominations_outcome ON nominations(outcome);

-- ============================================
-- VIEWS
-- ============================================

-- All media with franchise name
CREATE VIEW v_media AS
SELECT 
    m.id,
    m.title,
    m.year,
    m.media_type,
    fr.name AS franchise_name
FROM media m
JOIN franchises fr ON m.franchise_id = fr.id;

-- Films with all details
CREATE VIEW v_films AS
SELECT 
    f.id AS film_id,
    m.id AS media_id,
    m.title,
    m.year,
    s.name AS studio_name,
    fr.name AS franchise_name,
    f.animation_type
FROM films f
JOIN media m ON f.media_id = m.id
JOIN studios s ON f.studio_id = s.id
JOIN franchises fr ON m.franchise_id = fr.id;

-- Games with all details
CREATE VIEW v_games AS
SELECT 
    g.id AS game_id,
    m.id AS media_id,
    m.title,
    m.year,
    fr.name AS franchise_name,
    g.developer,
    g.publisher,
    array_agg(DISTINCT p.name) AS platforms
FROM games g
JOIN media m ON g.media_id = m.id
JOIN franchises fr ON m.franchise_id = fr.id
LEFT JOIN game_platforms gp ON g.id = gp.game_id
LEFT JOIN platforms p ON gp.platform_id = p.id
GROUP BY g.id, m.id, m.title, m.year, fr.name, g.developer, g.publisher;

-- Soundtracks with source media
CREATE VIEW v_soundtracks AS
SELECT 
    st.id AS soundtrack_id,
    m.id AS media_id,
    m.title,
    m.year,
    fr.name AS franchise_name,
    st.label,
    src.title AS source_title,
    src.media_type AS source_type
FROM soundtracks st
JOIN media m ON st.media_id = m.id
JOIN franchises fr ON m.franchise_id = fr.id
LEFT JOIN media src ON st.source_media_id = src.id;

-- Songs with all credits
CREATE VIEW v_songs AS
SELECT 
    s.id AS song_id,
    s.title AS song_title,
    s.track_number,
    s.duration_seconds,
    st.id AS soundtrack_id,
    m.title AS soundtrack_title,
    m.year,
    array_agg(DISTINCT t.name || ' (' || ct.name || ')') AS credits
FROM songs s
JOIN soundtracks st ON s.soundtrack_id = st.id
JOIN media m ON st.media_id = m.id
LEFT JOIN song_credits sc ON s.id = sc.song_id
LEFT JOIN talent t ON sc.talent_id = t.id
LEFT JOIN credit_types ct ON sc.credit_type_id = ct.id
GROUP BY s.id, s.title, s.track_number, s.duration_seconds, st.id, m.title, m.year;

-- Character appearances with all names resolved
CREATE VIEW v_character_appearances AS
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

-- Box office aggregated by film
CREATE VIEW v_box_office_by_film AS
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

-- Game sales aggregated by game
CREATE VIEW v_game_sales_by_game AS
SELECT 
    g.id AS game_id,
    m.title,
    m.year,
    SUM(gs.units_sold) AS total_units,
    SUM(gs.revenue) AS total_revenue
FROM games g
JOIN media m ON g.media_id = m.id
JOIN game_sales gs ON gs.game_id = g.id
GROUP BY g.id, m.title, m.year;

-- Nominations with all names resolved
CREATE VIEW v_nominations AS
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

-- Award wins summary by media
CREATE VIEW v_media_awards AS
SELECT 
    m.id AS media_id,
    m.title,
    m.year,
    m.media_type,
    COUNT(*) FILTER (WHERE n.outcome = 'nominated') AS total_nominations,
    COUNT(*) FILTER (WHERE n.outcome = 'won') AS total_wins
FROM media m
LEFT JOIN nominations n ON n.media_id = m.id
GROUP BY m.id, m.title, m.year, m.media_type;
