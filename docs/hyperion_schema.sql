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
    id                  SERIAL PRIMARY KEY,
    name                VARCHAR(100) NOT NULL UNIQUE,
    parent_species_id   INT REFERENCES species(id),
    is_anthropomorphic  BOOLEAN NOT NULL DEFAULT FALSE,
    notes               TEXT
);

-- Seed canonical species (hierarchical where appropriate)
INSERT INTO species (id, name, parent_species_id, is_anthropomorphic, notes) VALUES
    -- Top-level species
    (1, 'human', NULL, FALSE, 'Includes variants: super, skeleton/spirit, witch, etc.'),
    (2, 'toy', NULL, FALSE, 'Toy Story franchise - use species_subtype for specific toy type'),
    (3, 'monster', NULL, FALSE, 'Monsters Inc and general monsters'),
    (4, 'robot', NULL, FALSE, 'Mechanical beings'),
    (5, 'emotion', NULL, FALSE, 'Inside Out personified emotions'),
    (6, 'fairy', NULL, FALSE, 'Tinker Bell franchise'),
    (7, 'car', NULL, FALSE, 'Cars/Planes franchise vehicles'),
    (8, 'alien', NULL, FALSE, 'Extraterrestrial beings'),
    (9, 'element', NULL, FALSE, 'Elemental beings - use species_subtype for fire/water/earth/air'),
    (10, 'mermaid', NULL, FALSE, 'Little Mermaid and aquatic humanoids'),

    -- Kingdom Hearts specific
    (11, 'heartless', NULL, FALSE, 'Kingdom Hearts enemy type'),
    (12, 'nobody', NULL, FALSE, 'Kingdom Hearts - Organization XIII'),
    (13, 'dream eater', NULL, FALSE, 'Kingdom Hearts 3D'),

    -- Mythical/supernatural
    (14, 'god', NULL, FALSE, 'Deities - Hercules, Moana'),
    (15, 'demigod', NULL, FALSE, 'Hercules, Maui'),
    (16, 'genie', NULL, FALSE, 'Aladdin'),
    (17, 'ghost', NULL, FALSE, 'Spirits and apparitions'),
    (18, 'dragon', NULL, FALSE, 'Mushu, Elliot, etc.'),
    (19, 'enchanted object', NULL, FALSE, 'Beauty and the Beast cursed items'),

    -- Mammals - top level
    (20, 'dog', NULL, FALSE, 'Canines'),
    (21, 'cat', NULL, FALSE, 'Felines'),
    (22, 'mouse', NULL, FALSE, 'Mickey and friends, Ratatouille'),
    (23, 'duck', NULL, FALSE, 'Donald and friends'),
    (24, 'rabbit', NULL, FALSE, 'Thumper, White Rabbit, etc.'),
    (25, 'lion', NULL, FALSE, 'Lion King'),
    (26, 'bear', NULL, FALSE, 'Brother Bear, Winnie the Pooh'),
    (27, 'elephant', NULL, FALSE, 'Dumbo, Jungle Book'),
    (28, 'gorilla', NULL, FALSE, 'Tarzan'),
    (29, 'deer', NULL, FALSE, 'Bambi'),
    (30, 'fox', NULL, FALSE, 'Robin Hood, Zootopia'),
    (31, 'horse', NULL, FALSE, 'Various'),
    (32, 'pig', NULL, FALSE, 'Various'),
    (33, 'wolf', NULL, FALSE, 'Various'),
    (34, 'tiger', NULL, FALSE, 'Jungle Book, Winnie the Pooh'),

    -- Other mammals
    (35, 'hyena', NULL, FALSE, 'Lion King'),
    (36, 'meerkat', NULL, FALSE, 'Timon'),
    (37, 'warthog', NULL, FALSE, 'Pumbaa'),
    (38, 'raccoon', NULL, FALSE, 'Meeko, various'),
    (39, 'skunk', NULL, FALSE, 'Bambi'),
    (40, 'chipmunk', NULL, FALSE, 'Chip and Dale'),
    (41, 'squirrel', NULL, FALSE, 'Various'),
    (42, 'rat', NULL, FALSE, 'Ratatouille'),
    (43, 'monkey', NULL, FALSE, 'Aladdin, Tarzan'),
    (44, 'lemur', NULL, FALSE, 'Various'),
    (45, 'sloth', NULL, FALSE, 'Ice Age'),
    (46, 'mammoth', NULL, FALSE, 'Ice Age'),
    (47, 'saber-toothed cat', NULL, FALSE, 'Ice Age'),

    -- Birds
    (50, 'owl', NULL, FALSE, 'Winnie the Pooh, various'),
    (51, 'parrot', NULL, FALSE, 'Aladdin, various'),
    (52, 'macaw', NULL, FALSE, 'Rio'),
    (53, 'crow', NULL, FALSE, 'Dumbo, various'),
    (54, 'seagull', NULL, FALSE, 'Little Mermaid, Finding Nemo'),
    (55, 'hornbill', NULL, FALSE, 'Zazu'),
    (56, 'penguin', NULL, FALSE, 'Mary Poppins, various'),
    (57, 'chicken', NULL, FALSE, 'Various'),
    (58, 'vulture', NULL, FALSE, 'Jungle Book, various'),

    -- Aquatic
    (60, 'fish', NULL, FALSE, 'Finding Nemo/Dory'),
    (61, 'clownfish', 60, FALSE, 'Nemo, Marlin'),
    (62, 'blue tang', 60, FALSE, 'Dory'),
    (63, 'shark', NULL, FALSE, 'Finding Nemo'),
    (64, 'whale', NULL, FALSE, 'Various'),
    (65, 'octopus', NULL, FALSE, 'Various'),
    (66, 'crab', NULL, FALSE, 'Sebastian, Tamatoa'),
    (67, 'sea turtle', NULL, FALSE, 'Finding Nemo'),
    (68, 'sea monster', NULL, FALSE, 'Luca'),

    -- Insects/arthropods
    (70, 'ant', NULL, FALSE, 'A Bug''s Life'),
    (71, 'cricket', NULL, FALSE, 'Mulan, Pinocchio'),
    (72, 'grasshopper', NULL, FALSE, 'A Bug''s Life'),
    (73, 'ladybug', NULL, FALSE, 'A Bug''s Life'),
    (74, 'caterpillar', NULL, FALSE, 'A Bug''s Life, Alice'),
    (75, 'firefly', NULL, FALSE, 'Princess and the Frog'),
    (76, 'spider', NULL, FALSE, 'Various'),

    -- Reptiles/amphibians
    (80, 'dinosaur', NULL, FALSE, 'Good Dinosaur, various'),
    (81, 'frog', NULL, FALSE, 'Princess and the Frog'),
    (82, 'snake', NULL, FALSE, 'Jungle Book, various'),
    (83, 'crocodile', NULL, FALSE, 'Peter Pan, various'),
    (84, 'chameleon', NULL, FALSE, 'Tangled'),

    -- Fantasy humanoids
    (90, 'dwarf', NULL, FALSE, 'Snow White'),
    (91, 'elf', NULL, FALSE, 'Various'),
    (92, 'troll', NULL, FALSE, 'Frozen'),
    (93, 'gargoyle', NULL, FALSE, 'Hunchback'),
    (94, 'satyr', NULL, FALSE, 'Hercules'),
    (95, 'centaur', NULL, FALSE, 'Fantasia, Hercules'),

    -- Miscellaneous
    (96, 'soul', NULL, FALSE, 'Soul, Coco spirits'),
    (97, 'mind worker', NULL, FALSE, 'Inside Out'),
    (98, 'who', NULL, FALSE, 'Horton Hears a Who'),
    (99, 'leafman', NULL, FALSE, 'Epic');

-- Reset sequence after explicit IDs
SELECT setval('species_id_seq', (SELECT MAX(id) FROM species));

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
    id                  SERIAL PRIMARY KEY,
    name                VARCHAR(150) NOT NULL,
    origin_franchise    VARCHAR(100) NOT NULL,
    species_id          INT REFERENCES species(id),
    species_subtype     VARCHAR(100),  -- e.g., 'cowboy doll' for toy, 'fire' for element
    is_anthropomorphic  BOOLEAN NOT NULL DEFAULT FALSE,  -- override for this character
    gender_id           INT REFERENCES genders(id),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

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
-- ETL MAPPING TABLES
-- ============================================

-- Maps raw species strings from source JSON to canonical species
-- Used by Kafka consumer during character ingestion
CREATE TABLE etl_species_mapping (
    id                  SERIAL PRIMARY KEY,
    raw_value           VARCHAR(150) NOT NULL UNIQUE,  -- exact string from source JSON
    species_id          INT NOT NULL REFERENCES species(id),
    species_subtype     VARCHAR(100),                  -- extracted subtype if applicable
    is_anthropomorphic  BOOLEAN NOT NULL DEFAULT FALSE,
    notes               TEXT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Seed ETL species mappings based on actual source data analysis
-- Human variants
INSERT INTO etl_species_mapping (raw_value, species_id, species_subtype, is_anthropomorphic) VALUES
    ('human', 1, NULL, FALSE),
    ('Human', 1, NULL, FALSE),
    ('human (super)', 1, 'super', FALSE),
    ('human (skeleton/spirit)', 1, 'skeleton/spirit', FALSE),
    ('human (witch)', 1, 'witch', FALSE),
    ('human (sorcerer)', 1, 'sorcerer', FALSE),
    ('human (ghost)', 1, 'ghost', FALSE),
    ('human (toon)', 1, 'toon', FALSE),
    ('human (red panda curse)', 1, 'red panda curse', FALSE),
    ('human (transformed to bear)', 1, 'transformed to bear', FALSE),
    ('human (transformed to bears)', 1, 'transformed to bears', FALSE),
    ('human (caveboy)', 1, 'caveboy', FALSE),
    ('human (ageless)', 1, 'ageless', FALSE),
    ('human (CGI)', 1, 'CGI', FALSE),
    ('human (ancestral spirit)', 1, 'ancestral spirit', FALSE),
    ('human (former mermaid)', 1, 'former mermaid', FALSE),
    ('human_video_game', 1, 'video game', FALSE),
    ('human/spirit', 1, 'spirit', FALSE),
    ('human/mermaid', 1, 'mermaid hybrid', FALSE),
    ('human/genie', 1, 'genie hybrid', FALSE),
    ('human/program', 1, 'program hybrid', FALSE),
    ('human / soul', 1, 'soul', FALSE),
    ('Human (cyborg)', 1, 'cyborg', FALSE),
    ('Human / Pigeon (transformed)', 1, 'pigeon transformed', FALSE),
    ('cursed human', 1, 'cursed', FALSE),
    ('atlantean', 1, 'atlantean', FALSE);

-- Toy variants (Toy Story)
INSERT INTO etl_species_mapping (raw_value, species_id, species_subtype, is_anthropomorphic) VALUES
    ('toy', 2, NULL, FALSE),
    ('toy (cowboy doll)', 2, 'cowboy doll', FALSE),
    ('toy (action figure)', 2, 'action figure', FALSE),
    ('toy (cowgirl doll)', 2, 'cowgirl doll', FALSE),
    ('toy (horse)', 2, 'horse', FALSE),
    ('toy (dinosaur)', 2, 'dinosaur', FALSE),
    ('toy (piggy bank)', 2, 'piggy bank', FALSE),
    ('toy (dog)', 2, 'dog', FALSE),
    ('toy (potato)', 2, 'potato', FALSE),
    ('toy (squeeze toy)', 2, 'squeeze toy', FALSE),
    ('toy (porcelain lamp)', 2, 'porcelain lamp', FALSE),
    ('toy (army man)', 2, 'army man', FALSE),
    ('toy (fashion doll)', 2, 'fashion doll', FALSE),
    ('toy (remote control car)', 2, 'remote control car', FALSE),
    ('toy (binoculars)', 2, 'binoculars', FALSE),
    ('toy (Etch A Sketch)', 2, 'Etch A Sketch', FALSE),
    ('toy (electronic toy)', 2, 'electronic toy', FALSE),
    ('toy (wrestler figure)', 2, 'wrestler figure', FALSE),
    ('toy (snake)', 2, 'snake', FALSE),
    ('toy (robot)', 2, 'robot', FALSE),
    ('toy (mutant)', 2, 'mutant', FALSE),
    ('toy (prospector doll)', 2, 'prospector doll', FALSE),
    ('toy (squeaky penguin)', 2, 'squeaky penguin', FALSE),
    ('toy (teddy bear)', 2, 'teddy bear', FALSE),
    ('toy (baby doll)', 2, 'baby doll', FALSE),
    ('toy (octopus)', 2, 'octopus', FALSE),
    ('toy (rock monster)', 2, 'rock monster', FALSE),
    ('toy (insect warrior)', 2, 'insect warrior', FALSE),
    ('toy (glow worm)', 2, 'glow worm', FALSE),
    ('toy (telephone)', 2, 'telephone', FALSE),
    ('toy (rag doll)', 2, 'rag doll', FALSE),
    ('toy (unicorn)', 2, 'unicorn', FALSE),
    ('toy (hedgehog)', 2, 'hedgehog', FALSE),
    ('toy (clown)', 2, 'clown', FALSE),
    ('toy (plush peas)', 2, 'plush peas', FALSE),
    ('toy (spork craft)', 2, 'spork craft', FALSE),
    ('toy (vintage doll)', 2, 'vintage doll', FALSE),
    ('toy (stuntman figure)', 2, 'stuntman figure', FALSE),
    ('toy (carnival plush)', 2, 'carnival plush', FALSE),
    ('toy (miniature figure)', 2, 'miniature figure', FALSE),
    ('toy (ventriloquist dummies)', 2, 'ventriloquist dummies', FALSE),
    ('toy (carnival toy)', 2, 'carnival toy', FALSE);

-- Monster variants
INSERT INTO etl_species_mapping (raw_value, species_id, species_subtype, is_anthropomorphic) VALUES
    ('monster', 3, NULL, FALSE),
    ('monster (yeti)', 3, 'yeti', FALSE),
    ('monster (slug-like)', 3, 'slug-like', FALSE),
    ('monster (octopus-like)', 3, 'octopus-like', FALSE),
    ('monster (crab-like)', 3, 'crab-like', FALSE),
    ('monster (gorgon-like)', 3, 'gorgon-like', FALSE),
    ('monster (two-headed)', 3, 'two-headed', FALSE),
    ('monster (chameleon-like)', 3, 'chameleon-like', FALSE),
    ('monster (centipede-dragon)', 3, 'centipede-dragon', FALSE);

-- Robot variants
INSERT INTO etl_species_mapping (raw_value, species_id, species_subtype, is_anthropomorphic) VALUES
    ('robot', 4, NULL, FALSE),
    ('Robot', 4, NULL, FALSE),
    ('robot (AI)', 4, 'AI', FALSE),
    ('robot (cat)', 4, 'cat', FALSE),
    ('Robot (mechanical bull)', 4, 'mechanical bull', FALSE),
    ('ai_program', 4, 'AI program', FALSE),
    ('program', 4, 'program', FALSE),
    ('data construct', 4, 'data construct', FALSE);

-- Emotion (Inside Out)
INSERT INTO etl_species_mapping (raw_value, species_id, species_subtype, is_anthropomorphic) VALUES
    ('emotion', 5, NULL, FALSE),
    ('mind worker', 97, NULL, FALSE);

-- Fairy variants
INSERT INTO etl_species_mapping (raw_value, species_id, species_subtype, is_anthropomorphic) VALUES
    ('fairy', 6, NULL, FALSE),
    ('frost fairy', 6, 'frost', FALSE),
    ('scout fairy', 6, 'scout', FALSE),
    ('animal fairy', 6, 'animal', FALSE);

-- Car/Vehicle variants (Cars, Planes)
INSERT INTO etl_species_mapping (raw_value, species_id, species_subtype, is_anthropomorphic) VALUES
    ('car', 7, NULL, FALSE),
    ('car (race car)', 7, 'race car', FALSE),
    ('car (tow truck)', 7, 'tow truck', FALSE),
    ('car (Porsche)', 7, 'Porsche', FALSE),
    ('car (Hudson Hornet)', 7, 'Hudson Hornet', FALSE),
    ('car (Fiat)', 7, 'Fiat', FALSE),
    ('car (pickup truck)', 7, 'pickup truck', FALSE),
    ('car (fire truck)', 7, 'fire truck', FALSE),
    ('car (police car)', 7, 'police car', FALSE),
    ('car (semi truck)', 7, 'semi truck', FALSE),
    ('car (VW bus)', 7, 'VW bus', FALSE),
    ('car (Corvette)', 7, 'Corvette', FALSE),
    ('car (Impala)', 7, 'Impala', FALSE),
    ('car (Formula 1)', 7, 'Formula 1', FALSE),
    ('car (Aston Martin)', 7, 'Aston Martin', FALSE),
    ('car (Model T)', 7, 'Model T', FALSE),
    ('car (Range Rover)', 7, 'Range Rover', FALSE),
    ('car (Gremlin)', 7, 'Gremlin', FALSE),
    ('car (Pacer)', 7, 'Pacer', FALSE),
    ('car (Zündapp)', 7, 'Zündapp', FALSE),
    ('car (next-gen race car)', 7, 'next-gen race car', FALSE),
    ('car (military jeep)', 7, 'military jeep', FALSE),
    ('car (forklift)', 7, 'forklift', FALSE),
    ('car (show car)', 7, 'show car', FALSE),
    ('car (three-wheeler)', 7, 'three-wheeler', FALSE),
    ('forklift', 7, 'forklift', FALSE),
    ('plane', 7, 'plane', FALSE),
    ('plane (racing)', 7, 'racing plane', FALSE),
    ('plane (crop duster)', 7, 'crop duster', FALSE),
    ('plane (firefighter)', 7, 'firefighter plane', FALSE),
    ('plane (crop duster/firefighter)', 7, 'crop duster/firefighter', FALSE),
    ('plane (biplane)', 7, 'biplane', FALSE),
    ('plane (Corsair)', 7, 'Corsair', FALSE),
    ('helicopter', 7, 'helicopter', FALSE),
    ('helicopter (firefighter)', 7, 'firefighter helicopter', FALSE),
    ('fire engine', 7, 'fire engine', FALSE),
    ('fuel truck', 7, 'fuel truck', FALSE),
    ('tug', 7, 'tug', FALSE),
    ('jet', 7, 'jet', FALSE),
    ('cargo plane', 7, 'cargo plane', FALSE),
    ('train', 7, 'train', FALSE),
    ('various vehicles', 7, 'various', FALSE);

-- Element variants (Elemental)
INSERT INTO etl_species_mapping (raw_value, species_id, species_subtype, is_anthropomorphic) VALUES
    ('fire element', 9, 'fire', FALSE),
    ('water element', 9, 'water', FALSE),
    ('earth element', 9, 'earth', FALSE),
    ('air element', 9, 'air', FALSE);

-- Kingdom Hearts species
INSERT INTO etl_species_mapping (raw_value, species_id, species_subtype, is_anthropomorphic) VALUES
    ('heartless', 11, NULL, FALSE),
    ('nobody', 12, NULL, FALSE),
    ('dream eater', 13, 'dream eater', FALSE),
    ('replica', 1, 'replica', FALSE),
    ('being of darkness', 11, 'darkness', FALSE);

-- Anthropomorphic animals
INSERT INTO etl_species_mapping (raw_value, species_id, species_subtype, is_anthropomorphic) VALUES
    ('dog (anthropomorphic)', 20, NULL, TRUE),
    ('anthropomorphic dog', 20, NULL, TRUE),
    ('cat (anthropomorphic)', 21, NULL, TRUE),
    ('anthropomorphic cat', 21, NULL, TRUE),
    ('anthropomorphic mouse', 22, NULL, TRUE),
    ('anthropomorphic duck', 23, NULL, TRUE);

-- Direct animal mappings
INSERT INTO etl_species_mapping (raw_value, species_id, species_subtype, is_anthropomorphic) VALUES
    ('dog', 20, NULL, FALSE),
    ('cat', 21, NULL, FALSE),
    ('mouse', 22, NULL, FALSE),
    ('Mouse', 22, NULL, FALSE),
    ('duck', 23, NULL, FALSE),
    ('rabbit', 24, NULL, FALSE),
    ('Rabbit', 24, NULL, FALSE),
    ('lion', 25, NULL, FALSE),
    ('lion (spirit)', 25, 'spirit', FALSE),
    ('bear', 26, NULL, FALSE),
    ('Bear', 26, NULL, FALSE),
    ('bear (stuffed)', 26, 'stuffed', FALSE),
    ('stuffed bear', 26, 'stuffed', FALSE),
    ('bear (cursed human)', 26, 'cursed human', FALSE),
    ('bear (human-turned)', 26, 'human-turned', FALSE),
    ('elephant', 27, NULL, FALSE),
    ('Elephant', 27, NULL, FALSE),
    ('gorilla', 28, NULL, FALSE),
    ('Gorilla', 28, NULL, FALSE),
    ('gorilla (hermit)', 28, 'hermit', FALSE),
    ('deer', 29, NULL, FALSE),
    ('fox', 30, NULL, FALSE),
    ('horse', 31, NULL, FALSE),
    ('pig', 32, NULL, FALSE),
    ('pig (stuffed)', 32, 'stuffed', FALSE),
    ('stuffed pig', 32, 'stuffed', FALSE),
    ('wolf', 33, NULL, FALSE),
    ('tiger', 34, NULL, FALSE),
    ('tiger (stuffed)', 34, 'stuffed', FALSE),
    ('stuffed tiger', 34, 'stuffed', FALSE),
    ('hyena', 35, NULL, FALSE),
    ('meerkat', 36, NULL, FALSE),
    ('warthog', 37, NULL, FALSE),
    ('raccoon', 38, NULL, FALSE),
    ('skunk', 39, NULL, FALSE),
    ('chipmunk', 40, NULL, FALSE),
    ('chipmunk (CGI)', 40, 'CGI', FALSE),
    ('chipmunk (2D)', 40, '2D', FALSE),
    ('squirrel', 41, NULL, FALSE),
    ('rat', 42, NULL, FALSE),
    ('monkey', 43, NULL, FALSE),
    ('lemur', 44, NULL, FALSE),
    ('sloth', 45, NULL, FALSE),
    ('kangaroo', 5, NULL, FALSE),
    ('kangaroo (stuffed)', 24, 'stuffed', FALSE),
    ('donkey', 31, NULL, FALSE),
    ('donkey (stuffed)', 31, 'stuffed', FALSE);

-- Birds
INSERT INTO etl_species_mapping (raw_value, species_id, species_subtype, is_anthropomorphic) VALUES
    ('owl', 50, NULL, FALSE),
    ('parrot', 51, NULL, FALSE),
    ('Spix''s macaw', 52, 'Spix''s', FALSE),
    ('crow', 53, NULL, FALSE),
    ('seagull', 54, NULL, FALSE),
    ('hornbill', 55, NULL, FALSE),
    ('penguin', 56, NULL, FALSE),
    ('chicken', 57, NULL, FALSE),
    ('vulture', 58, NULL, FALSE),
    ('Vulture', 58, NULL, FALSE),
    ('bird', 51, NULL, FALSE),
    ('Bird', 51, NULL, FALSE),
    ('albatross', 51, 'albatross', FALSE),
    ('toucan', 51, 'toucan', FALSE),
    ('Toco toucan', 51, 'Toco toucan', FALSE),
    ('Keel-billed toucan', 51, 'Keel-billed', FALSE),
    ('rooster', 57, 'rooster', FALSE),
    ('goose', 51, 'goose', FALSE),
    ('Goose', 51, 'goose', FALSE),
    ('hummingbird', 51, 'hummingbird', FALSE),
    ('Pigeon', 51, 'pigeon', FALSE),
    ('stork', 51, 'stork', FALSE),
    ('eagle', 51, 'eagle', FALSE),
    ('raven', 51, 'raven', FALSE),
    ('loon', 51, 'loon', FALSE);

-- Aquatic
INSERT INTO etl_species_mapping (raw_value, species_id, species_subtype, is_anthropomorphic) VALUES
    ('fish', 60, NULL, FALSE),
    ('clownfish', 61, NULL, FALSE),
    ('blue tang', 62, NULL, FALSE),
    ('shark', 63, NULL, FALSE),
    ('great white shark', 63, 'great white', FALSE),
    ('hammerhead shark', 63, 'hammerhead', FALSE),
    ('mako shark', 63, 'mako', FALSE),
    ('shark (shrunken)', 63, 'shrunken', FALSE),
    ('whale', 64, NULL, FALSE),
    ('Whale', 64, NULL, FALSE),
    ('blue whale', 64, 'blue', FALSE),
    ('beluga whale', 64, 'beluga', FALSE),
    ('whale shark', 63, 'whale shark', FALSE),
    ('octopus', 65, NULL, FALSE),
    ('octopus (septopus)', 65, 'septopus', FALSE),
    ('crab', 66, NULL, FALSE),
    ('sea turtle', 67, NULL, FALSE),
    ('sea monster', 68, NULL, FALSE),
    ('sea monster (deep sea)', 68, 'deep sea', FALSE),
    ('mermaid', 10, NULL, FALSE),
    ('merman', 10, 'merman', FALSE),
    ('sea witch', 10, 'sea witch', FALSE),
    ('cecaelia', 10, 'cecaelia', FALSE),
    ('sea lion', 60, 'sea lion', FALSE),
    ('sea otter', 60, 'sea otter', FALSE),
    ('walrus', 60, 'walrus', FALSE),
    ('pelican', 54, 'pelican', FALSE),
    ('starfish', 60, 'starfish', FALSE),
    ('jellyfish', 60, 'jellyfish', FALSE);

-- Insects
INSERT INTO etl_species_mapping (raw_value, species_id, species_subtype, is_anthropomorphic) VALUES
    ('ant', 70, NULL, FALSE),
    ('cricket', 71, NULL, FALSE),
    ('grasshopper', 72, NULL, FALSE),
    ('ladybug', 73, NULL, FALSE),
    ('caterpillar', 74, NULL, FALSE),
    ('firefly', 75, NULL, FALSE),
    ('spider', 76, NULL, FALSE),
    ('black widow spider', 76, 'black widow', FALSE),
    ('centipede', 76, 'centipede', FALSE),
    ('praying mantis', 70, 'praying mantis', FALSE),
    ('stick insect', 70, 'stick insect', FALSE),
    ('rhinoceros beetle', 70, 'rhinoceros beetle', FALSE),
    ('moth', 75, 'moth', FALSE),
    ('fly', 70, 'fly', FALSE),
    ('flea', 70, 'flea', FALSE),
    ('pill bug', 70, 'pill bug', FALSE),
    ('pill_bug', 70, 'pill bug', FALSE);

-- Dinosaurs
INSERT INTO etl_species_mapping (raw_value, species_id, species_subtype, is_anthropomorphic) VALUES
    ('dinosaur', 80, NULL, FALSE),
    ('dinosaur (T-Rex)', 80, 'T-Rex', FALSE),
    ('dinosaur (Apatosaurus)', 80, 'Apatosaurus', FALSE),
    ('dinosaur (Pterodactyl)', 80, 'Pterodactyl', FALSE),
    ('dinosaur (Styracosaurus)', 80, 'Styracosaurus', FALSE),
    ('Tyrannosaurus Rex', 80, 'T-Rex', FALSE),
    ('iguanodon', 80, 'iguanodon', FALSE),
    ('carnotaurus', 80, 'carnotaurus', FALSE),
    ('styracosaurus', 80, 'styracosaurus', FALSE),
    ('ankylosaur', 80, 'ankylosaur', FALSE),
    ('brachiosaurus', 80, 'brachiosaurus', FALSE),
    ('Dromaeosaurus', 80, 'Dromaeosaurus', FALSE),
    ('Baryonyx (albino)', 80, 'Baryonyx albino', FALSE);

-- Other reptiles/amphibians
INSERT INTO etl_species_mapping (raw_value, species_id, species_subtype, is_anthropomorphic) VALUES
    ('frog', 81, NULL, FALSE),
    ('Poison dart frog', 81, 'poison dart', FALSE),
    ('Toad', 81, 'toad', FALSE),
    ('snake', 82, NULL, FALSE),
    ('python', 82, 'python', FALSE),
    ('crocodile', 83, NULL, FALSE),
    ('alligator', 83, 'alligator', FALSE),
    ('chameleon', 84, NULL, FALSE),
    ('salamander', 81, 'salamander', FALSE),
    ('goanna', 84, 'goanna', FALSE);

-- Mythical/supernatural
INSERT INTO etl_species_mapping (raw_value, species_id, species_subtype, is_anthropomorphic) VALUES
    ('god', 14, NULL, FALSE),
    ('goddess', 14, 'goddess', FALSE),
    ('demigod', 15, NULL, FALSE),
    ('genie', 16, NULL, FALSE),
    ('ghost', 17, NULL, FALSE),
    ('dragon', 18, NULL, FALSE),
    ('dragon (pet)', 18, 'pet', FALSE),
    ('dragon (curse manifestation)', 18, 'curse', FALSE),
    ('enchanted_object', 19, NULL, FALSE),
    ('enchanted object', 19, NULL, FALSE),
    ('dwarf', 90, NULL, FALSE),
    ('elf', 91, NULL, FALSE),
    ('elf (partial)', 91, 'partial', FALSE),
    ('troll', 92, NULL, FALSE),
    ('gargoyle', 93, NULL, FALSE),
    ('satyr', 94, NULL, FALSE),
    ('centaur', 95, NULL, FALSE),
    ('pegasus', 31, 'pegasus', FALSE),
    ('muse', 14, 'muse', FALSE),
    ('snowman', 17, 'snowman', FALSE),
    ('demon', 17, 'demon', FALSE),
    ('goblin', 17, 'goblin', FALSE),
    ('witch', 1, 'witch', FALSE),
    ('cyclops', 3, 'cyclops', FALSE),
    ('giant', 1, 'giant', FALSE),
    ('manticore', 3, 'manticore', FALSE);

-- Ice Age specific
INSERT INTO etl_species_mapping (raw_value, species_id, species_subtype, is_anthropomorphic) VALUES
    ('Woolly mammoth', 46, NULL, FALSE),
    ('Ground sloth', 45, NULL, FALSE),
    ('Saber-toothed tiger', 47, NULL, FALSE),
    ('Saber-toothed squirrel', 41, 'saber-toothed', FALSE),
    ('Saber-toothed flying squirrel', 41, 'saber-toothed flying', FALSE),
    ('Possum', 38, 'possum', FALSE),
    ('possum', 38, 'possum', FALSE),
    ('Molehog', 26, 'molehog', FALSE),
    ('weasel', 33, 'weasel', FALSE),
    ('Weasel', 33, 'weasel', FALSE),
    ('Weasel (imaginary)', 33, 'weasel imaginary', FALSE);

-- Alien variants
INSERT INTO etl_species_mapping (raw_value, species_id, species_subtype, is_anthropomorphic) VALUES
    ('alien', 8, NULL, FALSE),
    ('alien experiment', 8, 'experiment', FALSE),
    ('alien experiments', 8, 'experiments', FALSE);

-- Soul/spirit (Coco, Soul)
INSERT INTO etl_species_mapping (raw_value, species_id, species_subtype, is_anthropomorphic) VALUES
    ('soul', 96, NULL, FALSE),
    ('alebrije (spirit guide)', 96, 'alebrije', FALSE),
    ('spirit', 96, 'spirit', FALSE);

-- Miscellaneous
INSERT INTO etl_species_mapping (raw_value, species_id, species_subtype, is_anthropomorphic) VALUES
    ('Who', 98, NULL, FALSE),
    ('Leafman', 99, NULL, FALSE),
    ('Boggan', 99, 'Boggan', FALSE),
    ('unknown', 1, NULL, FALSE),
    ('various', 1, NULL, FALSE),
    ('n/a', 1, NULL, FALSE),
    ('N/A', 1, NULL, FALSE);

CREATE INDEX idx_etl_species_raw ON etl_species_mapping(raw_value);

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
    c.species_subtype,
    c.is_anthropomorphic,
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
