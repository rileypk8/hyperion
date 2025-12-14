#!/usr/bin/env node
/**
 * Consolidate all character JSON data into SQLite database
 * Usage: node consolidate-to-sqlite.js [output.db]
 */

const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');

const DATA_DIR = path.join(__dirname, '..', 'data');
const SCHEMA_FILE = path.join(__dirname, '..', 'docs', 'hyperion_schema_sqlite.sql');
const DEFAULT_DB_PATH = path.join(__dirname, '..', 'data', 'hyperion.db');

// Studio mappings based on folder structure
const STUDIO_MAP = {
  'wdas_characters': { id: 'wdas', name: 'Walt Disney Animation Studios', shortName: 'WDAS' },
  'pixar_characters': { id: 'pixar', name: 'Pixar Animation Studios', shortName: 'Pixar' },
  'blue_sky_characters': { id: 'blue-sky', name: 'Blue Sky Studios', shortName: 'Blue Sky' },
  'disneytoon_characters': { id: 'disneytoon', name: 'DisneyToon Studios', shortName: 'DisneyToon' },
  'kingdom_hearts_characters': { id: 'kingdom-hearts', name: 'Kingdom Hearts (Square Enix)', shortName: 'KH' },
  'marvel_animation_characters': { id: 'marvel', name: 'Marvel Animation', shortName: 'Marvel' },
  'disney_interactive_characters': { id: 'interactive', name: 'Disney Interactive', shortName: 'Interactive' },
  '20th_century_animation': { id: '20th-century', name: '20th Century Animation', shortName: '20th Century' },
  'fox_disney_plus_characters': { id: 'fox-disney-plus', name: 'Fox/Disney+ Animation', shortName: 'Fox D+' },
};

// Normalize role names
function normalizeRole(role) {
  if (!role) return 'minor';
  const r = role.toLowerCase().replace(/\s+/g, '_');
  const validRoles = ['protagonist', 'deuteragonist', 'hero', 'villain', 'antagonist',
                      'henchman', 'sidekick', 'mentor', 'love_interest', 'comic_relief',
                      'ally', 'supporting', 'minor'];
  if (r === 'comic relief') return 'comic_relief';
  if (r === 'love interest') return 'love_interest';
  if (r.includes('hench')) return 'henchman';
  if (validRoles.includes(r)) return r;
  return 'supporting';
}

// Normalize gender
function normalizeGender(gender) {
  if (!gender) return 'unknown';
  const g = gender.toLowerCase();
  if (g === 'male') return 'male';
  if (g === 'female') return 'female';
  if (g === 'n/a' || g === 'none' || g === 'various') return 'n/a';
  if (g === 'mixed') return 'mixed';
  return 'unknown';
}

// Normalize species
function normalizeSpecies(species) {
  if (!species) return 'unknown';
  const s = species.toLowerCase();
  if (s.includes('human')) return 'human';
  if (s.includes('toy')) return 'toy';
  if (s.includes('animal') || s.includes('dog') || s.includes('cat') ||
      s.includes('lion') || s.includes('fish') || s.includes('bird')) return 'animal';
  if (s.includes('monster')) return 'monster';
  if (s.includes('robot') || s.includes('machine')) return 'robot';
  if (s.includes('fairy') || s.includes('magic')) return 'fairy';
  if (s.includes('heartless')) return 'heartless';
  if (s.includes('nobody')) return 'nobody';
  if (s.includes('car')) return 'car';
  if (s.includes('emotion')) return 'emotion';
  if (s.includes('insect')) return 'insect';
  if (s.includes('ghost')) return 'ghost';
  if (s.includes('alien')) return 'alien';
  if (s.includes('dream eater')) return 'dream eater';
  return 'unknown';
}

// Generate slug ID from name
function slugify(str) {
  return str.toLowerCase()
    .replace(/[^a-z0-9\s-]/g, '')
    .replace(/\s+/g, '-')
    .replace(/-+/g, '-')
    .substring(0, 50);
}

// Extract year from film name like "Frozen (2013)"
function extractYear(filmName) {
  const match = filmName.match(/\((\d{4})\)/);
  return match ? parseInt(match[1]) : null;
}

// Extract characters from various JSON structures
function extractCharacters(data, studioId, franchiseName) {
  const characters = [];

  // Direct characters array (Kingdom Hearts style)
  if (data.characters && Array.isArray(data.characters)) {
    for (const char of data.characters) {
      characters.push({
        ...char,
        franchiseName: franchiseName || data.franchise || data.game || 'Unknown',
        studioId,
        mediaTitle: data.game || data.film || data.franchise,
        year: data.year
      });
    }
  }

  // Nested categories structure (WDAS/Pixar style)
  if (data.categories) {
    for (const [catKey, category] of Object.entries(data.categories)) {
      if (category.characters && Array.isArray(category.characters)) {
        for (const char of category.characters) {
          characters.push({
            ...char,
            franchiseName: franchiseName || data.franchise || 'Unknown',
            studioId,
            mediaTitle: catKey.replace(/_/g, ' '),
            year: category.year || data.year
          });
        }
      }
    }
  }

  return characters;
}

// Main consolidation and loading
function consolidateAndLoad(dbPath) {
  console.log('=== Hyperion SQLite ETL ===\n');

  // Initialize database
  console.log(`Creating database: ${dbPath}`);

  // Remove existing database if it exists
  if (fs.existsSync(dbPath)) {
    fs.unlinkSync(dbPath);
    console.log('Removed existing database');
  }

  const db = new Database(dbPath);

  // Enable foreign keys
  db.pragma('foreign_keys = ON');

  // Load and execute schema
  console.log('Loading schema...');
  const schema = fs.readFileSync(SCHEMA_FILE, 'utf-8');
  db.exec(schema);
  console.log('Schema created successfully\n');

  // Prepare lookup caches
  const genderCache = {};
  const speciesCache = {};
  const roleCache = {};
  const studioCache = {};
  const franchiseCache = {};
  const mediaCache = {};

  // Load existing lookups into caches
  for (const row of db.prepare('SELECT id, name FROM genders').all()) {
    genderCache[row.name] = row.id;
  }
  for (const row of db.prepare('SELECT id, name FROM species').all()) {
    speciesCache[row.name] = row.id;
  }
  for (const row of db.prepare('SELECT id, name FROM roles').all()) {
    roleCache[row.name] = row.id;
  }

  // Prepare insert statements
  const insertStudio = db.prepare(`
    INSERT OR IGNORE INTO studios (slug, name, short_name, status)
    VALUES (@slug, @name, @shortName, 'active')
  `);

  const insertFranchise = db.prepare(`
    INSERT OR IGNORE INTO franchises (slug, name, studio_id)
    VALUES (@slug, @name, @studioId)
  `);

  const insertMedia = db.prepare(`
    INSERT OR IGNORE INTO media (slug, title, year, franchise_id, media_type)
    VALUES (@slug, @title, @year, @franchiseId, @mediaType)
  `);

  const insertFilm = db.prepare(`
    INSERT OR IGNORE INTO films (media_id, studio_id, animation_type, character_count)
    VALUES (@mediaId, @studioId, @animationType, @characterCount)
  `);

  const insertCharacter = db.prepare(`
    INSERT OR IGNORE INTO characters (slug, name, origin_franchise, species_id, gender_id, voice_actor, notes, franchise_id, studio_id)
    VALUES (@slug, @name, @originFranchise, @speciesId, @genderId, @voiceActor, @notes, @franchiseId, @studioId)
  `);

  const insertSpecies = db.prepare(`
    INSERT OR IGNORE INTO species (name) VALUES (@name)
  `);

  const getSpeciesId = db.prepare(`SELECT id FROM species WHERE name = ?`);
  const getStudioId = db.prepare(`SELECT id FROM studios WHERE slug = ?`);
  const getFranchiseId = db.prepare(`SELECT id FROM franchises WHERE slug = ?`);
  const getMediaId = db.prepare(`SELECT id FROM media WHERE slug = ?`);

  // Collect all data first
  const allCharacters = [];
  const allFilms = new Map();
  const franchiseSet = new Set();
  const studioStats = {};

  // Initialize studio stats
  for (const studio of Object.values(STUDIO_MAP)) {
    studioStats[studio.id] = { filmCount: 0, characterCount: 0, franchises: new Set() };
  }

  console.log('Processing data files...');

  // Process each data folder
  for (const [folder, studioInfo] of Object.entries(STUDIO_MAP)) {
    const folderPath = path.join(DATA_DIR, folder);
    if (!fs.existsSync(folderPath)) continue;

    const files = fs.readdirSync(folderPath).filter(f => f.endsWith('.json'));

    for (const file of files) {
      try {
        const content = fs.readFileSync(path.join(folderPath, file), 'utf-8');
        const data = JSON.parse(content);

        const franchise = data.franchise || data.game || file.replace('_characters.json', '').replace(/_/g, ' ');
        franchiseSet.add({ name: franchise, studioId: studioInfo.id });
        studioStats[studioInfo.id].franchises.add(franchise);

        // Extract films
        if (data.films) {
          for (const filmName of data.films) {
            const year = extractYear(filmName);
            const title = filmName.replace(/\s*\(\d{4}\)/, '');
            const filmId = slugify(title) + (year ? `-${year}` : '');
            if (!allFilms.has(filmId)) {
              allFilms.set(filmId, {
                id: filmId,
                title,
                year: year || 2000,
                franchiseSlug: slugify(franchise),
                studioSlug: studioInfo.id,
                characterCount: 0
              });
              studioStats[studioInfo.id].filmCount++;
            }
          }
        } else if (data.game) {
          const filmId = slugify(data.game) + (data.year ? `-${data.year}` : '');
          if (!allFilms.has(filmId)) {
            allFilms.set(filmId, {
              id: filmId,
              title: data.game,
              year: data.year || 2000,
              franchiseSlug: slugify(franchise),
              studioSlug: studioInfo.id,
              characterCount: 0,
              isGame: true
            });
            studioStats[studioInfo.id].filmCount++;
          }
        }

        // Extract characters
        const chars = extractCharacters(data, studioInfo.id, franchise);
        for (const char of chars) {
          allCharacters.push(char);
          studioStats[studioInfo.id].characterCount++;
        }

      } catch (err) {
        console.error(`Error processing ${file}:`, err.message);
      }
    }
  }

  // Also process root-level JSON files
  const rootFiles = fs.readdirSync(DATA_DIR).filter(f => f.endsWith('.json'));
  for (const file of rootFiles) {
    try {
      const content = fs.readFileSync(path.join(DATA_DIR, file), 'utf-8');
      const data = JSON.parse(content);

      if (!data.characters && !data.categories) continue;

      const franchise = data.franchise || file.replace('.json', '').replace(/_/g, ' ');
      const studioId = 'wdas';

      franchiseSet.add({ name: franchise, studioId });

      const chars = extractCharacters(data, studioId, franchise);
      for (const char of chars) {
        allCharacters.push(char);
      }
    } catch (err) {
      // Skip files that can't be parsed as character data
    }
  }

  // Deduplicate characters by name + franchise
  const charMap = new Map();
  for (const char of allCharacters) {
    const key = `${char.name}-${char.franchiseName}-${char.studioId}`;
    if (!charMap.has(key)) {
      charMap.set(key, char);
    }
  }

  console.log(`\nFound ${charMap.size} unique characters`);
  console.log(`Found ${allFilms.size} films/games`);
  console.log(`Found ${franchiseSet.size} franchises\n`);

  // Begin transaction for bulk inserts
  console.log('Inserting data into database...');

  const insertAll = db.transaction(() => {
    // Insert studios
    for (const studioInfo of Object.values(STUDIO_MAP)) {
      insertStudio.run({
        slug: studioInfo.id,
        name: studioInfo.name,
        shortName: studioInfo.shortName
      });
    }

    // Build studio cache
    for (const row of db.prepare('SELECT id, slug FROM studios').all()) {
      studioCache[row.slug] = row.id;
    }

    // Insert franchises
    for (const { name, studioId } of franchiseSet) {
      const slug = slugify(name);
      const studioDbId = studioCache[studioId];
      insertFranchise.run({
        slug,
        name,
        studioId: studioDbId
      });
    }

    // Build franchise cache
    for (const row of db.prepare('SELECT id, slug FROM franchises').all()) {
      franchiseCache[row.slug] = row.id;
    }

    // Insert media (films/games)
    for (const film of allFilms.values()) {
      const franchiseDbId = franchiseCache[film.franchiseSlug];
      insertMedia.run({
        slug: film.id,
        title: film.title,
        year: film.year,
        franchiseId: franchiseDbId,
        mediaType: film.isGame ? 'game' : 'film'
      });
    }

    // Build media cache
    for (const row of db.prepare('SELECT id, slug FROM media').all()) {
      mediaCache[row.slug] = row.id;
    }

    // Insert films table entries
    for (const film of allFilms.values()) {
      if (!film.isGame) {
        const mediaDbId = mediaCache[film.id];
        const studioDbId = studioCache[film.studioSlug];
        if (mediaDbId && studioDbId) {
          insertFilm.run({
            mediaId: mediaDbId,
            studioId: studioDbId,
            animationType: 'fully_animated',
            characterCount: 0
          });
        }
      }
    }

    // Insert characters
    let charIndex = 0;
    for (const char of charMap.values()) {
      const slug = slugify(char.name) + '-' + (charIndex++);
      const species = normalizeSpecies(char.species);
      const gender = normalizeGender(char.gender);
      const role = normalizeRole(char.role);

      // Ensure species exists
      if (!speciesCache[species]) {
        insertSpecies.run({ name: species });
        const row = getSpeciesId.get(species);
        if (row) speciesCache[species] = row.id;
      }

      const speciesId = speciesCache[species] || null;
      const genderId = genderCache[gender] || null;
      const studioDbId = studioCache[char.studioId] || null;
      const franchiseDbId = franchiseCache[slugify(char.franchiseName)] || null;

      insertCharacter.run({
        slug,
        name: char.name,
        originFranchise: char.franchiseName,
        speciesId,
        genderId,
        voiceActor: char.voice_actor || null,
        notes: char.notes || null,
        franchiseId: franchiseDbId,
        studioId: studioDbId
      });
    }
  });

  insertAll();

  // Get final counts
  const stats = {
    studios: db.prepare('SELECT COUNT(*) as count FROM studios').get().count,
    franchises: db.prepare('SELECT COUNT(*) as count FROM franchises').get().count,
    media: db.prepare('SELECT COUNT(*) as count FROM media').get().count,
    films: db.prepare('SELECT COUNT(*) as count FROM films').get().count,
    characters: db.prepare('SELECT COUNT(*) as count FROM characters').get().count,
  };

  console.log('\n=== Database Stats ===');
  console.log(`Studios:    ${stats.studios}`);
  console.log(`Franchises: ${stats.franchises}`);
  console.log(`Media:      ${stats.media}`);
  console.log(`Films:      ${stats.films}`);
  console.log(`Characters: ${stats.characters}`);

  // Show sample queries
  console.log('\n=== Sample Data ===');

  console.log('\nTop 5 Studios by Character Count:');
  const studioQuery = db.prepare(`
    SELECT s.name, s.short_name, COUNT(c.id) as char_count
    FROM studios s
    LEFT JOIN characters c ON c.studio_id = s.id
    GROUP BY s.id
    ORDER BY char_count DESC
    LIMIT 5
  `);
  for (const row of studioQuery.all()) {
    console.log(`  ${row.short_name}: ${row.char_count} characters`);
  }

  console.log('\nGender Distribution:');
  const genderQuery = db.prepare(`
    SELECT g.name, COUNT(*) as count
    FROM characters c
    LEFT JOIN genders g ON c.gender_id = g.id
    GROUP BY g.name
    ORDER BY count DESC
  `);
  for (const row of genderQuery.all()) {
    console.log(`  ${row.name || 'unknown'}: ${row.count}`);
  }

  db.close();

  console.log(`\nDatabase saved to: ${dbPath}`);
  console.log('Done!');
}

// Run
const dbPath = process.argv[2] || DEFAULT_DB_PATH;
consolidateAndLoad(dbPath);
