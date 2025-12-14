/**
 * DuckDB-WASM Integration for Hyperion
 *
 * Provides client-side analytics by loading Parquet files into DuckDB-WASM.
 * Replaces the static mockData imports with dynamic SQL queries.
 */

import { useState, useEffect, useCallback, createContext, useContext } from 'react';
import * as duckdb from '@duckdb/duckdb-wasm';
import type {
  Studio,
  Franchise,
  Film,
  Character,
  GenderByYear,
  GenderByRole,
  TalentStats,
} from '../types';

// CDN URLs for DuckDB WASM bundles
const DUCKDB_BUNDLES: duckdb.DuckDBBundles = {
  mvp: {
    mainModule: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/dist/duckdb-mvp.wasm',
    mainWorker: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/dist/duckdb-browser-mvp.worker.js',
  },
  eh: {
    mainModule: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/dist/duckdb-eh.wasm',
    mainWorker: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/dist/duckdb-browser-eh.worker.js',
  },
};

// Parquet file paths (relative to public directory)
const PARQUET_FILES = {
  studios: '/data/studios.parquet',
  franchises: '/data/franchises.parquet',
  films: '/data/films.parquet',
  characters: '/data/characters.parquet',
  genderByYear: '/data/gender_by_year.parquet',
  genderByRole: '/data/gender_by_role.parquet',
  topTalents: '/data/top_talents.parquet',
} as const;

/**
 * Convert snake_case keys to camelCase
 */
function snakeToCamel(str: string): string {
  return str.replace(/_([a-z])/g, (_, letter) => letter.toUpperCase());
}

/**
 * Transform object keys from snake_case to camelCase
 */
function transformKeys<T>(obj: Record<string, unknown>): T {
  const result: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(obj)) {
    result[snakeToCamel(key)] = value;
  }
  return result as T;
}

// DuckDB context types
interface DuckDBContextValue {
  db: duckdb.AsyncDuckDB | null;
  conn: duckdb.AsyncDuckDBConnection | null;
  isLoading: boolean;
  isReady: boolean;
  error: Error | null;
  query: <T>(sql: string) => Promise<T[]>;
}

const DuckDBContext = createContext<DuckDBContextValue | null>(null);

// Singleton for database instance
let dbInstance: duckdb.AsyncDuckDB | null = null;
let connInstance: duckdb.AsyncDuckDBConnection | null = null;
let initPromise: Promise<void> | null = null;

/**
 * Initialize DuckDB-WASM and load Parquet files
 */
async function initializeDuckDB(): Promise<{
  db: duckdb.AsyncDuckDB;
  conn: duckdb.AsyncDuckDBConnection;
}> {
  if (dbInstance && connInstance) {
    return { db: dbInstance, conn: connInstance };
  }

  // Select best bundle for browser
  const bundle = await duckdb.selectBundle(DUCKDB_BUNDLES);
  const worker = new Worker(bundle.mainWorker!);
  const logger = new duckdb.ConsoleLogger();

  // Instantiate DuckDB
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

  // Create connection
  const conn = await db.connect();

  // Register Parquet files as tables
  for (const [tableName, filePath] of Object.entries(PARQUET_FILES)) {
    try {
      // Fetch the parquet file
      const response = await fetch(filePath);
      if (!response.ok) {
        console.warn(`Parquet file not found: ${filePath}, skipping...`);
        continue;
      }
      const buffer = await response.arrayBuffer();

      // Register file in DuckDB's virtual filesystem
      await db.registerFileBuffer(filePath, new Uint8Array(buffer));

      // Create table from parquet
      const snakeCaseName = tableName.replace(/([A-Z])/g, '_$1').toLowerCase();
      await conn.query(`
        CREATE TABLE IF NOT EXISTS ${snakeCaseName} AS
        SELECT * FROM read_parquet('${filePath}')
      `);

      console.log(`Loaded table: ${snakeCaseName}`);
    } catch (err) {
      console.warn(`Failed to load ${tableName}:`, err);
    }
  }

  dbInstance = db;
  connInstance = conn;

  return { db, conn };
}

/**
 * DuckDB Provider Component
 */
export function DuckDBProvider({ children }: { children: React.ReactNode }) {
  const [db, setDb] = useState<duckdb.AsyncDuckDB | null>(null);
  const [conn, setConn] = useState<duckdb.AsyncDuckDBConnection | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    if (!initPromise) {
      initPromise = initializeDuckDB()
        .then(({ db, conn }) => {
          setDb(db);
          setConn(conn);
          setIsLoading(false);
        })
        .catch((err) => {
          console.error('DuckDB initialization failed:', err);
          setError(err);
          setIsLoading(false);
        });
    } else {
      initPromise.then(() => {
        setDb(dbInstance);
        setConn(connInstance);
        setIsLoading(false);
      });
    }
  }, []);

  const query = useCallback(
    async <T,>(sql: string): Promise<T[]> => {
      if (!conn) {
        throw new Error('DuckDB connection not ready');
      }
      const result = await conn.query(sql);
      // Transform snake_case keys to camelCase to match TypeScript types
      return result.toArray().map((row) => transformKeys<T>(row.toJSON()));
    },
    [conn]
  );

  const value: DuckDBContextValue = {
    db,
    conn,
    isLoading,
    isReady: !isLoading && !error && !!conn,
    error,
    query,
  };

  return <DuckDBContext.Provider value={value}>{children}</DuckDBContext.Provider>;
}

/**
 * Core hook to access DuckDB
 */
export function useDuckDB() {
  const context = useContext(DuckDBContext);
  if (!context) {
    throw new Error('useDuckDB must be used within a DuckDBProvider');
  }
  return context;
}

/**
 * Hook for running arbitrary SQL queries
 */
export function useQuery<T>(sql: string, deps: unknown[] = []) {
  const { query, isReady } = useDuckDB();
  const [data, setData] = useState<T[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    if (!isReady) return;

    setLoading(true);
    query<T>(sql)
      .then((result) => {
        setData(result);
        setLoading(false);
      })
      .catch((err) => {
        setError(err);
        setLoading(false);
      });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isReady, sql, ...deps]);

  return { data, loading, error };
}

// =============================================================================
// Data Hooks - Replace mockData imports with SQL queries
// =============================================================================

/**
 * Get all studios
 */
export function useStudios() {
  return useQuery<Studio>('SELECT * FROM studios ORDER BY name');
}

/**
 * Get studio by ID
 */
export function useStudioById(id: string) {
  const { data, loading, error } = useQuery<Studio>(
    `SELECT * FROM studios WHERE id = '${id}' LIMIT 1`,
    [id]
  );
  return { studio: data[0] ?? null, loading, error };
}

/**
 * Get all franchises
 */
export function useFranchises() {
  return useQuery<Franchise>('SELECT * FROM franchises ORDER BY name');
}

/**
 * Get franchises by studio
 */
export function useFranchisesByStudio(studioId: string) {
  return useQuery<Franchise>(
    `SELECT * FROM franchises WHERE studio_id = '${studioId}' ORDER BY name`,
    [studioId]
  );
}

/**
 * Get all films
 */
export function useFilms() {
  return useQuery<Film>('SELECT * FROM films ORDER BY year DESC, title');
}

/**
 * Get film by ID
 */
export function useFilmById(id: string) {
  const { data, loading, error } = useQuery<Film>(
    `SELECT * FROM films WHERE id = '${id}' LIMIT 1`,
    [id]
  );
  return { film: data[0] ?? null, loading, error };
}

/**
 * Get films by studio
 */
export function useFilmsByStudio(studioId: string) {
  return useQuery<Film>(
    `SELECT * FROM films WHERE studio_id = '${studioId}' ORDER BY year DESC`,
    [studioId]
  );
}

/**
 * Get all characters
 */
export function useCharacters() {
  return useQuery<Character>('SELECT * FROM characters ORDER BY name');
}

/**
 * Get character by ID
 */
export function useCharacterById(id: string) {
  const { data, loading, error } = useQuery<Character>(
    `SELECT * FROM characters WHERE id = '${id}' LIMIT 1`,
    [id]
  );
  return { character: data[0] ?? null, loading, error };
}

/**
 * Get characters by studio
 */
export function useCharactersByStudio(studioId: string) {
  return useQuery<Character>(
    `SELECT * FROM characters WHERE studio_id = '${studioId}' ORDER BY name`,
    [studioId]
  );
}

/**
 * Get characters by film (using film_ids JSON string)
 */
export function useCharactersByFilm(filmId: string) {
  // film_ids is stored as a JSON string like '["film1", "film2"]'
  // Use LIKE to match the quoted ID within the JSON array
  const escapedId = filmId.replace(/'/g, "''");
  return useQuery<Character>(
    `SELECT * FROM characters WHERE film_ids LIKE '%"${escapedId}"%' ORDER BY name`,
    [filmId]
  );
}

/**
 * Search characters by name, voice actor, or species
 */
export function useCharacterSearch(query: string) {
  const searchQuery = query.toLowerCase().replace(/'/g, "''");
  return useQuery<Character>(
    query
      ? `SELECT * FROM characters
         WHERE lower(name) LIKE '%${searchQuery}%'
            OR lower(voice_actor) LIKE '%${searchQuery}%'
            OR lower(species) LIKE '%${searchQuery}%'
         ORDER BY name
         LIMIT 100`
      : 'SELECT * FROM characters ORDER BY name LIMIT 100',
    [query]
  );
}

/**
 * Get gender distribution by year
 */
export function useGenderByYear() {
  return useQuery<GenderByYear>('SELECT * FROM gender_by_year ORDER BY year');
}

/**
 * Get gender distribution by role
 */
export function useGenderByRole() {
  return useQuery<GenderByRole>('SELECT * FROM gender_by_role ORDER BY role');
}

/**
 * Get top voice talents
 */
export function useTopTalents(limit = 20) {
  return useQuery<TalentStats>(
    `SELECT * FROM top_talents ORDER BY film_count DESC LIMIT ${limit}`,
    [limit]
  );
}

// =============================================================================
// Aggregation Queries - Compute analytics on-the-fly
// =============================================================================

/**
 * Get character count by role for a studio
 */
export function useRoleBreakdownByStudio(studioId: string) {
  return useQuery<{ role: string; count: number }>(
    `SELECT role, COUNT(*) as count
     FROM characters
     WHERE studio_id = '${studioId}'
     GROUP BY role
     ORDER BY count DESC`,
    [studioId]
  );
}

/**
 * Get gender breakdown for a film
 */
export function useGenderBreakdownByFilm(filmId: string) {
  const escapedId = filmId.replace(/'/g, "''");
  return useQuery<{ gender: string; count: number }>(
    `SELECT gender, COUNT(*) as count
     FROM characters
     WHERE film_ids LIKE '%"${escapedId}"%'
     GROUP BY gender`,
    [filmId]
  );
}

/**
 * Get species breakdown for a studio
 */
export function useSpeciesBreakdownByStudio(studioId: string) {
  return useQuery<{ species: string; count: number }>(
    `SELECT species, COUNT(*) as count
     FROM characters
     WHERE studio_id = '${studioId}'
     GROUP BY species
     ORDER BY count DESC
     LIMIT 20`,
    [studioId]
  );
}

/**
 * Get films count by year
 */
export function useFilmsByYear() {
  return useQuery<{ year: number; count: number }>(
    `SELECT year, COUNT(*) as count
     FROM films
     GROUP BY year
     ORDER BY year`
  );
}

export default useDuckDB;
