/**
 * Hooks module exports
 */

export {
  DuckDBProvider,
  useDuckDB,
  useQuery,
  // Data hooks
  useStudios,
  useStudioById,
  useFranchises,
  useFranchisesByStudio,
  useFilms,
  useFilmById,
  useFilmsByStudio,
  useCharacters,
  useCharacterById,
  useCharactersByStudio,
  useCharactersByFilm,
  useCharacterSearch,
  useGenderByYear,
  useGenderByRole,
  useTopTalents,
  // Aggregation hooks
  useRoleBreakdownByStudio,
  useGenderBreakdownByFilm,
  useSpeciesBreakdownByStudio,
  useFilmsByYear,
} from './useDuckDB';
