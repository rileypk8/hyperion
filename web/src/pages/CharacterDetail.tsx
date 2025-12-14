import { useMemo } from 'react';
import { useParams, Link } from 'react-router-dom';
import { useCharacterById, useStudioById, useFilms } from '../hooks/useDuckDB';
import {
  getCharacterById as getMockCharacter,
  getStudioById as getMockStudio,
  films as mockFilms,
} from '../data/mockData';

export function CharacterDetail() {
  const { id } = useParams<{ id: string }>();
  const charId = id || '';

  // DuckDB data
  const { character: duckCharacter, loading: charLoading } = useCharacterById(charId);
  const { data: duckFilms, loading: filmsLoading } = useFilms();

  // Fallback to mockData
  const character = duckCharacter || getMockCharacter(charId);
  const films = duckFilms.length > 0 ? duckFilms : mockFilms;

  // Get studio
  const { studio: duckStudio } = useStudioById(character?.studioId || '');
  const studio = duckStudio || (character ? getMockStudio(character.studioId) : undefined);

  // Get character's films
  const characterFilms = useMemo(() => {
    if (!character) return [];
    // filmIds may be a JSON string from DuckDB or an array from mockData
    const filmIds = typeof character.filmIds === 'string'
      ? JSON.parse(character.filmIds)
      : character.filmIds || [];
    return films.filter((f) => filmIds.includes(f.id));
  }, [character, films]);

  const loading = charLoading || filmsLoading;

  if (!character && !loading) {
    return (
      <div className="page not-found">
        <h1>Character Not Found</h1>
        <Link to="/characters">Back to Characters</Link>
      </div>
    );
  }

  if (!character) {
    return <div className="page">Loading...</div>;
  }

  // Parse filmIds for display
  const filmIds = typeof character.filmIds === 'string'
    ? JSON.parse(character.filmIds)
    : character.filmIds || [];

  return (
    <div className="page character-detail">
      <div className="page-header">
        <Link to="/characters" className="back-link">
          &larr; All Characters
        </Link>
      </div>

      <div className="character-hero">
        <div className="char-avatar-large">{character.name.charAt(0)}</div>
        <div className="char-hero-info">
          <h1>{character.name}</h1>
          <div className="char-badges">
            <span className="role-badge">{character.role}</span>
            <span className="gender-badge">{character.gender}</span>
            <span className="species-badge">{character.species}</span>
          </div>
        </div>
      </div>

      <div className="detail-grid">
        <div className="detail-card">
          <h3>Voice Actor</h3>
          <p>{character.voiceActor || 'Non-speaking / Unknown'}</p>
        </div>
        <div className="detail-card">
          <h3>Studio</h3>
          {studio ? (
            <Link to={`/studio/${studio.id}`}>{studio.name}</Link>
          ) : (
            <p>Unknown</p>
          )}
        </div>
        <div className="detail-card">
          <h3>Appearances</h3>
          <p>{filmIds.length} film(s)</p>
        </div>
      </div>

      {character.notes && (
        <div className="section">
          <h2>Notes</h2>
          <p>{character.notes}</p>
        </div>
      )}

      <div className="section">
        <h2>Film Appearances</h2>
        {characterFilms.length > 0 ? (
          <div className="films-list">
            {characterFilms.map((film) => (
              <Link key={film.id} to={`/film/${film.id}`} className="film-item">
                <span className="film-year">{film.year}</span>
                <span className="film-title">{film.title}</span>
              </Link>
            ))}
          </div>
        ) : (
          <p className="empty-state">Film data not yet available.</p>
        )}
      </div>
    </div>
  );
}
