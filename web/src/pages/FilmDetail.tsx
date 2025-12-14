import { useParams, Link } from 'react-router-dom';
import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip, Legend } from 'recharts';
import { useFilmById, useCharactersByFilm, useStudioById, useDuckDB } from '../hooks/useDuckDB';
import {
  getFilmById as getMockFilm,
  getCharactersByFilm as getMockCharacters,
  getStudioById as getMockStudio,
} from '../data/mockData';

const GENDER_COLORS = ['#0088FE', '#FF6B9D', '#00C49F'];

export function FilmDetail() {
  const { id } = useParams<{ id: string }>();
  const filmId = id || '';

  // Check DuckDB status
  const { isReady: duckDBReady, error: duckDBError } = useDuckDB();

  // DuckDB data
  const { film: duckFilm, loading: filmLoading } = useFilmById(filmId);
  const { data: duckCharacters, loading: charsLoading } = useCharactersByFilm(filmId);

  // Fallback to mockData if:
  // 1. DuckDB has an error, OR
  // 2. DuckDB is ready but we have no data and loading is complete
  const duckDBFailed = duckDBError !== null;
  const duckDBLoadedButEmpty = duckDBReady && !filmLoading && !duckFilm;
  const useMockData = duckDBFailed || duckDBLoadedButEmpty;

  const film = duckFilm || (useMockData ? getMockFilm(filmId) : null);

  // For characters, also fallback if DuckDB returned empty after loading
  const charsLoadedButEmpty = duckDBReady && !charsLoading && duckCharacters.length === 0;
  const useCharactersMock = duckDBFailed || charsLoadedButEmpty;
  const characters = duckCharacters.length > 0 ? duckCharacters : (useCharactersMock ? getMockCharacters(filmId) : []);

  // Get studio (need to handle async)
  const { studio: duckStudio } = useStudioById(film?.studioId || '');
  const studio = duckStudio || (film ? getMockStudio(film.studioId) : undefined);

  const loading = filmLoading || charsLoading;

  // Debug logging
  console.log('FilmDetail debug:', {
    filmId,
    duckDBReady,
    duckDBError: duckDBError?.message || null,
    filmLoading,
    charsLoading,
    duckFilm: duckFilm?.title || null,
    duckCharactersCount: duckCharacters.length,
    useMockData,
    useCharactersMock,
    finalFilm: film?.title || null,
    finalCharactersCount: characters.length,
  });

  if (!film && !loading) {
    return (
      <div className="page not-found">
        <h1>Film Not Found</h1>
        <p style={{ fontSize: '12px', color: '#666' }}>
          DuckDB: {duckDBReady ? 'Ready' : 'Loading...'} | Error: {duckDBError ? duckDBError.message : 'none'}
        </p>
        <Link to="/films">Back to Films</Link>
      </div>
    );
  }

  if (!film) {
    return (
      <div className="page">
        Loading...
        <p style={{ fontSize: '12px', color: '#666' }}>
          DuckDB: {duckDBReady ? 'Ready' : 'Loading...'} | Film loading: {filmLoading ? 'yes' : 'no'}
        </p>
      </div>
    );
  }

  // Gender breakdown
  const genderCounts = characters.reduce(
    (acc, c) => {
      if (c.gender === 'male') acc.male++;
      else if (c.gender === 'female') acc.female++;
      else acc.other++;
      return acc;
    },
    { male: 0, female: 0, other: 0 }
  );

  const genderData = [
    { name: 'Male', value: genderCounts.male },
    { name: 'Female', value: genderCounts.female },
    { name: 'Other', value: genderCounts.other },
  ].filter((d) => d.value > 0);

  return (
    <div className="page film-detail">
      <div className="page-header">
        <Link to="/films" className="back-link">
          &larr; All Films
        </Link>
        <h1>{film.title}</h1>
        <div className="film-meta-header">
          <span className="year">{film.year}</span>
          {studio && (
            <Link to={`/studio/${studio.id}`} className="studio-link">
              {studio.name}
            </Link>
          )}
        </div>
      </div>

      <div className="stats-row">
        <div className="stat-card">
          <div className="stat-value">{characters.length}</div>
          <div className="stat-label">Characters</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">
            {new Set(characters.map((c) => c.voiceActor).filter(Boolean)).size}
          </div>
          <div className="stat-label">Voice Actors</div>
        </div>
      </div>

      {genderData.length > 0 && (
        <div className="chart-card chart-small">
          <h3>Gender Distribution</h3>
          <ResponsiveContainer width="100%" height={200}>
            <PieChart>
              <Pie
                data={genderData}
                dataKey="value"
                nameKey="name"
                cx="50%"
                cy="50%"
                outerRadius={70}
                label
              >
                {genderData.map((_, index) => (
                  <Cell key={`cell-${index}`} fill={GENDER_COLORS[index % GENDER_COLORS.length]} />
                ))}
              </Pie>
              <Tooltip />
              <Legend />
            </PieChart>
          </ResponsiveContainer>
        </div>
      )}

      <div className="section">
        <h2>Characters</h2>
        {characters.length > 0 ? (
          <div className="characters-table">
            <table>
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Role</th>
                  <th>Species</th>
                  <th>Voice Actor</th>
                </tr>
              </thead>
              <tbody>
                {characters.map((char) => (
                  <tr key={char.id}>
                    <td>
                      <Link to={`/character/${char.id}`}>{char.name}</Link>
                    </td>
                    <td>{char.role}</td>
                    <td>{char.species}</td>
                    <td>{char.voiceActor || '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="empty-state">No character data available for this film yet.</p>
        )}
      </div>
    </div>
  );
}
