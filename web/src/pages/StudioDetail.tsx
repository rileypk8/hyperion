import { useParams, Link } from 'react-router-dom';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts';
import { getStudioById, getFilmsByStudio, getCharactersByStudio } from '../data/mockData';

export function StudioDetail() {
  const { id } = useParams<{ id: string }>();
  const studio = getStudioById(id || '');
  const films = getFilmsByStudio(id || '');
  const characters = getCharactersByStudio(id || '');

  if (!studio) {
    return (
      <div className="page not-found">
        <h1>Studio Not Found</h1>
        <Link to="/studios">Back to Studios</Link>
      </div>
    );
  }

  // Group characters by role
  const roleBreakdown = characters.reduce(
    (acc, c) => {
      acc[c.role] = (acc[c.role] || 0) + 1;
      return acc;
    },
    {} as Record<string, number>
  );

  const roleData = Object.entries(roleBreakdown).map(([role, count]) => ({
    role,
    count,
  }));

  return (
    <div className="page studio-detail">
      <div className="page-header">
        <Link to="/studios" className="back-link">
          &larr; All Studios
        </Link>
        <h1>{studio.name}</h1>
      </div>

      <div className="stats-row">
        <div className="stat-card">
          <div className="stat-value">{studio.filmCount}</div>
          <div className="stat-label">Films</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">{studio.characterCount}</div>
          <div className="stat-label">Characters</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">{studio.franchiseIds.length}</div>
          <div className="stat-label">Franchises</div>
        </div>
      </div>

      {roleData.length > 0 && (
        <div className="chart-card">
          <h3>Characters by Role</h3>
          <ResponsiveContainer width="100%" height={250}>
            <BarChart data={roleData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="role" />
              <YAxis />
              <Tooltip />
              <Bar dataKey="count" fill="#8884d8" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      <div className="section">
        <h2>Films ({films.length})</h2>
        {films.length > 0 ? (
          <div className="films-list">
            {films.map((film) => (
              <Link key={film.id} to={`/film/${film.id}`} className="film-item">
                <span className="film-year">{film.year}</span>
                <span className="film-title">{film.title}</span>
                <span className="film-chars">{film.characterCount} characters</span>
              </Link>
            ))}
          </div>
        ) : (
          <p className="empty-state">No films in mock data for this studio yet.</p>
        )}
      </div>

      <div className="section">
        <h2>Characters ({characters.length})</h2>
        {characters.length > 0 ? (
          <div className="characters-grid">
            {characters.slice(0, 12).map((char) => (
              <Link key={char.id} to={`/character/${char.id}`} className="character-card-small">
                <div className="char-name">{char.name}</div>
                <div className="char-meta">
                  {char.role} &bull; {char.species}
                </div>
              </Link>
            ))}
          </div>
        ) : (
          <p className="empty-state">No characters in mock data for this studio yet.</p>
        )}
      </div>
    </div>
  );
}
