import { Link } from 'react-router-dom';
import { useStudios } from '../hooks/useDuckDB';
import { studios as mockStudios } from '../data/mockData';

export function StudioList() {
  const { data: duckDBStudios, loading, error } = useStudios();

  // Use DuckDB data when available, fallback to mockData
  const studios = duckDBStudios.length > 0 ? duckDBStudios : mockStudios;

  return (
    <div className="page studio-list">
      <h1>Studios</h1>
      <p className="page-desc">Animation studios in the Disney media portfolio</p>

      {loading && <p className="loading-indicator">Loading from DuckDB...</p>}
      {error && <p className="error-indicator">DuckDB error, using cached data</p>}

      <div className="studio-grid">
        {studios.map((studio) => (
          <Link key={studio.id} to={`/studio/${studio.id}`} className="studio-card">
            <div className="studio-icon">{studio.shortName.charAt(0)}</div>
            <div className="studio-info">
              <h3>{studio.name}</h3>
              <div className="studio-stats">
                <span>{studio.filmCount} films</span>
                <span>{studio.characterCount} characters</span>
              </div>
            </div>
          </Link>
        ))}
      </div>
    </div>
  );
}
