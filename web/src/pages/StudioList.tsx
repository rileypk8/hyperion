import { Link } from 'react-router-dom';
import { studios } from '../data/mockData';

export function StudioList() {
  return (
    <div className="page studio-list">
      <h1>Studios</h1>
      <p className="page-desc">Animation studios in the Disney media portfolio</p>

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
