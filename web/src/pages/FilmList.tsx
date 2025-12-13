import { useState } from 'react';
import { Link } from 'react-router-dom';
import { films, studios } from '../data/mockData';

export function FilmList() {
  const [sortBy, setSortBy] = useState<'year' | 'title'>('year');
  const [filterStudio, setFilterStudio] = useState<string>('all');

  const filteredFilms = films
    .filter((f) => filterStudio === 'all' || f.studioId === filterStudio)
    .sort((a, b) => {
      if (sortBy === 'year') return b.year - a.year;
      return a.title.localeCompare(b.title);
    });

  return (
    <div className="page film-list">
      <h1>Films</h1>
      <p className="page-desc">Animated films across all Disney studios</p>

      <div className="filters">
        <select value={filterStudio} onChange={(e) => setFilterStudio(e.target.value)}>
          <option value="all">All Studios</option>
          {studios.map((s) => (
            <option key={s.id} value={s.id}>
              {s.shortName}
            </option>
          ))}
        </select>
        <select value={sortBy} onChange={(e) => setSortBy(e.target.value as 'year' | 'title')}>
          <option value="year">Sort by Year</option>
          <option value="title">Sort by Title</option>
        </select>
      </div>

      <div className="films-grid">
        {filteredFilms.map((film) => {
          const studio = studios.find((s) => s.id === film.studioId);
          return (
            <Link key={film.id} to={`/film/${film.id}`} className="film-card">
              <div className="film-year-badge">{film.year}</div>
              <h3>{film.title}</h3>
              <div className="film-meta">
                <span className="film-studio">{studio?.shortName}</span>
                <span className="film-chars">{film.characterCount} characters</span>
              </div>
            </Link>
          );
        })}
      </div>

      {filteredFilms.length === 0 && (
        <p className="empty-state">No films match your filters.</p>
      )}
    </div>
  );
}
