import { useState, useMemo } from 'react';
import { Link } from 'react-router-dom';
import { useFilms, useStudios } from '../hooks/useDuckDB';
import { films as mockFilms, studios as mockStudios } from '../data/mockData';

export function FilmList() {
  const [sortBy, setSortBy] = useState<'year' | 'title'>('year');
  const [filterStudio, setFilterStudio] = useState<string>('all');

  // DuckDB data with fallback
  const { data: duckFilms, loading: filmsLoading } = useFilms();
  const { data: duckStudios, loading: studiosLoading } = useStudios();

  const films = duckFilms.length > 0 ? duckFilms : mockFilms;
  const studios = duckStudios.length > 0 ? duckStudios : mockStudios;

  const filteredFilms = useMemo(() => {
    return films
      .filter((f) => filterStudio === 'all' || f.studioId === filterStudio)
      .sort((a, b) => {
        if (sortBy === 'year') return b.year - a.year;
        return a.title.localeCompare(b.title);
      });
  }, [films, filterStudio, sortBy]);

  const loading = filmsLoading || studiosLoading;

  return (
    <div className="page film-list">
      <h1>Films</h1>
      <p className="page-desc">Animated films across all Disney studios</p>

      {loading && <p className="loading-indicator">Loading...</p>}

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
