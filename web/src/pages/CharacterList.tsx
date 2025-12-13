import { useState } from 'react';
import { Link } from 'react-router-dom';
import { characters, studios } from '../data/mockData';
import type { Role } from '../types';

const ROLES: Role[] = [
  'protagonist',
  'deuteragonist',
  'hero',
  'villain',
  'antagonist',
  'sidekick',
  'comic_relief',
  'supporting',
  'minor',
];

export function CharacterList() {
  const [search, setSearch] = useState('');
  const [filterRole, setFilterRole] = useState<string>('all');
  const [filterStudio, setFilterStudio] = useState<string>('all');
  const [filterGender, setFilterGender] = useState<string>('all');

  const filtered = characters.filter((c) => {
    if (search && !c.name.toLowerCase().includes(search.toLowerCase())) return false;
    if (filterRole !== 'all' && c.role !== filterRole) return false;
    if (filterStudio !== 'all' && c.studioId !== filterStudio) return false;
    if (filterGender !== 'all' && c.gender !== filterGender) return false;
    return true;
  });

  return (
    <div className="page character-list">
      <h1>Characters</h1>
      <p className="page-desc">Browse characters across Disney animation</p>

      <div className="search-bar">
        <input
          type="text"
          placeholder="Search characters..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
      </div>

      <div className="filters">
        <select value={filterStudio} onChange={(e) => setFilterStudio(e.target.value)}>
          <option value="all">All Studios</option>
          {studios.map((s) => (
            <option key={s.id} value={s.id}>
              {s.shortName}
            </option>
          ))}
        </select>
        <select value={filterRole} onChange={(e) => setFilterRole(e.target.value)}>
          <option value="all">All Roles</option>
          {ROLES.map((r) => (
            <option key={r} value={r}>
              {r.replace('_', ' ')}
            </option>
          ))}
        </select>
        <select value={filterGender} onChange={(e) => setFilterGender(e.target.value)}>
          <option value="all">All Genders</option>
          <option value="male">Male</option>
          <option value="female">Female</option>
          <option value="unknown">Unknown</option>
        </select>
      </div>

      <p className="results-count">{filtered.length} characters</p>

      <div className="characters-grid">
        {filtered.map((char) => {
          const studio = studios.find((s) => s.id === char.studioId);
          return (
            <Link key={char.id} to={`/character/${char.id}`} className="character-card">
              <div className="char-avatar">{char.name.charAt(0)}</div>
              <div className="char-info">
                <h3>{char.name}</h3>
                <div className="char-meta">
                  <span className="role-badge">{char.role}</span>
                  <span className="gender-badge">{char.gender}</span>
                </div>
                <div className="char-details">
                  {char.species} &bull; {studio?.shortName}
                </div>
                {char.voiceActor && <div className="voice-actor">Voiced by {char.voiceActor}</div>}
              </div>
            </Link>
          );
        })}
      </div>

      {filtered.length === 0 && <p className="empty-state">No characters match your filters.</p>}
    </div>
  );
}
