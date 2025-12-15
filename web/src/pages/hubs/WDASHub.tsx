import { useState } from 'react';
import type { ReactNode } from 'react';
import { Link } from 'react-router-dom';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  LineChart,
  Line,
  PieChart,
  Pie,
  Cell,
} from 'recharts';
import { ChartZoomModal } from '../../components/ChartZoomModal';

// Mock soundtrack data - would come from wdas_soundtracks_complete.json
const soundtracksByDecade = [
  {
    decade: '1930s',
    films: [
      { year: 1937, title: 'Snow White', tracks: 3, oscarWin: false, songs: ['Heigh-Ho', 'Whistle While You Work', 'Some Day My Prince Will Come'] },
    ],
  },
  {
    decade: '1940s',
    films: [
      { year: 1940, title: 'Pinocchio', tracks: 3, oscarWin: true, songs: ['When You Wish Upon a Star', 'Give a Little Whistle'] },
      { year: 1940, title: 'Fantasia', tracks: 6, oscarWin: false, songs: ['The Sorcerer\'s Apprentice'] },
      { year: 1941, title: 'Dumbo', tracks: 4, oscarWin: false, songs: ['Baby Mine', 'Pink Elephants on Parade'] },
      { year: 1942, title: 'Bambi', tracks: 4, oscarWin: false, songs: ['Love Is a Song'] },
    ],
  },
  {
    decade: '1950s',
    films: [
      { year: 1950, title: 'Cinderella', tracks: 4, oscarWin: false, songs: ['A Dream Is a Wish Your Heart Makes', 'Bibbidi-Bobbidi-Boo'] },
      { year: 1951, title: 'Alice in Wonderland', tracks: 5, oscarWin: false, songs: ['The Unbirthday Song'] },
      { year: 1953, title: 'Peter Pan', tracks: 4, oscarWin: false, songs: ['You Can Fly!', 'The Second Star to the Right'] },
      { year: 1955, title: 'Lady and the Tramp', tracks: 3, oscarWin: false, songs: ['Bella Notte'] },
      { year: 1959, title: 'Sleeping Beauty', tracks: 4, oscarWin: false, songs: ['Once Upon a Dream'] },
    ],
  },
  {
    decade: '1960s',
    films: [
      { year: 1961, title: '101 Dalmatians', tracks: 3, oscarWin: false, songs: ['Cruella de Vil'] },
      { year: 1963, title: 'The Sword in the Stone', tracks: 3, oscarWin: false, songs: [] },
      { year: 1967, title: 'The Jungle Book', tracks: 5, oscarWin: false, songs: ['The Bare Necessities', 'I Wan\'na Be Like You'] },
    ],
  },
  {
    decade: '1970s-80s',
    films: [
      { year: 1970, title: 'The Aristocats', tracks: 4, oscarWin: false, songs: ['Ev\'rybody Wants to Be a Cat'] },
      { year: 1977, title: 'The Rescuers', tracks: 3, oscarWin: false, songs: [] },
      { year: 1988, title: 'Oliver & Company', tracks: 5, oscarWin: false, songs: ['Why Should I Worry'] },
    ],
  },
  {
    decade: '1990s (Renaissance)',
    films: [
      { year: 1989, title: 'The Little Mermaid', tracks: 6, oscarWin: true, songs: ['Under the Sea', 'Part of Your World', 'Kiss the Girl'] },
      { year: 1991, title: 'Beauty and the Beast', tracks: 6, oscarWin: true, songs: ['Beauty and the Beast', 'Be Our Guest', 'Belle'] },
      { year: 1992, title: 'Aladdin', tracks: 5, oscarWin: true, songs: ['A Whole New World', 'Friend Like Me'] },
      { year: 1994, title: 'The Lion King', tracks: 5, oscarWin: true, songs: ['Circle of Life', 'Hakuna Matata', 'Can You Feel the Love Tonight'] },
      { year: 1995, title: 'Pocahontas', tracks: 5, oscarWin: true, songs: ['Colors of the Wind'] },
      { year: 1996, title: 'The Hunchback of Notre Dame', tracks: 6, oscarWin: false, songs: ['God Help the Outcasts', 'Hellfire'] },
      { year: 1997, title: 'Hercules', tracks: 5, oscarWin: false, songs: ['Go the Distance', 'I Won\'t Say I\'m in Love'] },
      { year: 1998, title: 'Mulan', tracks: 5, oscarWin: false, songs: ['Reflection', 'I\'ll Make a Man Out of You'] },
      { year: 1999, title: 'Tarzan', tracks: 5, oscarWin: true, songs: ['You\'ll Be in My Heart'] },
    ],
  },
  {
    decade: '2000s',
    films: [
      { year: 2002, title: 'Lilo & Stitch', tracks: 4, oscarWin: false, songs: ['Hawaiian Roller Coaster Ride'] },
      { year: 2007, title: 'Enchanted', tracks: 5, oscarWin: false, songs: ['That\'s How You Know', 'So Close'] },
      { year: 2009, title: 'The Princess and the Frog', tracks: 6, oscarWin: false, songs: ['Almost There', 'Down in New Orleans'] },
    ],
  },
  {
    decade: '2010s (Revival)',
    films: [
      { year: 2010, title: 'Tangled', tracks: 5, oscarWin: false, songs: ['I See the Light', 'When Will My Life Begin'] },
      { year: 2013, title: 'Frozen', tracks: 6, oscarWin: true, songs: ['Let It Go', 'Do You Want to Build a Snowman?', 'For the First Time in Forever'] },
      { year: 2016, title: 'Moana', tracks: 6, oscarWin: false, songs: ['How Far I\'ll Go', 'You\'re Welcome', 'Shiny'] },
      { year: 2017, title: 'Coco', tracks: 5, oscarWin: true, songs: ['Remember Me'] },
      { year: 2019, title: 'Frozen II', tracks: 6, oscarWin: false, songs: ['Into the Unknown', 'Show Yourself'] },
    ],
  },
  {
    decade: '2020s',
    films: [
      { year: 2021, title: 'Encanto', tracks: 6, oscarWin: false, songs: ['We Don\'t Talk About Bruno', 'Surface Pressure'] },
      { year: 2023, title: 'Wish', tracks: 5, oscarWin: false, songs: ['This Wish'] },
    ],
  },
];

// Oscar wins by decade
const oscarData = soundtracksByDecade.map((d) => ({
  decade: d.decade.replace(' (Renaissance)', '').replace(' (Revival)', ''),
  wins: d.films.filter((f) => f.oscarWin).length,
  nominations: d.films.length,
}));

// Composer frequency (mock)
const composerData = [
  { name: 'Alan Menken', films: 9, color: '#e74c3c' },
  { name: 'Randy Newman', films: 4, color: '#3498db' },
  { name: 'Kristen Anderson-Lopez', films: 3, color: '#9b59b6' },
  { name: 'Lin-Manuel Miranda', films: 2, color: '#2ecc71' },
  { name: 'Elton John', films: 2, color: '#f39c12' },
  { name: 'Phil Collins', films: 1, color: '#1abc9c' },
];

// Era comparison
const eraComparison = [
  { era: 'Golden Age (1937-1959)', avgSongs: 4.0, films: 10 },
  { era: 'Dark Age (1960-1988)', avgSongs: 3.5, films: 7 },
  { era: 'Renaissance (1989-1999)', avgSongs: 5.4, films: 10 },
  { era: 'Experimental (2000-2009)', avgSongs: 4.3, films: 6 },
  { era: 'Revival (2010-present)', avgSongs: 5.6, films: 8 },
];

interface ChartConfig {
  id: string;
  title: string;
  render: (height: number) => ReactNode;
}

export function WDASHub() {
  const [zoomedChart, setZoomedChart] = useState<string | null>(null);

  const charts: ChartConfig[] = [
    {
      id: 'oscar-wins',
      title: 'Best Original Song Oscars by Decade',
      render: (height) => (
        <ResponsiveContainer width="100%" height={height}>
          <BarChart data={oscarData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#475569" />
            <XAxis dataKey="decade" stroke="#94a3b8" fontSize={10} angle={-30} textAnchor="end" height={60} />
            <YAxis stroke="#94a3b8" fontSize={12} />
            <Tooltip
              contentStyle={{
                background: '#1e293b',
                border: '1px solid #475569',
                borderRadius: '6px',
              }}
            />
            <Bar dataKey="wins" fill="#ffd700" name="Oscar Wins" radius={[4, 4, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      ),
    },
    {
      id: 'composers',
      title: 'Most Prolific Composers',
      render: (height) => (
        <ResponsiveContainer width="100%" height={height}>
          <PieChart>
            <Pie
              data={composerData}
              cx="50%"
              cy="50%"
              outerRadius={height > 400 ? 140 : 100}
              paddingAngle={2}
              dataKey="films"
              label={({ name, value }) => `${name}: ${value}`}
              labelLine={false}
            >
              {composerData.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={entry.color} />
              ))}
            </Pie>
            <Tooltip
              contentStyle={{
                background: '#1e293b',
                border: '1px solid #475569',
                borderRadius: '6px',
              }}
            />
          </PieChart>
        </ResponsiveContainer>
      ),
    },
    {
      id: 'era-comparison',
      title: 'Average Songs per Film by Era',
      render: (height) => (
        <ResponsiveContainer width="100%" height={height}>
          <LineChart data={eraComparison}>
            <CartesianGrid strokeDasharray="3 3" stroke="#475569" />
            <XAxis dataKey="era" stroke="#94a3b8" fontSize={9} angle={-20} textAnchor="end" height={80} />
            <YAxis stroke="#94a3b8" fontSize={12} domain={[0, 6]} />
            <Tooltip
              contentStyle={{
                background: '#1e293b',
                border: '1px solid #475569',
                borderRadius: '6px',
              }}
            />
            <Line type="monotone" dataKey="avgSongs" stroke="#1e90ff" strokeWidth={3} dot={{ fill: '#1e90ff', r: 6 }} />
          </LineChart>
        </ResponsiveContainer>
      ),
    },
  ];

  const selectedChart = charts.find((c) => c.id === zoomedChart);

  return (
    <div className="hub-page">
      <Link to="/" className="back-link">← Back to Hub</Link>

      <div className="hub-header">
        <div className="hub-icon" style={{ background: '#1e90ff' }}>WD</div>
        <div className="hub-title">
          <h1>Walt Disney Animation Studios</h1>
          <p>Soundtracks & musical legacy since 1937</p>
        </div>
      </div>

      <div className="stats-row">
        <div className="stat-card">
          <div className="stat-value">62</div>
          <div className="stat-label">Animated Features</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">13</div>
          <div className="stat-label">Oscar-Winning Songs</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">1937</div>
          <div className="stat-label">First Feature</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">87</div>
          <div className="stat-label">Years of Music</div>
        </div>
      </div>

      <div className="charts-grid">
        {charts.map((chart) => (
          <div
            key={chart.id}
            className="chart-card zoomable"
            onClick={() => setZoomedChart(chart.id)}
          >
            <h3>{chart.title}</h3>
            {chart.render(300)}
          </div>
        ))}
      </div>

      {/* Zoom Modal */}
      {selectedChart && (
        <ChartZoomModal
          isOpen={!!zoomedChart}
          onClose={() => setZoomedChart(null)}
          title={selectedChart.title}
        >
          {selectedChart.render(500)}
        </ChartZoomModal>
      )}

      {/* Soundtrack Timeline */}
      <div className="section">
        <h2>Soundtrack Timeline</h2>
        <div className="soundtrack-timeline">
          {soundtracksByDecade.map((decade) => (
            <div key={decade.decade} className="decade-section">
              <div className="decade-header">{decade.decade}</div>
              {decade.films.map((film) => (
                <div key={film.title} className="soundtrack-item">
                  <span className="soundtrack-year">{film.year}</span>
                  <div className="soundtrack-film">
                    <strong>{film.title}</strong>
                    {film.oscarWin && <span className="award-badge">Oscar Winner</span>}
                    {film.songs.length > 0 && (
                      <div className="soundtrack-tracks">
                        {film.songs.slice(0, 3).join(' • ')}
                        {film.songs.length > 3 && ` +${film.songs.length - 3} more`}
                      </div>
                    )}
                  </div>
                </div>
              ))}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
