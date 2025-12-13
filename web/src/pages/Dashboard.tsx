import {
  BarChart,
  Bar,
  LineChart,
  Line,
  ScatterChart,
  Scatter,
  PieChart,
  Pie,
  Cell,
  XAxis,
  YAxis,
  ZAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts';
import { studios, genderByYear, genderByRole, topTalents, mostAppearances, crossMediaStars, speciesBreakdown, sequelRetention } from '../data/mockData';

import { Link } from 'react-router-dom';

const SPECIES_COLORS = ['#8884d8', '#82ca9d', '#ffc658', '#ff7c43', '#00C49F', '#FFBB28', '#FF8042', '#0088FE', '#aaa'];

export function Dashboard() {
  // Calculate totals
  const totalFilms = studios.reduce((sum, s) => sum + s.filmCount, 0);
  const totalCharacters = studios.reduce((sum, s) => sum + s.characterCount, 0);

  // Prepare role data for pie chart
  const roleData = genderByRole.slice(0, 4).map((r) => ({
    name: r.role,
    male: r.male,
    female: r.female,
  }));

  return (
    <div className="dashboard">
      <h1>Dashboard</h1>

      {/* Stats cards */}
      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-value">{studios.length}</div>
          <div className="stat-label">Studios</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">{totalFilms}</div>
          <div className="stat-label">Films</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">{totalCharacters.toLocaleString()}</div>
          <div className="stat-label">Characters</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">{topTalents.length}+</div>
          <div className="stat-label">Voice Talents</div>
        </div>
      </div>

      {/* Charts row */}
      <div className="charts-grid">
        {/* Gender representation over time */}
        <div className="chart-card">
          <h3>Gender Representation Over Time</h3>
          <ResponsiveContainer width="100%" height={300}>
            <LineChart data={genderByYear}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="year" />
              <YAxis />
              <Tooltip />
              <Legend />
              <Line type="monotone" dataKey="male" stroke="#0088FE" strokeWidth={2} />
              <Line type="monotone" dataKey="female" stroke="#FF6B9D" strokeWidth={2} />
              <Line type="monotone" dataKey="other" stroke="#00C49F" strokeWidth={2} />
            </LineChart>
          </ResponsiveContainer>
        </div>

        {/* Characters by studio */}
        <div className="chart-card">
          <h3>Characters by Studio</h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={studios} layout="vertical">
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis type="number" />
              <YAxis dataKey="shortName" type="category" width={80} />
              <Tooltip />
              <Bar dataKey="characterCount" fill="#8884d8" />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Gender by role */}
        <div className="chart-card">
          <h3>Gender Distribution by Role</h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={roleData}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="name" />
              <YAxis />
              <Tooltip />
              <Legend />
              <Bar dataKey="male" fill="#0088FE" />
              <Bar dataKey="female" fill="#FF6B9D" />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Top voice talents */}
        <div className="chart-card">
          <h3>Top Voice Talents (by Film Earnings)</h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={topTalents.slice(0, 6)} layout="vertical">
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis type="number" tickFormatter={(v) => `$${(v / 1e9).toFixed(1)}B`} />
              <YAxis dataKey="name" type="category" width={120} />
              <Tooltip formatter={(v: number) => `$${(v / 1e9).toFixed(2)}B`} />
              <Bar dataKey="estimatedEarnings" fill="#82ca9d" />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Most appearances across media */}
        <div className="chart-card">
          <h3>Most Appearances Across Media</h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={mostAppearances.slice(0, 8)} layout="vertical">
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis type="number" />
              <YAxis dataKey="name" type="category" width={100} />
              <Tooltip />
              <Legend />
              <Bar dataKey="films" stackId="a" fill="#8884d8" name="Films" />
              <Bar dataKey="games" stackId="a" fill="#82ca9d" name="Games" />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Cross-media stars scatter plot */}
        <div className="chart-card">
          <h3>Cross-Media Stars (Films vs Games)</h3>
          <ResponsiveContainer width="100%" height={300}>
            <ScatterChart>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis type="number" dataKey="films" name="Films" unit=" films" />
              <YAxis type="number" dataKey="games" name="Games" unit=" games" />
              <ZAxis type="number" dataKey="khAppearances" range={[50, 400]} name="KH Appearances" />
              <Tooltip
                cursor={{ strokeDasharray: '3 3' }}
                content={({ payload }) => {
                  if (payload && payload.length) {
                    const d = payload[0].payload;
                    return (
                      <div className="custom-tooltip">
                        <p><strong>{d.name}</strong></p>
                        <p>Films: {d.films} | Games: {d.games}</p>
                        <p>Kingdom Hearts: {d.khAppearances}</p>
                        <p>Origin: {d.origin}</p>
                      </div>
                    );
                  }
                  return null;
                }}
              />
              <Scatter name="Characters" data={crossMediaStars} fill="#8884d8" />
            </ScatterChart>
          </ResponsiveContainer>
        </div>

        {/* Species breakdown pie chart */}
        <div className="chart-card">
          <h3>Character Species Breakdown</h3>
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie
                data={speciesBreakdown}
                dataKey="count"
                nameKey="species"
                cx="50%"
                cy="50%"
                outerRadius={100}
                label={({ name, percent }) => `${name} ${((percent ?? 0) * 100).toFixed(0)}%`}
              >
                {speciesBreakdown.map((_, index) => (
                  <Cell key={`cell-${index}`} fill={SPECIES_COLORS[index % SPECIES_COLORS.length]} />
                ))}
              </Pie>
              <Tooltip formatter={(value: number) => `${value} characters`} />
            </PieChart>
          </ResponsiveContainer>
        </div>

        {/* Sequel character retention */}
        <div className="chart-card">
          <h3>Sequel Character Retention by Franchise</h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={sequelRetention} layout="vertical">
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis type="number" domain={[0, 100]} unit="%" />
              <YAxis dataKey="franchise" type="category" width={100} />
              <Tooltip
                formatter={(value: number, name: string) => [
                  name === 'retention' ? `${value}%` : value,
                  name === 'retention' ? 'Retention Rate' : name,
                ]}
              />
              <Bar dataKey="retention" fill="#82ca9d" name="Retention %" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Quick links */}
      <div className="quick-links">
        <h3>Quick Links</h3>
        <div className="links-grid">
          {studios.slice(0, 4).map((studio) => (
            <Link key={studio.id} to={`/studio/${studio.id}`} className="quick-link-card">
              <div className="ql-name">{studio.shortName}</div>
              <div className="ql-stats">
                {studio.filmCount} films, {studio.characterCount} characters
              </div>
            </Link>
          ))}
        </div>
      </div>
    </div>
  );
}
