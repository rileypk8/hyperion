import { useState } from 'react';
import type { ReactNode } from 'react';
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
import { useStudios, useGenderByYear, useGenderByRole, useTopTalents } from '../hooks/useDuckDB';
import {
  studios as mockStudios,
  genderByYear as mockGenderByYear,
  genderByRole as mockGenderByRole,
  topTalents as mockTopTalents,
  mostAppearances,
  crossMediaStars,
  speciesBreakdown,
  sequelRetention,
  prolificActors,
  studioLoyalty,
  femaleProtagonists,
  villainGender,
  franchiseLongevity,
  characterDensity,
  roleBalance,
} from '../data/mockData';
import { ChartZoomModal } from '../components/ChartZoomModal';

import { Link } from 'react-router-dom';

const SPECIES_COLORS = ['#8884d8', '#82ca9d', '#ffc658', '#ff7c43', '#00C49F', '#FFBB28', '#FF8042', '#0088FE', '#aaa'];

interface ChartConfig {
  id: string;
  title: string;
  render: (height: number) => ReactNode;
}

export function Dashboard() {
  const [zoomedChart, setZoomedChart] = useState<string | null>(null);

  // DuckDB data with fallback to mockData
  const { data: duckStudios, loading: studiosLoading } = useStudios();
  const { data: duckGenderByYear, loading: gbyLoading } = useGenderByYear();
  const { data: duckGenderByRole, loading: gbrLoading } = useGenderByRole();
  const { data: duckTopTalents, loading: ttLoading } = useTopTalents(15);

  const studios = duckStudios.length > 0 ? duckStudios : mockStudios;
  const genderByYear = duckGenderByYear.length > 0 ? duckGenderByYear : mockGenderByYear;
  const genderByRole = duckGenderByRole.length > 0 ? duckGenderByRole : mockGenderByRole;
  const topTalents = duckTopTalents.length > 0 ? duckTopTalents : mockTopTalents;

  const loading = studiosLoading || gbyLoading || gbrLoading || ttLoading;

  // Calculate totals
  const totalFilms = studios.reduce((sum, s) => sum + s.filmCount, 0);
  const totalCharacters = studios.reduce((sum, s) => sum + s.characterCount, 0);

  // Prepare role data for pie chart
  const roleData = genderByRole.slice(0, 4).map((r) => ({
    name: r.role,
    male: r.male,
    female: r.female,
  }));

  // Chart configurations
  const charts: ChartConfig[] = [
    {
      id: 'gender-over-time',
      title: 'Gender Representation Over Time',
      render: (height) => (
        <ResponsiveContainer width="100%" height={height}>
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
      ),
    },
    {
      id: 'characters-by-studio',
      title: 'Characters by Studio',
      render: (height) => (
        <ResponsiveContainer width="100%" height={height}>
          <BarChart data={studios} layout="vertical">
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis type="number" />
            <YAxis dataKey="shortName" type="category" width={80} />
            <Tooltip />
            <Bar dataKey="characterCount" fill="#8884d8" />
          </BarChart>
        </ResponsiveContainer>
      ),
    },
    {
      id: 'gender-by-role',
      title: 'Gender Distribution by Role',
      render: (height) => (
        <ResponsiveContainer width="100%" height={height}>
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
      ),
    },
    {
      id: 'top-voice-talents',
      title: 'Top Voice Talents (by Film Earnings)',
      render: (height) => (
        <ResponsiveContainer width="100%" height={height}>
          <BarChart data={topTalents.slice(0, 6)} layout="vertical">
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis type="number" tickFormatter={(v) => `$${(v / 1e9).toFixed(1)}B`} />
            <YAxis dataKey="name" type="category" width={120} />
            <Tooltip formatter={(v: number) => `$${(v / 1e9).toFixed(2)}B`} />
            <Bar dataKey="estimatedEarnings" fill="#82ca9d" />
          </BarChart>
        </ResponsiveContainer>
      ),
    },
    {
      id: 'most-appearances',
      title: 'Most Appearances Across Media',
      render: (height) => (
        <ResponsiveContainer width="100%" height={height}>
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
      ),
    },
    {
      id: 'cross-media-stars',
      title: 'Cross-Media Stars (Films vs Games)',
      render: (height) => (
        <ResponsiveContainer width="100%" height={height}>
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
      ),
    },
    {
      id: 'species-breakdown',
      title: 'Character Species Breakdown',
      render: (height) => (
        <ResponsiveContainer width="100%" height={height}>
          <PieChart>
            <Pie
              data={speciesBreakdown}
              dataKey="count"
              nameKey="species"
              cx="50%"
              cy="50%"
              outerRadius={height > 400 ? 150 : 100}
              label={({ name, percent }) => `${name} ${((percent ?? 0) * 100).toFixed(0)}%`}
            >
              {speciesBreakdown.map((_, index) => (
                <Cell key={`cell-${index}`} fill={SPECIES_COLORS[index % SPECIES_COLORS.length]} />
              ))}
            </Pie>
            <Tooltip formatter={(value: number) => `${value} characters`} />
          </PieChart>
        </ResponsiveContainer>
      ),
    },
    {
      id: 'sequel-retention',
      title: 'Sequel Character Retention by Franchise',
      render: (height) => (
        <ResponsiveContainer width="100%" height={height}>
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
      ),
    },
    {
      id: 'prolific-actors',
      title: 'Most Prolific Voice Actors',
      render: (height) => (
        <ResponsiveContainer width="100%" height={height}>
          <BarChart data={prolificActors.slice(0, 8)} layout="vertical">
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis type="number" />
            <YAxis dataKey="name" type="category" width={130} />
            <Tooltip />
            <Legend />
            <Bar dataKey="characters" fill="#8884d8" name="Characters" />
            <Bar dataKey="films" fill="#ffc658" name="Films" />
          </BarChart>
        </ResponsiveContainer>
      ),
    },
    {
      id: 'studio-loyalty',
      title: 'Voice Actor Studio Loyalty',
      render: (height) => (
        <ResponsiveContainer width="100%" height={height}>
          <BarChart data={studioLoyalty}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="studio" />
            <YAxis />
            <Tooltip />
            <Legend />
            <Bar dataKey="loyalActors" fill="#8884d8" name="Loyal Actors" />
            <Bar dataKey="exclusiveActors" fill="#ff7c43" name="Exclusive Actors" />
          </BarChart>
        </ResponsiveContainer>
      ),
    },
    {
      id: 'female-protagonists',
      title: 'Female Protagonists Over Time',
      render: (height) => (
        <ResponsiveContainer width="100%" height={height}>
          <LineChart data={femaleProtagonists}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="decade" />
            <YAxis domain={[0, 100]} unit="%" />
            <Tooltip formatter={(value: number) => `${value}%`} />
            <Line type="monotone" dataKey="percentage" stroke="#FF6B9D" strokeWidth={2} name="Female %" />
          </LineChart>
        </ResponsiveContainer>
      ),
    },
    {
      id: 'villain-gender',
      title: 'Villain Gender Distribution',
      render: (height) => (
        <ResponsiveContainer width="100%" height={height}>
          <PieChart>
            <Pie
              data={villainGender}
              dataKey="count"
              nameKey="category"
              cx="50%"
              cy="50%"
              outerRadius={height > 400 ? 150 : 100}
              label={({ name, percent }) => `${name} ${((percent ?? 0) * 100).toFixed(0)}%`}
            >
              <Cell fill="#0088FE" />
              <Cell fill="#FF6B9D" />
              <Cell fill="#00C49F" />
            </Pie>
            <Tooltip />
          </PieChart>
        </ResponsiveContainer>
      ),
    },
    {
      id: 'franchise-longevity',
      title: 'Franchise Longevity (Years Active)',
      render: (height) => (
        <ResponsiveContainer width="100%" height={height}>
          <BarChart data={franchiseLongevity.slice(0, 8)} layout="vertical">
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis type="number" unit=" yrs" />
            <YAxis dataKey="franchise" type="category" width={120} />
            <Tooltip formatter={(value: number) => `${value} years`} />
            <Bar dataKey="span" fill="#82ca9d" />
          </BarChart>
        </ResponsiveContainer>
      ),
    },
    {
      id: 'character-density',
      title: 'Avg Characters Per Film by Studio',
      render: (height) => (
        <ResponsiveContainer width="100%" height={height}>
          <BarChart data={characterDensity}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="studio" />
            <YAxis />
            <Tooltip />
            <Bar dataKey="avgCharacters" fill="#8884d8" name="Avg Characters" />
          </BarChart>
        </ResponsiveContainer>
      ),
    },
    {
      id: 'role-balance',
      title: 'Role Balance by Studio',
      render: (height) => (
        <ResponsiveContainer width="100%" height={height}>
          <BarChart data={roleBalance}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="studio" />
            <YAxis />
            <Tooltip />
            <Legend />
            <Bar dataKey="protagonists" fill="#82ca9d" name="Protagonists" />
            <Bar dataKey="antagonists" fill="#ff7c43" name="Antagonists" />
            <Bar dataKey="sidekicks" fill="#8884d8" name="Sidekicks" />
          </BarChart>
        </ResponsiveContainer>
      ),
    },
  ];

  const selectedChart = charts.find((c) => c.id === zoomedChart);

  return (
    <div className="dashboard">
      <h1>Dashboard</h1>
      {loading && <p className="loading-indicator">Loading data...</p>}

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
