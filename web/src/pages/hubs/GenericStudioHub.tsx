import { Link, useParams } from 'react-router-dom';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts';

interface StudioConfig {
  name: string;
  shortName: string;
  color: string;
  description: string;
  stats: { label: string; value: string }[];
  chartData: { name: string; value: number }[];
  chartLabel: string;
}

const studioConfigs: Record<string, StudioConfig> = {
  pixar: {
    name: 'Pixar Animation Studios',
    shortName: 'PX',
    color: '#ff6b35',
    description: 'Computer animation pioneers since 1995',
    stats: [
      { label: 'Feature Films', value: '28' },
      { label: 'First Film', value: '1995' },
      { label: 'Oscar Wins', value: '15' },
      { label: 'Avg. RT Score', value: '89%' },
    ],
    chartData: [
      { name: 'Toy Story', value: 373 },
      { name: 'Finding Nemo', value: 871 },
      { name: 'Incredibles', value: 631 },
      { name: 'Finding Dory', value: 1029 },
      { name: 'Incredibles 2', value: 1243 },
      { name: 'Inside Out', value: 857 },
      { name: 'Coco', value: 807 },
      { name: 'Inside Out 2', value: 1698 },
    ],
    chartLabel: 'Top Box Office (millions)',
  },
  'blue-sky': {
    name: 'Blue Sky Studios',
    shortName: 'BS',
    color: '#00b4d8',
    description: 'Animation studio (1987-2021)',
    stats: [
      { label: 'Feature Films', value: '13' },
      { label: 'Years Active', value: '1987-2021' },
      { label: 'Ice Age Films', value: '5' },
      { label: 'Total Box Office', value: '$5.9B' },
    ],
    chartData: [
      { name: 'Ice Age', value: 383 },
      { name: 'Ice Age 2', value: 660 },
      { name: 'Ice Age 3', value: 886 },
      { name: 'Ice Age 4', value: 877 },
      { name: 'Rio', value: 484 },
      { name: 'Rio 2', value: 500 },
    ],
    chartLabel: 'Box Office by Film (millions)',
  },
  disneytoon: {
    name: 'DisneyToon Studios',
    shortName: 'DT',
    color: '#2a9d8f',
    description: 'Direct-to-video sequels (1990-2018)',
    stats: [
      { label: 'Films Produced', value: '47' },
      { label: 'Years Active', value: '1990-2018' },
      { label: 'Sequel Series', value: '12' },
      { label: 'Planes Films', value: '2' },
    ],
    chartData: [
      { name: 'Aladdin', value: 3 },
      { name: 'Lion King', value: 2 },
      { name: 'Little Mermaid', value: 2 },
      { name: 'Tinker Bell', value: 6 },
      { name: 'Planes', value: 2 },
      { name: 'Cinderella', value: 2 },
    ],
    chartLabel: 'Films per Franchise',
  },
  marvel: {
    name: 'Marvel Animation',
    shortName: 'MA',
    color: '#e63946',
    description: 'Animated superhero content',
    stats: [
      { label: 'TV Series', value: '45+' },
      { label: 'First Series', value: '1966' },
      { label: 'X-Men Series', value: '4' },
      { label: 'Spider-Man Series', value: '8' },
    ],
    chartData: [
      { name: 'Spider-Man', value: 8 },
      { name: 'X-Men', value: 4 },
      { name: 'Avengers', value: 3 },
      { name: 'Hulk', value: 3 },
      { name: 'Iron Man', value: 2 },
      { name: 'What If', value: 2 },
    ],
    chartLabel: 'Series per Franchise',
  },
  records: {
    name: 'Walt Disney Records',
    shortName: 'WR',
    color: '#f4a261',
    description: 'Music label since 1956',
    stats: [
      { label: 'Soundtracks', value: '500+' },
      { label: 'Founded', value: '1956' },
      { label: 'Grammy Wins', value: '25+' },
      { label: 'Oscar Songs', value: '15' },
    ],
    chartData: [
      { name: '1950s', value: 15 },
      { name: '1960s', value: 22 },
      { name: '1970s', value: 18 },
      { name: '1980s', value: 25 },
      { name: '1990s', value: 45 },
      { name: '2000s', value: 52 },
      { name: '2010s', value: 68 },
      { name: '2020s', value: 30 },
    ],
    chartLabel: 'Soundtrack Releases by Decade',
  },
  interactive: {
    name: 'Disney Interactive',
    shortName: 'DI',
    color: '#e76f51',
    description: 'Video games division',
    stats: [
      { label: 'Games Published', value: '200+' },
      { label: 'Years Active', value: '1988-2016' },
      { label: 'Key Franchise', value: 'Kingdom Hearts' },
      { label: 'Disney Infinity', value: '3 Games' },
    ],
    chartData: [
      { name: 'PS2', value: 45 },
      { name: 'PS3/PS4', value: 32 },
      { name: 'Xbox', value: 28 },
      { name: 'Nintendo', value: 52 },
      { name: 'Mobile', value: 75 },
      { name: 'PC', value: 38 },
    ],
    chartLabel: 'Games by Platform',
  },
};

export function GenericStudioHub() {
  const { studioId } = useParams<{ studioId: string }>();
  const config = studioConfigs[studioId || ''];

  if (!config) {
    return (
      <div className="hub-page">
        <Link to="/" className="back-link">← Back to Hub</Link>
        <h1>Studio Not Found</h1>
        <p>The requested studio hub doesn't exist.</p>
      </div>
    );
  }

  return (
    <div className="hub-page">
      <Link to="/" className="back-link">← Back to Hub</Link>

      <div className="hub-header">
        <div className="hub-icon" style={{ background: config.color }}>
          {config.shortName}
        </div>
        <div className="hub-title">
          <h1>{config.name}</h1>
          <p>{config.description}</p>
        </div>
      </div>

      <div className="stats-row">
        {config.stats.map((stat) => (
          <div key={stat.label} className="stat-card">
            <div className="stat-value">{stat.value}</div>
            <div className="stat-label">{stat.label}</div>
          </div>
        ))}
      </div>

      <div className="charts-grid">
        <div className="chart-card" style={{ gridColumn: '1 / -1' }}>
          <h3>{config.chartLabel}</h3>
          <ResponsiveContainer width="100%" height={350}>
            <BarChart data={config.chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#475569" />
              <XAxis dataKey="name" stroke="#94a3b8" fontSize={12} />
              <YAxis stroke="#94a3b8" fontSize={12} />
              <Tooltip
                contentStyle={{
                  background: '#1e293b',
                  border: '1px solid #475569',
                  borderRadius: '6px',
                }}
              />
              <Bar dataKey="value" fill={config.color} radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="section">
        <h2>Coming Soon</h2>
        <div className="empty-state">
          <p>More detailed visualizations for {config.name} are in development.</p>
          <p>Check back soon for character breakdowns, timeline views, and more!</p>
        </div>
      </div>
    </div>
  );
}
