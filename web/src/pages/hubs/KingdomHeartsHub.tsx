import { useCallback, useMemo, useRef } from 'react';
import { Link } from 'react-router-dom';
import ForceGraph2D from 'react-force-graph-2d';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
} from 'recharts';

// Mock data for KH characters - would come from API
const khCharacters = [
  // Original KH characters
  { id: 'sora', name: 'Sora', origin: 'original', games: 10 },
  { id: 'riku', name: 'Riku', origin: 'original', games: 9 },
  { id: 'kairi', name: 'Kairi', origin: 'original', games: 7 },
  { id: 'roxas', name: 'Roxas', origin: 'original', games: 6 },
  { id: 'axel', name: 'Axel/Lea', origin: 'original', games: 7 },
  { id: 'xion', name: 'Xion', origin: 'original', games: 4 },
  { id: 'ventus', name: 'Ventus', origin: 'original', games: 4 },
  { id: 'aqua', name: 'Aqua', origin: 'original', games: 4 },
  { id: 'terra', name: 'Terra', origin: 'original', games: 4 },
  { id: 'xehanort', name: 'Xehanort', origin: 'original', games: 6 },
  { id: 'ansem', name: 'Ansem SoD', origin: 'original', games: 5 },
  { id: 'xemnas', name: 'Xemnas', origin: 'original', games: 5 },
  // Disney characters
  { id: 'donald', name: 'Donald Duck', origin: 'disney', games: 8 },
  { id: 'goofy', name: 'Goofy', origin: 'disney', games: 8 },
  { id: 'mickey', name: 'Mickey Mouse', origin: 'disney', games: 9 },
  { id: 'maleficent', name: 'Maleficent', origin: 'disney', games: 6 },
  { id: 'pete', name: 'Pete', origin: 'disney', games: 5 },
  { id: 'jack', name: 'Jack Sparrow', origin: 'disney', games: 2 },
  { id: 'simba', name: 'Simba', origin: 'disney', games: 3 },
  { id: 'aladdin', name: 'Aladdin', origin: 'disney', games: 2 },
  { id: 'hercules', name: 'Hercules', origin: 'disney', games: 3 },
  { id: 'beast', name: 'Beast', origin: 'disney', games: 2 },
  { id: 'jack-skellington', name: 'Jack Skellington', origin: 'disney', games: 3 },
  { id: 'tron', name: 'Tron', origin: 'disney', games: 2 },
  // Final Fantasy characters
  { id: 'cloud', name: 'Cloud', origin: 'final_fantasy', games: 3 },
  { id: 'leon', name: 'Leon (Squall)', origin: 'final_fantasy', games: 2 },
  { id: 'aerith', name: 'Aerith', origin: 'final_fantasy', games: 2 },
  { id: 'yuffie', name: 'Yuffie', origin: 'final_fantasy', games: 2 },
  { id: 'cid', name: 'Cid', origin: 'final_fantasy', games: 2 },
  { id: 'sephiroth', name: 'Sephiroth', origin: 'final_fantasy', games: 2 },
  { id: 'tifa', name: 'Tifa', origin: 'final_fantasy', games: 1 },
  { id: 'zack', name: 'Zack', origin: 'final_fantasy', games: 1 },
];

// Connections between characters
const connections = [
  // Sora's main connections
  { source: 'sora', target: 'riku' },
  { source: 'sora', target: 'kairi' },
  { source: 'sora', target: 'donald' },
  { source: 'sora', target: 'goofy' },
  { source: 'sora', target: 'roxas' },
  // Roxas connections
  { source: 'roxas', target: 'axel' },
  { source: 'roxas', target: 'xion' },
  // BBS trio
  { source: 'ventus', target: 'aqua' },
  { source: 'ventus', target: 'terra' },
  { source: 'aqua', target: 'terra' },
  { source: 'ventus', target: 'roxas' },
  // Villains
  { source: 'xehanort', target: 'ansem' },
  { source: 'xehanort', target: 'xemnas' },
  { source: 'xehanort', target: 'terra' },
  { source: 'maleficent', target: 'pete' },
  { source: 'riku', target: 'ansem' },
  { source: 'riku', target: 'mickey' },
  // Mickey connections
  { source: 'mickey', target: 'donald' },
  { source: 'mickey', target: 'goofy' },
  { source: 'mickey', target: 'aqua' },
  // FF characters in Traverse Town/Radiant Garden
  { source: 'leon', target: 'cloud' },
  { source: 'leon', target: 'aerith' },
  { source: 'leon', target: 'yuffie' },
  { source: 'leon', target: 'cid' },
  { source: 'cloud', target: 'sephiroth' },
  { source: 'cloud', target: 'tifa' },
  { source: 'zack', target: 'aqua' },
  { source: 'sora', target: 'cloud' },
  { source: 'sora', target: 'leon' },
  { source: 'sora', target: 'hercules' },
  { source: 'sora', target: 'jack-skellington' },
  { source: 'sora', target: 'simba' },
  { source: 'axel', target: 'kairi' },
];

const ORIGIN_COLORS: Record<string, string> = {
  original: '#9b59b6',
  disney: '#3498db',
  final_fantasy: '#2ecc71',
};

// Game timeline data
const gameTimeline = [
  { name: 'KH1', year: 2002, sales: 6.4 },
  { name: 'CoM', year: 2004, sales: 1.5 },
  { name: 'KH2', year: 2005, sales: 6.0 },
  { name: '358/2', year: 2009, sales: 1.5 },
  { name: 'BBS', year: 2010, sales: 2.5 },
  { name: 'Re:coded', year: 2010, sales: 1.0 },
  { name: 'DDD', year: 2012, sales: 1.5 },
  { name: '0.2', year: 2017, sales: 0.5 },
  { name: 'KH3', year: 2019, sales: 6.7 },
  { name: 'MoM', year: 2020, sales: 0.5 },
];

// Species breakdown
const speciesBreakdown = [
  { name: 'Human', value: 45, color: '#3498db' },
  { name: 'Heartless', value: 20, color: '#2c3e50' },
  { name: 'Nobody', value: 15, color: '#95a5a6' },
  { name: 'Disney Animal', value: 12, color: '#e74c3c' },
  { name: 'Dream Eater', value: 8, color: '#9b59b6' },
];

export function KingdomHeartsHub() {
  const graphRef = useRef<any>(null);

  const graphData = useMemo(() => ({
    nodes: khCharacters.map((char) => ({
      id: char.id,
      name: char.name,
      origin: char.origin,
      games: char.games,
      color: ORIGIN_COLORS[char.origin],
    })),
    links: connections,
  }), []);

  const handleNodeClick = useCallback((node: any) => {
    if (graphRef.current) {
      graphRef.current.centerAt(node.x, node.y, 500);
      graphRef.current.zoom(2, 500);
    }
  }, []);

  const nodeCanvasObject = useCallback((node: any, ctx: CanvasRenderingContext2D, globalScale: number) => {
    const label = node.name;
    const fontSize = 12 / globalScale;
    ctx.font = `${fontSize}px Sans-Serif`;
    const textWidth = ctx.measureText(label).width;
    const bckgDimensions = [textWidth, fontSize].map((n) => n + fontSize * 0.2);

    // Node circle
    const nodeSize = 4 + node.games * 0.5;
    ctx.beginPath();
    ctx.arc(node.x, node.y, nodeSize, 0, 2 * Math.PI, false);
    ctx.fillStyle = node.color;
    ctx.fill();
    ctx.strokeStyle = 'rgba(255,255,255,0.3)';
    ctx.lineWidth = 1 / globalScale;
    ctx.stroke();

    // Label background
    ctx.fillStyle = 'rgba(15, 23, 42, 0.8)';
    ctx.fillRect(
      node.x - bckgDimensions[0] / 2,
      node.y + nodeSize + 2,
      bckgDimensions[0],
      bckgDimensions[1]
    );

    // Label text
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillStyle = '#f8fafc';
    ctx.fillText(label, node.x, node.y + nodeSize + 2 + fontSize / 2);
  }, []);

  return (
    <div className="hub-page">
      <Link to="/" className="back-link">← Back to Hub</Link>

      <div className="hub-header">
        <div className="hub-icon" style={{ background: '#9b59b6' }}>KH</div>
        <div className="hub-title">
          <h1>Kingdom Hearts</h1>
          <p>Disney × Square Enix crossover universe</p>
        </div>
      </div>

      <div className="charts-grid">
        {/* Character Network Graph */}
        <div className="chart-card" style={{ gridColumn: '1 / -1' }}>
          <h3>Character Relationship Network</h3>
          <div className="network-container">
            <div className="network-legend">
              <div className="legend-item">
                <div className="legend-dot" style={{ background: ORIGIN_COLORS.original }} />
                <span>Original KH</span>
              </div>
              <div className="legend-item">
                <div className="legend-dot" style={{ background: ORIGIN_COLORS.disney }} />
                <span>Disney</span>
              </div>
              <div className="legend-item">
                <div className="legend-dot" style={{ background: ORIGIN_COLORS.final_fantasy }} />
                <span>Final Fantasy</span>
              </div>
            </div>
            <div className="network-graph">
              <ForceGraph2D
                ref={graphRef}
                graphData={graphData}
                nodeCanvasObject={nodeCanvasObject}
                nodePointerAreaPaint={(node: any, color, ctx) => {
                  const nodeSize = 4 + node.games * 0.5;
                  ctx.fillStyle = color;
                  ctx.beginPath();
                  ctx.arc(node.x, node.y, nodeSize + 5, 0, 2 * Math.PI, false);
                  ctx.fill();
                }}
                linkColor={() => 'rgba(148, 163, 184, 0.3)'}
                linkWidth={1}
                onNodeClick={handleNodeClick}
                backgroundColor="#1e293b"
                width={undefined}
                height={450}
              />
            </div>
          </div>
        </div>

        {/* Game Sales Timeline */}
        <div className="chart-card">
          <h3>Game Sales by Release (millions)</h3>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={gameTimeline}>
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
              <Bar dataKey="sales" fill="#9b59b6" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Species Breakdown */}
        <div className="chart-card">
          <h3>Character Species Breakdown</h3>
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie
                data={speciesBreakdown}
                cx="50%"
                cy="50%"
                innerRadius={60}
                outerRadius={100}
                paddingAngle={2}
                dataKey="value"
                label={({ name, percent }) =>
                  `${name} ${((percent ?? 0) * 100).toFixed(0)}%`
                }
                labelLine={false}
              >
                {speciesBreakdown.map((entry, index) => (
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
        </div>
      </div>

      {/* Stats row */}
      <div className="stats-row">
        <div className="stat-card">
          <div className="stat-value">10</div>
          <div className="stat-label">Main Games</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">2002</div>
          <div className="stat-label">First Release</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">35M+</div>
          <div className="stat-label">Units Sold</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">100+</div>
          <div className="stat-label">Characters</div>
        </div>
      </div>
    </div>
  );
}
