import { Link } from 'react-router-dom';

interface StudioNode {
  id: string;
  name: string;
  shortName: string;
  color: string;
}

const studioNodes: StudioNode[] = [
  {
    id: 'wdas',
    name: 'Walt Disney Animation Studios',
    shortName: 'WDAS',
    color: '#1e90ff',
  },
  {
    id: 'pixar',
    name: 'Pixar Animation Studios',
    shortName: 'Pixar',
    color: '#ff6b35',
  },
  {
    id: 'kingdom-hearts',
    name: 'Kingdom Hearts',
    shortName: 'KH',
    color: '#9b59b6',
  },
  {
    id: 'marvel',
    name: 'Marvel Animation',
    shortName: 'Marvel',
    color: '#e63946',
  },
  {
    id: 'records',
    name: 'Walt Disney Records',
    shortName: 'Records',
    color: '#f4a261',
  },
  {
    id: 'blue-sky',
    name: 'Blue Sky Studios',
    shortName: 'Blue Sky',
    color: '#00b4d8',
  },
  {
    id: 'disneytoon',
    name: 'DisneyToon Studios',
    shortName: 'DisneyToon',
    color: '#2a9d8f',
  },
  {
    id: 'interactive',
    name: 'Disney Interactive',
    shortName: 'Interactive',
    color: '#e76f51',
  },
];

// Calculate evenly spaced positions around a circle
function getNodePosition(index: number, total: number, radius: number = 38) {
  const angleStep = (2 * Math.PI) / total;
  const startAngle = -Math.PI / 2; // Start from top
  const angle = startAngle + index * angleStep;

  const x = 50 + radius * Math.cos(angle);
  const y = 50 + radius * Math.sin(angle);

  return { top: `${y}%`, left: `${x}%` };
}

export function LandingPage() {
  return (
    <div className="landing-page">
      <div className="hub-container">
        {/* Center Castle - Company Wide */}
        <Link to="/dashboard" className="hub-center">
          <div className="castle-icon">
            <svg viewBox="0 0 100 100" className="castle-svg">
              {/* Castle base */}
              <rect x="20" y="60" width="60" height="35" fill="currentColor" />
              {/* Main tower */}
              <rect x="40" y="30" width="20" height="30" fill="currentColor" />
              <polygon points="50,10 35,30 65,30" fill="currentColor" />
              {/* Left tower */}
              <rect x="15" y="45" width="15" height="15" fill="currentColor" />
              <polygon points="22.5,35 12,45 33,45" fill="currentColor" />
              {/* Right tower */}
              <rect x="70" y="45" width="15" height="15" fill="currentColor" />
              <polygon points="77.5,35 67,45 88,45" fill="currentColor" />
              {/* Door */}
              <rect x="42" y="75" width="16" height="20" fill="var(--bg-primary)" />
              {/* Windows */}
              <rect x="25" y="68" width="8" height="10" fill="var(--bg-primary)" />
              <rect x="67" y="68" width="8" height="10" fill="var(--bg-primary)" />
              <rect x="46" y="40" width="8" height="10" fill="var(--bg-primary)" />
            </svg>
          </div>
          <div className="hub-center-text">
            <h2>Walt Disney Studios</h2>
          </div>
        </Link>

        {/* Connecting lines */}
        <svg className="hub-lines" viewBox="0 0 100 100" preserveAspectRatio="none">
          {studioNodes.map((node, index) => {
            const pos = getNodePosition(index, studioNodes.length);
            return (
              <line
                key={node.id}
                x1="50%"
                y1="50%"
                x2={pos.left}
                y2={pos.top}
                stroke="var(--border)"
                strokeWidth="0.5"
                strokeDasharray="2,2"
              />
            );
          })}
        </svg>

        {/* Studio nodes */}
        {studioNodes.map((node, index) => {
          const pos = getNodePosition(index, studioNodes.length);
          return (
            <Link
              key={node.id}
              to={`/hub/${node.id}`}
              className="studio-node"
              style={{
                top: pos.top,
                left: pos.left,
                '--node-color': node.color,
              } as React.CSSProperties}
            >
              <div className="node-icon" style={{ background: node.color }}>
                {node.shortName.slice(0, 2)}
              </div>
              <span className="node-name">{node.shortName}</span>
            </Link>
          );
        })}
      </div>

      <div className="landing-footer">
        <p>Click the castle to view company-wide analytics, or select a studio for focused insights</p>
      </div>
    </div>
  );
}
