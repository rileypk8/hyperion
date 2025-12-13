import { Outlet, Link, useLocation } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';

export function MainLayout() {
  const { user, logout } = useAuth();
  const location = useLocation();

  const isActive = (path: string) => location.pathname === path;

  return (
    <div className="app-layout">
      <header className="app-header">
        <div className="header-left">
          <Link to="/" className="logo">
            <span className="logo-icon">H</span>
            <span className="logo-text">Hyperion</span>
          </Link>
          <nav className="main-nav">
            <Link to="/" className={isActive('/') ? 'active' : ''}>
              Hub
            </Link>
            <Link to="/dashboard" className={isActive('/dashboard') || location.pathname.startsWith('/hub') ? 'active' : ''}>
              Analytics
            </Link>
            <Link to="/studios" className={location.pathname.startsWith('/studio') ? 'active' : ''}>
              Studios
            </Link>
            <Link to="/films" className={location.pathname.startsWith('/film') ? 'active' : ''}>
              Films
            </Link>
            <Link to="/characters" className={location.pathname.startsWith('/character') ? 'active' : ''}>
              Characters
            </Link>
          </nav>
        </div>
        <div className="header-right">
          {user && (
            <div className="user-menu">
              <span className="user-name">{user.name}</span>
              <button onClick={logout} className="logout-btn">
                Logout
              </button>
            </div>
          )}
        </div>
      </header>
      <main className="app-main">
        <Outlet />
      </main>
      <footer className="app-footer">
        <p>Hyperion - Disney Media Properties Database</p>
      </footer>
    </div>
  );
}
