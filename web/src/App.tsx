import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { AuthProvider } from './context/AuthContext';
import { MainLayout } from './layouts/MainLayout';
import {
  LandingPage,
  Dashboard,
  StudioList,
  StudioDetail,
  FilmList,
  FilmDetail,
  CharacterList,
  CharacterDetail,
  KingdomHeartsHub,
  WDASHub,
  GenericStudioHub,
} from './pages';
import './App.css';

function App() {
  return (
    <AuthProvider>
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<MainLayout />}>
            <Route index element={<LandingPage />} />
            <Route path="dashboard" element={<Dashboard />} />
            {/* Studio Hubs */}
            <Route path="hub/kingdom-hearts" element={<KingdomHeartsHub />} />
            <Route path="hub/wdas" element={<WDASHub />} />
            <Route path="hub/:studioId" element={<GenericStudioHub />} />
            {/* Data browsing */}
            <Route path="studios" element={<StudioList />} />
            <Route path="studio/:id" element={<StudioDetail />} />
            <Route path="films" element={<FilmList />} />
            <Route path="film/:id" element={<FilmDetail />} />
            <Route path="characters" element={<CharacterList />} />
            <Route path="character/:id" element={<CharacterDetail />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </AuthProvider>
  );
}

export default App;
