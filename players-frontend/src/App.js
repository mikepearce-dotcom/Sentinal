import React from 'react';
import { BrowserRouter as Router, Navigate, Route, Routes } from 'react-router-dom';
import AppFrame from './components/AppFrame';
import { AuthProvider } from './context/AuthContext';
import { useAuth } from './hooks/useAuth';
import AuthActionPage from './pages/AuthActionPage';
import CreatePetitionPage from './pages/CreatePetitionPage';
import LandingPage from './pages/LandingPage';
import PetitionDetailPage from './pages/PetitionDetailPage';
import PetitionsPage from './pages/PetitionsPage';

function AuthLoadingState() {
  return (
    <div className="page-shell max-w-3xl py-12">
      <div className="card-glass p-6 text-slate-500">Authenticating…</div>
    </div>
  );
}

function RequireAuth({ children }) {
  const { user, authLoading } = useAuth();
  if (authLoading) {
    return <AuthLoadingState />;
  }
  return user ? children : <Navigate to="/login" replace />;
}

function AppRoutes() {
  const { authLoading } = useAuth();

  if (authLoading) {
    return <AuthLoadingState />;
  }

  return (
    <Routes>
      <Route path="/" element={<LandingPage />} />
      <Route path="/petitions" element={<PetitionsPage />} />
      <Route path="/petitions/mine" element={<PetitionsPage mineOnly />} />
      <Route path="/petitions/new" element={<RequireAuth><CreatePetitionPage /></RequireAuth>} />
      <Route path="/petitions/:slug" element={<PetitionDetailPage />} />
      <Route path="/login" element={<AuthActionPage mode="login" />} />
      <Route path="/signup" element={<AuthActionPage mode="signup" />} />
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}

function AppInner() {
  return (
    <Router>
      <AppFrame>
        <AppRoutes />
      </AppFrame>
    </Router>
  );
}

export default function App() {
  return (
    <AuthProvider>
      <AppInner />
    </AuthProvider>
  );
}
