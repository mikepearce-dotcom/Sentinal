import React, { useContext } from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { AuthProvider, AuthContext } from './context/AuthContext';
import Landing from './pages/Landing';
import Login from './pages/Login';
import Signup from './pages/Signup';
import ForgotPassword from './pages/ForgotPassword';
import Dashboard from './pages/Dashboard';
import GameDetail from './pages/GameDetail';
import Account from './pages/Account';

function AppRoutes() {
  const { user, authLoading } = useContext(AuthContext);

  if (authLoading) {
    return (
      <div className="min-h-screen bg-[#09090b] text-zinc-300 flex items-center justify-center">
        Authenticating...
      </div>
    );
  }

  return (
    <Routes>
      <Route path="/" element={<Landing />} />
      <Route path="/app" element={user ? <Dashboard /> : <Navigate to="/login" />} />
      <Route path="/games/:id" element={user ? <GameDetail /> : <Navigate to="/login" />} />
      <Route path="/account" element={user ? <Account /> : <Navigate to="/login" />} />
      <Route path="/login" element={!user ? <Login /> : <Navigate to="/app" />} />
      <Route path="/signup" element={!user ? <Signup /> : <Navigate to="/app" />} />
      <Route path="/forgot-password" element={!user ? <ForgotPassword /> : <Navigate to="/app" />} />
      <Route path="*" element={<Navigate to={user ? '/app' : '/'} />} />
    </Routes>
  );
}

function App() {
  return (
    <div className="min-h-screen bg-[#09090b] text-white">
      <Router>
        <AppRoutes />
      </Router>
    </div>
  );
}

export default function RootApp() {
  return (
    <AuthProvider>
      <App />
    </AuthProvider>
  );
}
