import React, { createContext, useState, useEffect } from 'react';
import api from '../api/axios';

export const AuthContext = createContext(null);

export const AuthProvider = ({ children }) => {
  const [user, setUser] = useState(null);

  const login = async (email, password) => {
    const resp = await api.post('/api/auth/login', { email, password });
    const token = resp.data.access_token;
    localStorage.setItem('token', token);
    await fetchUser();
  };

  const signup = async (email, name, password) => {
    await api.post('/api/auth/signup', { email, name, password });
    await login(email, password);
  };

  const logout = () => {
    localStorage.removeItem('token');
    setUser(null);
  };

  const fetchUser = async () => {
    try {
      const resp = await api.get('/api/auth/me');
      setUser(resp.data);
    } catch (e) {
      setUser(null);
    }
  };

  useEffect(() => {
    if (localStorage.getItem('token')) {
      fetchUser();
    }
  }, []);

  return (
    <AuthContext.Provider value={{ user, login, signup, logout }}>
      {children}
    </AuthContext.Provider>
  );
};
