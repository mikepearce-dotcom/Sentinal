import React, { createContext, useState, useEffect, useCallback } from 'react';
import api from '../api/axios';

export const AuthContext = createContext(null);

export const AuthProvider = ({ children }) => {
  const [user, setUser] = useState(null);

  const fetchUser = useCallback(async ({ throwOnError = false } = {}) => {
    try {
      const resp = await api.get('/api/auth/me');
      setUser(resp.data);
      return resp.data;
    } catch (e) {
      setUser(null);
      if (throwOnError) {
        throw e;
      }
      return null;
    }
  }, []);

  const login = async (email, password) => {
    const resp = await api.post('/api/auth/login', { email, password });
    const token = resp.data?.access_token;
    if (!token) {
      throw new Error('No access token returned by server');
    }
    localStorage.setItem('token', token);
    await fetchUser({ throwOnError: true });
  };

  const signup = async (email, name, password) => {
    await api.post('/api/auth/signup', { email, name, password });
    await login(email, password);
  };

  const logout = () => {
    localStorage.removeItem('token');
    setUser(null);
  };

  useEffect(() => {
    if (localStorage.getItem('token')) {
      fetchUser();
    }
  }, [fetchUser]);

  return (
    <AuthContext.Provider value={{ user, login, signup, logout }}>
      {children}
    </AuthContext.Provider>
  );
};
