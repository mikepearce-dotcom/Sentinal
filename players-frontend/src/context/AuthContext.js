import React, { createContext, useCallback, useEffect, useMemo, useState } from 'react';
import { useAuth0 } from '@auth0/auth0-react';
import api, { setAccessTokenGetter } from '../api/axios';

export const AuthContext = createContext(null);

const AUTH0_AUDIENCE = process.env.REACT_APP_AUTH0_AUDIENCE || '';

const getErrorMessage = (err) => {
  const detail = err?.response?.data?.detail;

  if (Array.isArray(detail)) {
    return detail.map((item) => item?.msg || 'Invalid input').join(', ');
  }
  if (typeof detail === 'string' && detail.trim()) {
    return detail;
  }
  if (!err?.response) {
    return 'Unable to reach backend. Check REACT_APP_BACKEND_URL and backend health.';
  }
  return 'Authentication failed. Please try again.';
};

export function AuthProvider({ children }) {
  const {
    isAuthenticated,
    isLoading,
    loginWithRedirect,
    logout: auth0Logout,
    getAccessTokenSilently,
  } = useAuth0();

  const [user, setUser] = useState(null);
  const [authLoading, setAuthLoading] = useState(true);
  const [authError, setAuthError] = useState('');

  const getToken = useCallback(async () => {
    if (!isAuthenticated) return '';
    return getAccessTokenSilently({ authorizationParams: { audience: AUTH0_AUDIENCE } });
  }, [getAccessTokenSilently, isAuthenticated]);

  const fetchUser = useCallback(async ({ throwOnError = false } = {}) => {
    try {
      const resp = await api.get('/api/auth/me');
      setUser(resp.data || null);
      return resp.data || null;
    } catch (err) {
      setUser(null);
      if (throwOnError) throw err;
      return null;
    }
  }, []);

  const login = useCallback(async () => {
    setAuthError('');
    await loginWithRedirect({ authorizationParams: { audience: AUTH0_AUDIENCE } });
  }, [loginWithRedirect]);

  const signup = useCallback(async () => {
    setAuthError('');
    await loginWithRedirect({ authorizationParams: { audience: AUTH0_AUDIENCE, screen_hint: 'signup' } });
  }, [loginWithRedirect]);

  const logout = useCallback(() => {
    setAuthError('');
    setUser(null);
    setAccessTokenGetter(null);
    auth0Logout({ logoutParams: { returnTo: window.location.origin } });
  }, [auth0Logout]);

  useEffect(() => {
    if (!isAuthenticated) {
      setAccessTokenGetter(null);
      return;
    }
    setAccessTokenGetter(getToken);
  }, [getToken, isAuthenticated]);

  useEffect(() => {
    let cancelled = false;

    const syncUser = async () => {
      if (isLoading) {
        if (!cancelled) setAuthLoading(true);
        return;
      }

      if (!isAuthenticated) {
        if (!cancelled) {
          setUser(null);
          setAuthError('');
          setAuthLoading(false);
        }
        return;
      }

      if (!cancelled) {
        setAuthLoading(true);
        setAuthError('');
      }

      try {
        await fetchUser({ throwOnError: true });
      } catch (err) {
        if (!cancelled) setAuthError(getErrorMessage(err));
      } finally {
        if (!cancelled) setAuthLoading(false);
      }
    };

    syncUser();
    return () => {
      cancelled = true;
    };
  }, [fetchUser, isAuthenticated, isLoading]);

  const value = useMemo(
    () => ({ user, authLoading, authError, isAuthenticated, login, signup, logout, refreshUser: fetchUser }),
    [authError, authLoading, fetchUser, isAuthenticated, login, logout, signup, user]
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}
