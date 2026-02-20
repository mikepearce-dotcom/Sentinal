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

export const AuthProvider = ({ children }) => {
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
    if (!isAuthenticated) {
      return '';
    }

    return await getAccessTokenSilently({
      authorizationParams: {
        audience: AUTH0_AUDIENCE,
      },
    });
  }, [getAccessTokenSilently, isAuthenticated]);

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

  const login = async () => {
    setAuthError('');
    await loginWithRedirect({
      authorizationParams: {
        audience: AUTH0_AUDIENCE,
      },
    });
  };

  const signup = async () => {
    setAuthError('');
    await loginWithRedirect({
      authorizationParams: {
        audience: AUTH0_AUDIENCE,
        screen_hint: 'signup',
      },
    });
  };

  const logout = () => {
    setAuthError('');
    setUser(null);
    setAccessTokenGetter(null);
    auth0Logout({
      logoutParams: {
        returnTo: window.location.origin,
      },
    });
  };

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
        if (!cancelled) {
          setAuthLoading(true);
        }
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
        if (!cancelled) {
          setAuthError(getErrorMessage(err));
        }
      } finally {
        if (!cancelled) {
          setAuthLoading(false);
        }
      }
    };

    syncUser();

    return () => {
      cancelled = true;
    };
  }, [fetchUser, isAuthenticated, isLoading]);

  const value = useMemo(
    () => ({
      user,
      login,
      signup,
      logout,
      authLoading,
      authError,
      isAuthenticated,
    }),
    [authError, authLoading, isAuthenticated, user]
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
};
