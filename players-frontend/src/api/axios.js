import axios from 'axios';

const api = axios.create({
  baseURL: process.env.REACT_APP_BACKEND_URL || 'http://localhost:8000',
});

let accessTokenGetter = null;

export const setAccessTokenGetter = (getter) => {
  accessTokenGetter = getter;
};

api.interceptors.request.use(async (config) => {
  if (!config.headers) {
    return config;
  }

  if (!accessTokenGetter) {
    return config;
  }

  try {
    const token = await accessTokenGetter();
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
  } catch (_error) {
    // If silent token retrieval fails, continue without Authorization header.
  }

  return config;
});

export default api;
