import React from 'react';
import ReactDOM from 'react-dom/client';
import { Auth0Provider } from '@auth0/auth0-react';
import App from './App';
import './index.css';

const domain = process.env.REACT_APP_AUTH0_DOMAIN;
const clientId = process.env.REACT_APP_AUTH0_CLIENT_ID;
const audience = process.env.REACT_APP_AUTH0_AUDIENCE;

const rootElement = document.getElementById('root');
if (!rootElement) {
  throw new Error('Root element not found');
}

const root = ReactDOM.createRoot(rootElement);

const missingKeys = [];
if (!domain) missingKeys.push('REACT_APP_AUTH0_DOMAIN');
if (!clientId) missingKeys.push('REACT_APP_AUTH0_CLIENT_ID');
if (!audience) missingKeys.push('REACT_APP_AUTH0_AUDIENCE');

if (missingKeys.length > 0) {
  console.error('Missing Auth0 environment variables:', missingKeys.join(', '));
  root.render(
    <React.StrictMode>
      <div className="min-h-screen bg-[#09090b] text-zinc-200 flex items-center justify-center px-6">
        <div className="max-w-xl border border-red-500/30 bg-red-500/10 p-6">
          <h1 className="font-heading text-2xl font-bold text-red-300">Auth0 Configuration Missing</h1>
          <p className="mt-3 text-zinc-300">
            Set the required frontend environment variables and restart the frontend service.
          </p>
          <p className="mt-3 font-mono text-sm text-zinc-400">
            {missingKeys.join(', ')}
          </p>
        </div>
      </div>
    </React.StrictMode>
  );
} else {
  root.render(
    <React.StrictMode>
      <Auth0Provider
        domain={domain}
        clientId={clientId}
        authorizationParams={{
          redirect_uri: window.location.origin,
          audience,
        }}
      >
        <App />
      </Auth0Provider>
    </React.StrictMode>
  );
}
