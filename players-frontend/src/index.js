import React from 'react';
import ReactDOM from 'react-dom/client';
import { Auth0Provider } from '@auth0/auth0-react';
import App from './App';
import './index.css';

const domain = process.env.REACT_APP_AUTH0_DOMAIN;
const clientId = process.env.REACT_APP_AUTH0_CLIENT_ID;
const audience = process.env.REACT_APP_AUTH0_AUDIENCE;

const rootElement = document.getElementById('root');
if (!rootElement) throw new Error('Root element not found');
const root = ReactDOM.createRoot(rootElement);

const missing = [];
if (!domain) missing.push('REACT_APP_AUTH0_DOMAIN');
if (!clientId) missing.push('REACT_APP_AUTH0_CLIENT_ID');
if (!audience) missing.push('REACT_APP_AUTH0_AUDIENCE');

if (missing.length > 0) {
  root.render(
    <React.StrictMode>
      <div className="min-h-screen bg-[#09090b] text-zinc-200 flex items-center justify-center px-6">
        <div className="card-glass p-6 max-w-xl w-full border border-red-400/20">
          <h1 className="font-heading text-3xl font-black text-red-300">Community Frontend Config Missing</h1>
          <p className="mt-3 text-zinc-300">Set the Auth0 environment variables for the players frontend service.</p>
          <p className="mt-3 font-mono text-sm text-zinc-400">{missing.join(', ')}</p>
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
