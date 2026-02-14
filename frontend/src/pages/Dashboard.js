import React, { useContext, useEffect, useState } from 'react';
import { AuthContext } from '../context/AuthContext';
import api from '../api/axios';

const Dashboard = () => {
  const { user, logout } = useContext(AuthContext);
  const [games, setGames] = useState([]);
  const [name, setName] = useState('');
  const [subreddit, setSubreddit] = useState('');

  const loadGames = async () => {
    const resp = await api.get('/api/games');
    setGames(resp.data);
  };

  const addGame = async () => {
    await api.post('/api/games', { name, subreddit });
    setName('');
    setSubreddit('');
    loadGames();
  };

  useEffect(() => {
    loadGames();
  }, []);

  if (!user) return <p>Loading...</p>;

  return (
    <div className="p-4">
      <div className="flex justify-between items-center">
        <h2 className="text-2xl">Tracked Games</h2>
        <button onClick={logout} className="bg-red-500 px-2 py-1 rounded">
          Logout
        </button>
      </div>
      <div className="my-4 space-y-2">
        {games.map((g) => (
          <div key={g.id} className="p-2 bg-gray-800 rounded flex justify-between items-center">
            <div>
              <h3 className="text-lg">{g.name}</h3>
              <p className="text-sm text-gray-400">r/{g.subreddit}</p>
            </div>
            <button
              className="bg-blue-500 px-2 py-1 rounded"
              onClick={async () => {
                await api.post(`/api/games/${g.id}/scan`);
                loadGames();
              }}
            >
              Run Scan
            </button>
          </div>
        ))}
      </div>
      <div className="mt-6">
        <h3 className="text-xl">Add Game</h3>
        <input
          type="text"
          placeholder="Name"
          value={name}
          onChange={(e) => setName(e.target.value)}
          className="w-full p-2 bg-gray-800 rounded mb-2"
        />
        <input
          type="text"
          placeholder="Subreddit"
          value={subreddit}
          onChange={(e) => setSubreddit(e.target.value)}
          className="w-full p-2 bg-gray-800 rounded mb-2"
        />
        <button onClick={addGame} className="bg-green-500 px-4 py-2 rounded">
          Add Game
        </button>
      </div>
    </div>
  );
};

export default Dashboard;
