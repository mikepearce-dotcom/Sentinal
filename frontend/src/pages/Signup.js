import React, { useState, useContext } from 'react';
import { Link } from 'react-router-dom';
import { AuthContext } from '../context/AuthContext';

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

  return 'Failed to create account. Please try again.';
};

const Signup = () => {
  const { signup } = useContext(AuthContext);
  const [email, setEmail] = useState('');
  const [name, setName] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState(null);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError(null);

    try {
      await signup(email, name, password);
    } catch (err) {
      setError(getErrorMessage(err));
    }
  };

  return (
    <div className="max-w-md mx-auto p-4">
      <h2 className="text-2xl mb-4">Sign Up</h2>
      {error && <p className="text-red-500 mb-2">{error}</p>}
      <form onSubmit={handleSubmit} className="space-y-2">
        <input
          type="email"
          placeholder="Email"
          value={email}
          onChange={(e) => setEmail(e.target.value)}
          className="w-full p-2 bg-gray-800 rounded"
          required
        />
        <input
          type="text"
          placeholder="Name"
          value={name}
          onChange={(e) => setName(e.target.value)}
          className="w-full p-2 bg-gray-800 rounded"
          required
        />
        <input
          type="password"
          placeholder="Password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          className="w-full p-2 bg-gray-800 rounded"
          required
        />
        <button className="w-full p-2 bg-green-500 rounded">Sign Up</button>
      </form>
      <p className="text-sm text-gray-300 mt-4">
        Already have an account?{' '}
        <Link to="/login" className="text-green-400 hover:underline">
          Log in
        </Link>
      </p>
    </div>
  );
};

export default Signup;
