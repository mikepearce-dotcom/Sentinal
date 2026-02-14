# Sentient Tracker

This repository contains the Sentient Tracker web application which monitors Reddit sentiment for video games.

- **backend/** - Python FastAPI API server
- **frontend/** - React dashboard

## Getting started

1. **Backend**
   ```bash
   cd backend
   python -m venv venv
   source venv/bin/activate    # Windows: venv\\Scripts\\activate
   pip install -r requirements.txt
   cp .env.example .env        # fill in values
   # required variables: MONGO_URL, DB_NAME, OPENAI_API_KEY, JWT_SECRET, CORS_ORIGINS
   uvicorn app.main:app --reload
   ```
2. **Frontend**
   ```bash
   cd frontend
   npm install
   npm start
   ```

Frontend expects the backend at `http://localhost:8000` by default.

Refer to `# Sentient Tracker - Specification.txt` for full requirements.
