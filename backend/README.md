# Sentient Tracker Backend

FastAPI backend for the Sentient Tracker application.

## Getting Started

1. Create a Python virtual environment and activate it:
   ```bash
   python -m venv venv
   source venv/bin/activate  # on Windows: venv\Scripts\activate
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Copy `.env.example` to `.env` and fill in environment variables.
   For production, set `AUTH0_DOMAIN`, `AUTH0_AUDIENCE`, and `AUTH0_CLIENT_ID`.
   Keep `ALLOW_LEGACY_AUTH` unset (or `false`) in production to avoid legacy-token fallback.
4. Run the server:
   ```bash
   uvicorn app.main:app --reload
   ```

## Testing

Run the test suite with:
```bash
pytest
```

The tests clear the database collections automatically, so they can be run repeatedly.
If you need to point tests at a different Mongo instance, set `MONGO_URL` before running.

## Structure

- `app/main.py` - application entrypoint
- `app/routes/` - API route modules
- `app/database.py` - MongoDB connection
- `app/models.py` - Pydantic schemas
