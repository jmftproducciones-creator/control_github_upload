import os

from app import app, run_migrations


if os.getenv("RUN_MIGRATIONS_ON_START", "1").strip().lower() in {"1", "true", "yes", "si", "s"}:
    run_migrations()

application = app
