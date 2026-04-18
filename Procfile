# Railway / Heroku-style: one long-running web process serving the read API.
# Set in Railway: ALPHA_DB_PATH, INTERNAL_READ_KEY, INTERNAL_READ_HOST=0.0.0.0
# PORT is set automatically. Daily jobs are separate (scheduler / cron).
web: python -m app.internal_read_v1
