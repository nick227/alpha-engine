# Secrets and Keys (Internal)

## Purpose
Explain how API keys are referenced and resolved during ingestion/backfill.

## Audience
- Operators
- Developers

## When to use this
- A source fails with auth errors or you’re setting up a new environment.

## Prereqs
- Ability to set environment variables / `.env`

---

## How keys are resolved
Key indirection is implemented by:
- `config/keys.yaml` (maps provider → key/secret fields)
- `app/ingest/key_manager.py` (resolves `ENV:VARNAME` entries from process environment)

Example pattern in `config/keys.yaml`:
- `alpaca.key: "ENV:ALPACA_KEY"`

## `.env` loading behavior
Entry-point `start.py` loads `.env` best-effort:
- If `python-dotenv` is installed, it uses `dotenv.load_dotenv()`.
- Otherwise it uses a minimal parser (`_load_dotenv_min`) that supports `KEY=VALUE`.

## Known mismatch to fix before production
- `.env.example` currently uses `ALPACA_API_KEY` and `ALPACA_API_SECRET`
- `config/keys.yaml` currently expects `ALPACA_KEY` and `ALPACA_SECRET`

Align these names so keys resolve correctly in all environments.

## Verification steps
- Print resolved provider keys in a controlled environment (do not log secrets in shared logs).
- Confirm adapters that require keys read from `FetchContext.key_manager` (see `app/ingest/fetch_context.py` and adapter implementations in `app/ingest/adapters/`).

