from pathlib import Path
from dotenv import load_dotenv
import os
import requests

root = Path.cwd()
load_dotenv(root / '.env', override=False)
key = os.getenv('FMP_API_KEY')
print('FMP_API_KEY set:', bool(key))
for url, params in [
    ('https://financialmodelingprep.com/api/v3/profile/AAPL', {'apikey': key}),
    ('https://financialmodelingprep.com/api/v3/income-statement/AAPL', {'apikey': key}),
    ('https://financialmodelingprep.com/stable/profile/AAPL', {'apikey': key}),
    ('https://financialmodelingprep.com/api/v4/profile/AAPL', {'apikey': key}),
    ('https://financialmodelingprep.com/stable/profile', {'symbol': 'AAPL', 'apikey': key}),
    ('https://financialmodelingprep.com/stable/income-statement', {'symbol': 'AAPL', 'period': 'quarter', 'limit': 8, 'apikey': key}),
]:
    try:
        r = requests.get(url, params=params, timeout=30)
        print('URL:', url)
        print('  status:', r.status_code)
        print('  headers:', {k: v for k, v in r.headers.items() if k.lower() in ['content-type', 'x-ratelimit-limit', 'x-ratelimit-remaining', 'x-ratelimit-reset']})
        print('  text:', r.text[:800].replace('\n', ' '))
    except Exception as exc:
        print('URL:', url)
        print('  exception:', type(exc).__name__, exc)
