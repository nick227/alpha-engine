import urllib.request
import json

req = urllib.request.Request('http://127.0.0.1:8090/openapi.json', headers={'x-internal-key': '2b5497217d6d8f2bbed32589f9f3b33828c6f05b116dc2ea97d1335f1edf7522'})
try:
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read().decode('utf-8'))
        print('PATHS COUNT:', len(data.get('paths', {})))
        for p in sorted(data.get('paths', {}).keys()):
            print(p)
except Exception as e:
    print('ERROR:', e)
