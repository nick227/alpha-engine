from app.discovery.fundamentals_fmp import fetch_fmp_fundamentals


def test_fetch_fmp_fundamentals_uses_stable_endpoints(monkeypatch):
    calls = []

    def fake_get_json(url, params, timeout_s=30):
        calls.append((url, dict(params)))
        if url == "https://financialmodelingprep.com/stable/profile":
            return [
                {
                    "sector": "Technology",
                    "industry": "Software",
                    "sharesOutstanding": "1000",
                }
            ]
        if url == "https://financialmodelingprep.com/stable/income-statement":
            return [
                {"revenue": 100},
                {"revenue": 200},
                {"revenue": 300},
                {"revenue": 400},
            ]
        raise AssertionError(f"Unexpected FMP URL: {url}")

    monkeypatch.setattr("app.discovery.fundamentals_fmp._get_json", fake_get_json)

    snapshot = fetch_fmp_fundamentals("aapl", api_key="test-key")

    assert snapshot.ticker == "AAPL"
    assert snapshot.sector == "Technology"
    assert snapshot.industry == "Software"
    assert snapshot.shares_outstanding == 1000.0
    assert snapshot.revenue_ttm == 1000.0
    assert calls[0][0] == "https://financialmodelingprep.com/stable/profile"
    assert calls[0][1]["symbol"] == "AAPL"
    assert calls[1][0] == "https://financialmodelingprep.com/stable/income-statement"
    assert calls[1][1]["symbol"] == "AAPL"
    assert calls[1][1]["period"] == "quarter"
    assert calls[1][1]["limit"] == 4
