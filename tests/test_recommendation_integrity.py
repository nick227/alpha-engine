"""Consumer contract regression tests; all data lives in an isolated in-memory DB."""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import sqlite3

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.core import pipeline_gates as gates
from app.internal_read_v1 import recommendations as recs
from app.internal_read_v1 import intelligence_read
from app.internal_read_v1.api_routes import router


@pytest.fixture
def warehouse(monkeypatch):
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE price_bars(tenant_id TEXT,ticker TEXT,timeframe TEXT,timestamp TEXT,
                                open REAL,high REAL,low REAL,close REAL,volume REAL);
        CREATE TABLE predictions(tenant_id TEXT,ticker TEXT,timestamp TEXT,run_id TEXT);
        CREATE TABLE ranking_snapshots(tenant_id TEXT,ticker TEXT,timestamp TEXT,score REAL);
        CREATE TABLE consensus_signals(tenant_id TEXT,ticker TEXT,created_at TEXT,p_final REAL);
        CREATE TABLE candidate_queue(tenant_id TEXT,ticker TEXT,status TEXT);
        CREATE TABLE prediction_jobs(id TEXT,tenant_id TEXT,job_type TEXT,started_at TEXT,
                                     completed_at TEXT,status TEXT,run_id TEXT);
        CREATE TABLE prediction_runs(id TEXT,tenant_id TEXT,timeframe TEXT,created_at TEXT,
            ingress_start TEXT,ingress_end TEXT,prediction_start TEXT,prediction_end TEXT,regime TEXT);
    """)
    now = datetime.now(timezone.utc) - timedelta(minutes=5)
    ts = now.isoformat()
    tickers = [f"T{i}" for i in range(11)]
    for mod in (gates, recs, intelligence_read):
        monkeypatch.setattr(mod, "get_active_universe_tickers", lambda **kw: tickers)
    profile = {"longName": "Test", "sector": "Tech", "industry": "Software", "website": "example.test",
               "country": "US", "ipoDate": "2000-01-01", "marketCap": 1e9}
    from app.internal_read_v1 import chart_market
    monkeypatch.setattr(chart_market, "load_company_profile_json", lambda t: profile)
    monkeypatch.setattr(recs, "load_company_profile_json", lambda t: profile)
    for ticker in tickers:
        conn.execute("INSERT INTO price_bars VALUES (?,?,?,?,?,?,?,?,?)", ('default',ticker,'1d',ts,1.5,1.6,1.4,1.5,2e6))
        conn.execute("INSERT INTO predictions VALUES (?,?,?,?)", ('default',ticker,ts,'run'))
        conn.execute("INSERT INTO ranking_snapshots VALUES (?,?,?,?)", ('default',ticker,ts,0.8))
        conn.execute("INSERT INTO consensus_signals VALUES (?,?,?,?)", ('default',ticker,ts,0.8))
        conn.execute("INSERT INTO candidate_queue VALUES (?,?,?)", ('default',ticker,'admitted'))
    conn.execute("INSERT INTO prediction_runs VALUES (?,?,?,?,?,?,?,?,?)",
                 ('run','default','1d',ts,ts,ts,ts,ts,'NORMAL'))
    conn.execute("INSERT INTO prediction_jobs VALUES (?,?,?,?,?,?,?)",
                 ('job','default','run_queue',ts,ts,'success','run'))
    recs._ensure_table(conn)
    # Existing materialization establishes breadth for the health read model.
    for ticker in tickers:
        conn.execute("INSERT INTO house_recommendations(tenant_id,mode,ticker,action,confidence,score,risk,horizon,as_of) "
                     "VALUES ('default','balanced',?,'WATCH',0,0,'Unavailable','policy',?)",(ticker,ts))
    conn.commit()
    yield conn
    conn.close()


@pytest.fixture
def client(warehouse):
    app = FastAPI()
    app.state.service = SimpleNamespace(store=SimpleNamespace(conn=warehouse))
    app.include_router(router)
    with TestClient(app) as client:
        yield client


def test_healthy_contract_preserves_scores_without_fabricated_guidance(client):
    response = client.get('/api/recommendations/latest')
    assert response.status_code == 200
    body = response.json()
    assert body['qualityStatus']['status'] == 'healthy'
    assert body['pipelineSignals']['predictionPipelineActive'] is True
    for row in body['recommendations']:
        assert row['actionable'] is True
        assert row['confidence'] is row['successProbability'] is None
        assert row['heuristicConfidence']['isProbability'] is False
        assert row['scoreMethodology']['unit'] == 'relative_score'
        assert row['entryZone'] is row['risk'] is row['expectedReturn'] is None
        assert row['thesis'] == row['avoidIf'] == []
        assert row['explanations']
        assert row['explanationMethodology'] == 'deterministic_score_components'
    assert client.get('/api/recommendations/best').status_code == 200


@pytest.mark.parametrize('table,column,reason', [
    ('price_bars','timestamp','INSUFFICIENT_FRESH_PRICE_COVERAGE'),
    ('predictions','timestamp','PREDICTIONS_STALE_OR_MISSING'),
    ('prediction_runs','created_at','PIPELINE_RUN_STALE_OR_INVALID'),
    ('consensus_signals','created_at','STALE_CONSENSUS'),
])
def test_stale_sources_fail_closed_independently(warehouse, client, monkeypatch, table, column, reason):
    monkeypatch.setenv('PIPELINE_BLOCK_BEST_WITHOUT_PREDICTIONS','0')
    monkeypatch.setenv('PIPELINE_BAR_COVERAGE_SLA','0')
    monkeypatch.setenv('PIPELINE_FRESH_BAR_DAYS','9999')
    warehouse.execute(f"UPDATE {table} SET {column} = '2020-01-01'")
    response = client.get('/api/recommendations/latest')
    assert response.status_code == 200
    body = response.json()
    assert reason in body['suppressionReasons']
    assert body['recommendations']
    assert all(r['action'] == 'WATCH' and not r['actionable'] for r in body['recommendations'])
    assert client.get('/api/recommendations/best').status_code == 503
    assert client.get('/api/recommendations/T0').json()['action'] == 'WATCH'
    assert all(r['action'] == 'WATCH' for r in client.get('/api/recommendations/under/2').json()['recommendations'])
    assert warehouse.execute("SELECT COUNT(*) FROM house_recommendations WHERE action IN ('BUY','SELL')").fetchone()[0] == 0


@pytest.mark.parametrize('table', ['predictions','prediction_runs','price_bars'])
def test_missing_required_intelligence(warehouse, client, table):
    warehouse.execute(f'DELETE FROM {table}')
    assert client.get('/api/recommendations/best').status_code == 503
    body = client.get('/api/recommendations/latest').json()
    assert body['qualityStatus']['actionable'] is False
    assert all(not r['actionable'] for r in body['recommendations'])


@pytest.mark.parametrize('table,column,label', [
    ('price_bars','timestamp','PRICE'), ('predictions','timestamp','PREDICTION'),
    ('ranking_snapshots','timestamp','RANKING'), ('consensus_signals','created_at','CONSENSUS'),
])
def test_one_stale_symbol_does_not_borrow_freshness(warehouse, client, table, column, label):
    # More than 90% coverage remains sufficient globally; T0 stays suppressed.
    warehouse.execute(f"UPDATE {table} SET {column}='2020-01-01' WHERE ticker='T0'")
    quality = gates.recommendation_quality(warehouse, tenant_id='default', ticker='T0')
    assert f'SYMBOL_{label}_STALE_OR_MISSING' in quality['suppressionReasons']
    assert quality['actionable'] is False
    if table != 'ranking_snapshots':
        row = client.get('/api/recommendations/T0').json()
        assert row['action'] == 'WATCH'
    best = client.get('/api/recommendations/best')
    assert best.status_code == 200
    assert best.json()['ticker'] != 'T0'


@pytest.mark.parametrize('stamp',['not-a-date','2099-01-01'])
def test_invalid_or_future_prediction_is_not_active(warehouse, stamp):
    warehouse.execute('UPDATE predictions SET timestamp=?',(stamp,))
    assert gates.compute_pipeline_signals(warehouse,tenant_id='default').prediction_pipeline_active is False


def test_run_linkage_and_no_cross_tenant_freshness(warehouse):
    warehouse.execute("UPDATE predictions SET run_id='old' WHERE ticker='T0'")
    quality = gates.recommendation_quality(warehouse,tenant_id='default',ticker='T0')
    assert 'SYMBOL_PREDICTION_RUN_MISMATCH' in quality['suppressionReasons']
    assert gates.compute_pipeline_signals(warehouse,tenant_id='other').prediction_pipeline_active is False


def test_health_degradation_and_unknown_health_block(warehouse, monkeypatch):
    for run in (None, {'id':'run','createdAt':datetime.now(timezone.utc).isoformat(),
                      'runStatus':'DEGRADED','degradedReasons':['TEST_DEGRADED']}):
        monkeypatch.setattr(intelligence_read,'get_prediction_run_latest',lambda *a,**kw: run)
        quality = gates.recommendation_quality(warehouse,tenant_id='default')
        assert quality['actionable'] is False
        assert quality['suppressionReasons']


def test_under_price_uses_quote_not_entry_and_never_fills_with_near_misses(warehouse, client):
    rows = client.get('/api/recommendations/under/2').json()['recommendations']
    assert rows
    for row in rows:
        assert row['referencePrice'] <= 2
        assert row['entryZone'] is row['fiftyDollarPotential'] is row['undervaluation'] is None
        assert row['researchScoreMethodology']['isProbability'] is False
        assert row['eligibilityPassed'] is True
    warehouse.execute('UPDATE price_bars SET volume=0')
    assert client.get('/api/recommendations/under/2').json()['recommendations'] == []


def test_long_only_does_not_fallback_to_sell(warehouse, client):
    warehouse.execute('UPDATE ranking_snapshots SET score=-1')
    warehouse.execute('UPDATE consensus_signals SET p_final=-1')
    warehouse.execute("UPDATE candidate_queue SET status='rejected'")
    assert client.get('/api/recommendations/latest?preference=long_only').json()['recommendations'] == []


def test_future_horizon_cannot_hide_stale_run_creation(warehouse):
    warehouse.execute("UPDATE prediction_runs SET created_at='2020-01-01',prediction_end='2099-01-01'")
    assert 'PIPELINE_RUN_STALE_OR_INVALID' in gates.recommendation_quality(warehouse,tenant_id='default')['suppressionReasons']


def test_missing_price_is_not_low_risk(warehouse, client):
    warehouse.execute("UPDATE price_bars SET close=NULL WHERE ticker='T0'")
    quality = gates.recommendation_quality(warehouse,tenant_id='default',ticker='T0')
    assert 'SYMBOL_PRICE_INVALID' in quality['suppressionReasons']
    assert quality['actionable'] is False


@pytest.mark.parametrize('status', ['failed', 'running', 'unknown'])
def test_job_failure_overrides_derived_healthy_status(warehouse, client, status):
    warehouse.execute('UPDATE prediction_jobs SET status=?', (status,))
    body = client.get('/api/recommendations/latest').json()
    assert 'PREDICTION_JOB_NOT_SUCCESSFUL' in body['suppressionReasons']
    assert all(not r['actionable'] for r in body['recommendations'])


def test_missing_job_is_unknown_not_success(warehouse, client):
    warehouse.execute('DELETE FROM prediction_jobs')
    assert client.get('/api/recommendations/best').status_code == 503
    assert 'PREDICTION_JOB_UNAVAILABLE' in client.get('/api/recommendations/latest').json()['suppressionReasons']


def test_stale_intraday_quote_cannot_borrow_daily_price_freshness(warehouse, client):
    warehouse.execute("INSERT INTO price_bars VALUES ('default','T0','1m','2020-01-01',1,1,1,1,10)")
    row = client.get('/api/recommendations/T0').json()
    assert row['action'] == 'WATCH'
    assert 'REFERENCE_QUOTE_STALE_OR_MISSING' in row['suppressionReasons']
    assert row['referencePriceAsOf'] == '2020-01-01'


def test_api_missing_symbol_has_quality_metadata(client):
    body = client.get('/api/recommendations/MISSING').json()
    assert body['found'] is False
    assert body['actionable'] is False
    assert 'qualityStatus' in body


def test_null_consensus_remains_research_only(warehouse, client):
    warehouse.execute("UPDATE consensus_signals SET p_final=NULL WHERE ticker='T0'")
    row = client.get('/api/recommendations/T0').json()
    assert row['action'] == 'WATCH'
    assert 'SYMBOL_CONSENSUS_INVALID' in row['suppressionReasons']


@pytest.mark.parametrize('cap', ['nan','inf','-1'])
def test_invalid_price_cap_is_client_error(client, cap):
    assert client.get(f'/api/recommendations/under/{cap}').status_code == 400


def test_source_selection_uses_time_not_iso_text_order(warehouse):
    # Same UTC date: lexical T ordering would incorrectly win over a newer space timestamp.
    old = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    new = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')
    warehouse.execute('UPDATE ranking_snapshots SET timestamp=?,score=0.1', (old,))
    warehouse.execute('UPDATE consensus_signals SET created_at=?,p_final=0.1', (old,))
    warehouse.execute("INSERT INTO ranking_snapshots VALUES ('default','T0',?,0.9)", (new,))
    warehouse.execute("INSERT INTO consensus_signals VALUES ('default','T0',?,0.9)", (new,))
    assert recs._latest_rankings(warehouse,tenant_id='default')['T0'] == 0.9
    assert recs._latest_consensus(warehouse,tenant_id='default')['T0'] == 0.9
