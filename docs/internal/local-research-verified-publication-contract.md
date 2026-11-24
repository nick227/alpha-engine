# Alpha Engine — Local Research → Verified Publication → Hosted Consumption Contract

**Status:** Canonical Architectural Contract  
**Applies to:** Alpha Engine (Python) & TradeLoom (Node/React)  
**Security & Verification Baseline:** Ed25519 Signed Publications (`research-publication/v1`), Deterministic Canonical Fingerprints (`bar-sync/v1`)  

---

## 1. Context & Architectural Principles

We explicitly reject cloud data lakes, distributed warehouse clusters, and hosted volume upgrades during POC development. Instead, we establish a clean, durable **division of responsibility**:

* **Localhost** is our **long-term historical data warehouse** and research laboratory. It holds multi-decade price histories, large machine learning datasets, experimental parameter sweeps, and raw research artifacts without storage or compute constraints.
* **Railway** is our **lightweight, production-facing operational engine**. It runs continuously, fast, and inexpensively on standard container volumes, holding only active market data, current published research, and active bot trading state.

```mermaid
flowchart TD
    subgraph Localhost["Localhost (Historical Research Warehouse)"]
        L_Bars["Decades of Historical Bars<br/>(1962–Present, Full Universe)"]
        L_ML["ML Training Matrices & Payloads<br/>(ml_train, feature sweeps)"]
        L_Exp["Research Experiments & Backtests"]
        L_Worker["Local Research Worker<br/>(Holds Ed25519 Private Key)"]
    end

    subgraph Channel["Verified Publication Channel (HTTPS)"]
        Sync["GET /api/sync/bars<br/>(Reconcile Active Symbols)"]
        Pub["POST /api/research/publications<br/>(Ed25519 Signed Payload)"]
    end

    subgraph Railway["Railway (Lightweight Operational Engine)"]
        R_DB["Compact alpha.db (< 1 GB)<br/>(Active Universe 5Y Bars, Published Evidence)"]
        R_API["Alpha Engine API (:8090)<br/>(Independent Re-derivation & Gate Verifier)"]
        R_TradeLoom["TradeLoom Staging / App (:3001)<br/>(Bots, Paper Trading, Public Strategies)"]
    end

    L_Bars --> L_Worker
    L_ML --> L_Worker
    L_Exp --> L_Worker

    R_API -->|Bar Sync| Sync --> L_Worker
    L_Worker -->|Signed Publication| Pub --> R_API

    R_API --> R_DB
    R_DB --> R_API
    R_API --> R_TradeLoom
```

---

## 2. Division of Responsibility

| Data Category | Localhost (Research Archive) | Railway (Hosted Operational Engine) |
|---|---|---|
| **Market Price Bars** | **Complete historical archive** (all symbols, intraday, decades back to 1962). | **Supported universe only** (10–150 symbols), 5-year daily history for active backtests and paper trading. |
| **Machine Learning Datasets** | **Full retention** (`ml_train`, `ml_learning_rows`, raw features). | **Excluded entirely** (0 MB on hosted volume). |
| **Feature Engineering History** | **Full retention** across historical experiments and universes. | **Only operationally active features** (current universe `feature_snapshot`). |
| **Research Experiments** | **Complete archive** of exploratory hypotheses, notebooks, and model runs. | **Current published research state** (`research_publications` table). |
| **Predictions & Rankings** | **Full historical telemetry** and prediction scoring records. | **Current and recent active records** needed for platform consumers. |
| **Platform Backtest Results** | **Optional historical copies** and variant exploration logs. | **Published version evidence** (persisted in TradeLoom `StrategyEvidence`). |
| **User Bots & Paper Executions** | Optional staging mirror / test fixtures. | **Authoritative live database** (TradeLoom MySQL / Prisma). |

---

## 3. The Three Operating Rules

### Rule 1: Localhost Retains the Long Tail
Historical datasets, feature matrices, ML training samples, and exploratory experiments can grow indefinitely on local developer disks without consuming Railway volume. Local storage is authoritative for model training and hypothesis testing.

### Rule 2: Railway Remains Independently Operational
The hosted platform must be fully self-sufficient. Daily paper trading, published strategy evidence, public strategy catalog viewing, and 5-year user backtests in TradeLoom must continue functioning with zero degradation **even when your local machine is completely offline or asleep**.

### Rule 3: Publication is Selective, Not Database Synchronization
We do **not** run SQLite database replication, cross-network file copies, or rsync scripts. Only validated datasets, strategy versions, and cryptographic evidence needed by the hosted application are published to Railway.

> [!IMPORTANT]
> **Durability Note:** While localhost is the primary historical warehouse, local disks fail. A secondary offsite backup (e.g. periodic archive copy of `alpha.db.backup_full` onto external media) is required for disaster recovery.

---

## 4. The Formal Publication Protocol

This contract reuses the existing `research-publication/v1` and `bar-sync/v1` implementations in [ae-research/app/research_publication/contract.py](file:///home/administrator/web/alpha-trading-platform/ae-research/app/research_publication/contract.py):

```mermaid
sequenceDiagram
    autonumber
    participant Local as Local Research Worker
    participant Railway as Railway Alpha Engine
    participant TradeLoom as TradeLoom Consumer

    Note over Local,Railway: Phase 1: Bar Reconciliation
    Local->>Railway: GET /api/sync/bars?wanted=SPY,QQQ... (sessions=604)
    Railway-->>Local: Return canonical session bars + sha256 fingerprints
    Local->>Local: Assert local market bars match Railway bar-for-bar

    Note over Local: Phase 2: Local Strategy Evaluation & Evidence Derivation
    Local->>Local: Run strategy, compute 5Y/1Y/holdout metrics, out-of-sample edge
    Local->>Local: Format canonical payload (version, params, metrics, input hashes)

    Note over Local,Railway: Phase 3: Signed Publication
    Local->>Local: Sign payload with private Ed25519 key (never leaves local)
    Local->>Railway: POST /api/research/publications (Headers: x-research-key-id, signature)

    Note over Railway: Phase 4: Independent Verification & Acceptance
    Railway->>Railway: Verify Ed25519 signature against ALPHA_PUBLISHER_KEYS
    Railway->>Railway: Re-derive strategy from internal bars (zero blind trust)
    Railway->>Railway: Evaluate gates (genuineOutOfSample, holdout size >= 250)
    Railway->>Railway: Store in research_publications (status: 'accepted')

    Note over Railway,TradeLoom: Phase 5: Hosted Consumption
    TradeLoom->>Railway: GET /api/public/strategies or /api/research/publications/:id
    Railway-->>TradeLoom: Verified version evidence & model scorecard
    TradeLoom->>TradeLoom: Serve public strategy detail & backtest starting points
```

### Request Signing Specification
Requests to `POST /api/research/publications` must supply:
* `x-research-key-id`: The identifier of the publisher key.
* `x-research-timestamp`: Current Unix timestamp in seconds (rejects clock skew $> 300\text{s}$).
* `x-research-signature`: Base64-encoded Ed25519 signature over:
  $$\text{METHOD} \parallel \text{"\textbackslash n"} \parallel \text{PATH?QUERY} \parallel \text{"\textbackslash n"} \parallel \text{TIMESTAMP} \parallel \text{"\textbackslash n"} \parallel \text{SHA256}(\text{body})$$

Railway holds **only public keys** in `ALPHA_PUBLISHER_KEYS`. Compromising the hosted environment cannot result in forged research publications.

### Independent Railway Verification (Zero-Trust Principle)
Railway treats worker claims strictly as hypotheses:
1. **Provenance:** Railway verifies that the worker's input bar fingerprints equal Railway's internal bars.
2. **Freshness:** Input bars must terminate on Railway's latest session close.
3. **Deterministic Reproduction:** Railway executes the registered strategy definition internally; output decisions and metrics must match the worker's payload byte-for-byte.
4. **Out-of-Sample Quality Gate:** Holdout window must be strictly out-of-sample (ends post development cutoff) with at least 250 sessions.

---

## 5. Storage Governance Policy

1. **Volume Ceiling Target:** Railway `alpha.db` shall remain **$< 1.5\text{ GB}$**, ensuring at least **$3.5\text{ GB}$ free** on the 5 GB plan.
2. **No Bulk Raw Exports on Hosted Volume:** Experimental tenant rows (like `ml_train`) are barred from Railway.
3. **No In-Place Vacuum on Railway:** When compaction is needed in the future, it must be performed externally or into temporary scratch space, never in-place on a full volume.
4. **TradeLoom Retention:** TradeLoom stores user bots, order executions, and published evidence snapshots in MySQL, completely independent of Alpha Engine's operational footprint.

---

## 6. Implementation Checklist & Status

- [x] **Compact Database Deployed:** `alpha.db` reduced from 5.74 GB to **788 MB** (86.3% reduction).
- [x] **Local Full Archive Secured:** Full 5.74 GB database backed up at `alpha-engine/backups_pre_compact/alpha.db.backup_20261124_full`.
- [x] **`scanner.py` Fixed:** Obsolete `ml_train` dependency updated to authoritative `default` market data.
- [x] **Wire Contract Operational:** Signed publication contract (`app.research_publication.contract`) and verification routes (`/api/sync/bars`, `/api/research/publications`) active and verified on port 8090.
- [x] **TradeLoom Integration Healthy:** Public strategies, 5-year backtests, and daily paper simulation running smoothly without hosted storage pressure.
