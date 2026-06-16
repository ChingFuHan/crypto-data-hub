# Universe Metadata Sources

> Phase 4 Source Authority Review. Subordinate to [ROOT.md](../ROOT.md) and
> [DATA_CONTRACT.md](../DATA_CONTRACT.md). If this document conflicts with a
> higher-priority artifact, the higher-priority artifact wins.

---

## Approved Sources

### Binance USD-M Futures exchangeInfo

- **Source identifier:** `https://fapi.binance.com/fapi/v1/exchangeInfo`
- **Source type:** public API
- **Phase 4 role:** primary implementation source
- **Purpose:** current Binance USD-M Futures tradable universe and contract
  metadata.
- **Refresh strategy:** fetch a new immutable raw snapshot when current source
  content changes; offline mode reuses committed raw snapshot.
- **Authority:** authoritative for current symbols returned by the endpoint with
  `status = TRADING`.
- **Limitations:** not authoritative for full historical delist, rename, merge,
  or symbol lifecycle graph reconstruction.

### Binance Data Vision / Public Archive Index

- **Source identifier:** `https://data.binance.vision/`
- **Source type:** public archive index
- **Phase 4 role:** optional exploratory source only.
- **Purpose:** discover historical symbol candidates.
- **Authority:** candidate evidence only unless cross-verified by stronger
  lifecycle evidence.
- **Limitations:** archive path existence does not prove listing time, delisting
  time, rename, merge, or current lifecycle state.

### Binance Official Announcements

- **Source identifier:** `https://www.binance.com/en/support/announcement`
- **Source type:** public documentation / announcements
- **Phase 4 role:** documentation and future evidence source.
- **Purpose:** future support for delist, rename, merge, and lifecycle event
  confirmation.
- **Authority:** useful event evidence, but not implemented as structured
  ingestion in Phase 4.
- **Limitations:** requires parsing, event typing, and reconciliation before it
  can drive registry-grade lifecycle rows.

---

## Field Coverage

Phase 4 implementation source = Binance USD-M Futures `exchangeInfo`.

| Field | Source / Method | Confidence | Notes |
| --- | --- | --- | --- |
| `instrument_id` | derived from exchange, market_type, symbol, listed_at | high | Format documented below. |
| `symbol` | `exchangeInfo.symbol` | high | Current source ticker. |
| `exchange` | ingestion constant `binance` | high | Venue-level normalization. |
| `base_asset` | `exchangeInfo.baseAsset` | high | Nullable if source omits. |
| `quote_asset` | `exchangeInfo.quoteAsset` | high | Nullable if source omits. |
| `market_type` | `exchangeInfo.contractType` | high | `PERPETUAL` → `perpetual`; other contract types → `futures`. |
| `contract_type` | USD-M Futures product class | high | Normalized as `linear`. |
| `status` | `exchangeInfo.status == TRADING` | high | Phase 4 emits only `active` rows. |
| `listed_at` | `exchangeInfo.onboardDate` | high | UTC ISO 8601. |
| `delisted_at` | null for current active coverage | high | Historical events not ingested. |
| `successor_id` | null for current active coverage | medium | Rename/merge graph not present in exchangeInfo. |
| `tick_size` | `PRICE_FILTER.tickSize` | high | Nullable if source omits. |
| `step_size` | `LOT_SIZE.stepSize` | high | Nullable if source omits. |
| `contract_size` | normalization convention | medium | Set to `1` for USD-M linear futures because exchangeInfo lacks separate contract-size field. |

---

## Coverage Status

Coverage status is separate from the Universe Metadata row `status`.

- `active_current` — implemented. Current Binance USD-M Futures symbols with
  source `status = TRADING`.
- `historical_candidate` — recognized concept, not implemented in artifact rows.
- `unresolved` — tracked in manifest coverage notes.
- `not_supported_yet` — full historical delist/rename/merge lifecycle.

Phase 4 row `status` values remain contract values. The MVP artifact emits only
`active` rows. Coverage lives in the manifest, not in row `status`.

---

## Instrument ID Decision

Human-readable deterministic format:

```text
binance.usd_m_futures.<market_type>.<symbol_lower>.<listed_yyyymmdd>
```

Example:

```text
binance.usd_m_futures.perpetual.btcusdt.20190908
```

Hash collision handling:

- If a duplicate id is generated, append `.h<sha256_prefix>` using a stable hash
  of base id, symbol, listed_at, and collision ordinal.
- Collision suffixing is deterministic and reproducible.

Rationale:

- Includes exchange, product family, market type, symbol, and symbol-era start.
- Avoids plain symbol as primary key.
- Stable across repeated ingestion from the same raw snapshot.
- Supports future symbol-era extension.

Known limitation:

- `listed_at` depends on `exchangeInfo.onboardDate`. If future sources prove a
  different authoritative listing instant, that is a dataset data correction.

---

## Refresh Strategy

Online:

```bash
python -m datahub.ingestion.universe_metadata --all
```

- Fetches current `exchangeInfo`.
- Computes raw response checksum.
- Reuses existing raw snapshot if checksum already exists.
- Creates a new immutable raw snapshot only when source content changes.
- Regenerates normalized artifact and manifest.

Offline:

```bash
python -m datahub.ingestion.universe_metadata --offline --all
```

- Uses committed raw snapshot from manifest or latest raw snapshot.
- Regenerates normalized artifact and manifest deterministically.
- Does not create diffs when inputs are unchanged.

---

## Raw Snapshot Policy

- Raw snapshots are immutable envelope JSON files.
- File name format:
  `exchange_info_<retrieved_at_utc>_<sha256_prefix>.json`.
- Existing raw snapshots are never overwritten.
- Online fetch computes the raw response checksum first.
- If the same checksum already exists, the existing snapshot is reused instead
  of writing a duplicate.
- If source content changes, a new raw snapshot is created.
- The stable manifest points to the raw snapshot selected for the committed
  draft artifact.

---

## Data Artifact Commit Policy

Phase 4 commits small reference artifacts because they are reviewable and make
offline validation reproducible:

- raw `exchangeInfo` snapshot
- normalized Universe Metadata draft artifact
- manifest / checksum metadata
- ingestion fixtures

Large OHLCV, funding, open interest, order book, or other market data files are
out of scope and must not be committed under this policy.

---

## Idempotency And Final Verification

Offline verification command:

```bash
python -m datahub.ingestion.universe_metadata --offline --all
```

Repeated offline runs with the committed raw snapshot must preserve:

- normalized artifact checksum
- manifest checksum
- committed artifact content

Final pre-commit verification:

```bash
python -m datahub.ingestion.universe_metadata --offline --all
python -m datahub.validation --all
python -m unittest discover tests
```

---

## Known Gaps

- Only Binance USD-M Futures current active universe is covered.
- Historical delisted symbols are not confirmed.
- Rename and merge events are not ingested.
- Archive index candidates are not promoted into authoritative rows.
- Announcement parsing is future work.
- Contract-size source remains a normalization convention, not a direct
  exchangeInfo field.

---

## Unresolved Source Questions

- Which sources should become authoritative for historical listings and delists?
- How should announcement evidence be parsed and reconciled with archive data?
- Should future artifacts include source-level confidence per row, or keep
  confidence only in manifest/provenance?
