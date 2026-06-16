# Naming Convention Standard

*Part of the Phase 1 Data Governance Foundation. Subordinate to [ROOT.md](../ROOT.md); if any rule here conflicts with a higher-priority document, the higher-priority document wins.*

This standard defines the **exact** naming rules for every named artifact in `crypto-data-hub`: datasets, dataset IDs, metadata fields, files, directories, and versions. It exists to keep the repository discoverable, machine-parseable, and self-onboarding as the dataset count and contributor count grow.

Authoritative inputs: the `conventions` block and `dataset_entry_schema` in [dataset_registry.json](../dataset_registry.json) are the machine-readable source of truth for the patterns below. Field semantics are defined in [DATA_CONTRACT.md](../DATA_CONTRACT.md) and [docs/metadata_standard.md](metadata_standard.md). This document is the prose specification of those patterns; it must never contradict them.

---

## Naming Style

A single, consistent style applies across the whole repository:

| Artifact | Style | Example |
| --- | --- | --- |
| `dataset_id` | lowercase; dot-separated namespace segments; `snake_case` within each segment | `market.btc_usd.ohlcv_1h` |
| `dataset_name` | human-readable Title Case free text | `BTC/USD OHLCV 1h` |
| Metadata field names | `snake_case` | `earliest_timestamp` |
| Governance / root docs | established fixed names (see [File Naming](#file-naming)) | `ROOT.md` |
| `docs/` files | `snake_case` with `.md` extension | `dataset_lifecycle.md` |
| Data / config files | `snake_case` | `dataset_registry.json` |
| Directories | lowercase `snake_case` short nouns | `scripts`, `reports` |
| Versions | SemVer, `v`-prefixed | `v0.2.0` |

Rule of thumb: **identifiers and filesystem names are lowercase and machine-friendly; only human-facing display text (`dataset_name`) uses Title Case.**

---

## Allowed Characters

| Artifact | Allowed characters | Disallowed |
| --- | --- | --- |
| `dataset_id` | `a`–`z`, `0`–`9`, `.`, `_`, `-` | uppercase, spaces, any other punctuation |
| Metadata field names | `a`–`z`, `0`–`9`, `_` | uppercase, `-`, `.`, spaces |
| `docs/` / data / config filenames | `a`–`z`, `0`–`9`, `_`, `.` (extension) | uppercase (except fixed root docs), spaces, `-` |
| Directory names | `a`–`z`, `0`–`9`, `_` | uppercase, spaces |
| Version strings | `v`, `0`–`9`, `.` | anything else |

Within a `dataset_id`: the **dot (`.`)** separates namespace segments, and **`snake_case`** is used within a segment. No uppercase and no spaces are ever permitted in a `dataset_id`.

---

## Dataset Naming

`dataset_name` is the human-readable label shown in [DATA_CATALOG.md](../DATA_CATALOG.md) and tooling.

- **Style:** Title Case, free text.
- **Purpose:** readability for humans; it is *not* a key and is *never* used for lookup.
- Keep it descriptive and unambiguous; it should map clearly to its `dataset_id`.

| `dataset_id` | `dataset_name` |
| --- | --- |
| `market.btc_usd.ohlcv_1h` | `BTC/USD OHLCV 1h` |
| `onchain.eth.gas_daily` | `Ethereum Daily Gas` |

---

## Dataset ID Naming

`dataset_id` is the **unique discovery key** for the registry (see `dataset_entry_schema.dataset_id` in `dataset_registry.json`). It is the single most important name in the platform.

- **Pattern (authoritative):** `^[a-z0-9]+(?:[._-][a-z0-9]+)*$`
- **Recommended form:** `<domain>.<entity>.<granularity>` — e.g. `market.btc_usd.ohlcv_1h`
- **Structure:** dot separates namespace segments; `snake_case` within each segment.
- **Case:** lowercase only; no uppercase, no spaces.

### Valid examples

| `dataset_id` | Why it is valid |
| --- | --- |
| `market.btc_usd.ohlcv_1h` | follows `<domain>.<entity>.<granularity>`; lowercase; `snake_case` segments |
| `onchain.eth.gas_daily` | lowercase, dot-separated, valid characters |
| `derived.funding_rate.binance_perp` | `snake_case` within segments; allowed `_` |

### Invalid examples

| Candidate | Why it is invalid |
| --- | --- |
| `Market.BTC_USD.OHLCV_1h` | contains uppercase |
| `market.btc usd.ohlcv_1h` | contains a space |
| `.market.btc_usd` | leading separator; fails the pattern |
| `market..btc_usd` | empty segment between dots |
| `market/btc_usd/ohlcv_1h` | `/` is not an allowed character |

---

## Metadata Naming

Metadata field names are the keys of each registry entry, defined exactly by `dataset_entry_schema` in [dataset_registry.json](../dataset_registry.json) and described in [docs/metadata_standard.md](metadata_standard.md).

- **Style:** `snake_case`.
- Field names are **fixed by the schema** — do not invent, rename, alias, or re-case fields (e.g. it is `latest_timestamp`, never `latestTimestamp` or `last_ts`).
- Nested object fields (e.g. `source.type`, `provenance.checksum`, `lineage.upstream`) follow the same `snake_case` rule.

---

## File Naming

File names fall into two classes:

1. **Top-level governance / root docs — keep their established names** (do not rename or re-case):
   `ROOT.md`, `AGENTS.md`, `HANDOFF.md`, `README.md`, `QUICKSTART.md`, `CHANGELOG.md`, `DATA_CATALOG.md`, `DATA_CONTRACT.md`.

2. **`docs/` documentation files — `snake_case` with a `.md` extension:**
   e.g. `dataset_lifecycle.md`, `metadata_standard.md`, `naming_convention.md`.

3. **Data and config files — `snake_case`:**
   e.g. `dataset_registry.json`.

---

## Directory Naming

- **Style:** lowercase `snake_case`, short nouns.
- Examples: `datahub`, `scripts`, `tests`, `reports`, `examples`, `logs`, `docs`.
- No uppercase, no spaces, no plural/singular churn — pick the established name and reuse it.

---

## Version Naming

- **Scheme:** Semantic Versioning, `v`-prefixed: `vMAJOR.MINOR.PATCH`.
- **Pattern (authoritative):** `^v[0-9]+\.[0-9]+\.[0-9]+$`
- The current repo version is `v0.2.0`.

Dataset version increments (see [DATA_CONTRACT.md](../DATA_CONTRACT.md)):

| Increment | Meaning |
| --- | --- |
| **PATCH** | data correction (no schema change) |
| **MINOR** | backward-compatible schema addition |
| **MAJOR** | breaking schema change |

`registry_version` bumps only when the registry **contract shape** changes, independent of dataset versions.

Valid: `v0.2.0`, `v1.0.0`, `v12.4.7`.
Invalid: `0.2.0` (missing `v`), `v0.2` (not three parts), `v1.0.0-beta` (suffix not allowed).

---

## Naming Consistency Rules

- **Identifiers are stable.** Once a `dataset_id` becomes `active`, it is **never renamed**. To change a name or break compatibility, **deprecate** the existing id and **create a new id** instead (see [docs/dataset_lifecycle.md](dataset_lifecycle.md)).
- **One id, one dataset.** `dataset_id` is unique across the registry; reuse of a retired id is forbidden.
- **Registry-truth.** Names in [DATA_CATALOG.md](../DATA_CATALOG.md) and all tooling must match the registry entry exactly; the registry is authoritative, the catalog is derived.
- **Status follows truth.** Naming changes never substitute for lifecycle transitions; renaming is not a state change.
- **No silent re-casing.** Field names, file names, and directory names are matched exactly as written here — casing differences are errors, not stylistic choices.
