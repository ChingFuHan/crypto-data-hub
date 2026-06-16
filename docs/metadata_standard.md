# Dataset Metadata Standard

> Part of the Phase 1 Data Governance Foundation. Subordinate to [ROOT.md](../ROOT.md); if any rule here conflicts with a higher-priority document, the higher-priority document wins.

This document defines the **metadata standard** for every dataset in crypto-data-hub: which fields exist, their types, whether they are required, and what they mean. It is the prose companion to the machine-readable `dataset_entry_schema` in [dataset_registry.json](../dataset_registry.json).

## Where Metadata Lives

Dataset metadata is **not** a separate file. It lives **inside the entries of** [dataset_registry.json](../dataset_registry.json) — each object in the `datasets[]` array is the complete, authoritative metadata record for one dataset. The registry is the single source of truth; [DATA_CATALOG.md](../DATA_CATALOG.md) is only a human-readable view derived from it and is never authoritative.

Every field below MUST match the `dataset_entry_schema` in the registry exactly (same names, same types). Field rules for schema and quality validation are defined in [DATA_CONTRACT.md](../DATA_CONTRACT.md); lifecycle states (`status`) are defined in [docs/dataset_lifecycle.md](dataset_lifecycle.md).

## Conventions

- All field names are `snake_case`.
- All timestamps are stored in **UTC**, **ISO 8601** with offset.
- `version` follows SemVer, v-prefixed: `^v[0-9]+\.[0-9]+\.[0-9]+$`.
- `dataset_id` follows `^[a-z0-9]+(?:[._-][a-z0-9]+)*$`, recommended form `<domain>.<entity>.<granularity>` (e.g. `market.btc_usd.ohlcv_1h`).
- Field type vocabulary: `string`, `enum`, `object`, `array<string>`, `string|null`, `boolean`.

## Field Reference

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `dataset_id` | string | required | Stable, unique identifier. Matches the `dataset_id` pattern. Once a dataset is `active`, the id is never renamed — deprecate it and create a new id instead. Primary discovery key. |
| `dataset_name` | string | required | Human-readable name in Title Case. |
| `description` | string | required | One-line summary of dataset contents. |
| `version` | string | required | Dataset semantic version (v-prefixed SemVer). PATCH = data correction, MINOR = backward-compatible schema addition, MAJOR = breaking schema change. |
| `status` | enum | required | Lifecycle state: one of `draft`, `active`, `deprecated`, `archived`. MUST equal the dataset's true lifecycle state. See [docs/dataset_lifecycle.md](dataset_lifecycle.md). |
| `owner` | string | required | Responsible team or agent. |
| `source` | object | required | Where the data originates. Object with `type` (one of `api`, `file`, `onchain`, `derived`) and `reference` (endpoint, path, or upstream `dataset_id`). |
| `timezone` | string | required | IANA timezone of source semantics, e.g. `UTC`. Storage is always UTC; this records the source's timezone. |
| `update_frequency` | string | required | Cadence, e.g. `realtime`, `1h`, `daily`, `weekly`, `manual`. |
| `schema_ref` | string | required | Pointer to this dataset's schema section in [DATA_CONTRACT.md](../DATA_CONTRACT.md). |
| `primary_key` | array&lt;string&gt; | required | Field(s) forming the unique, non-null primary key (at least one field). |
| `provenance` | object | required | Information sufficient to reproduce the dataset. See [Provenance](#provenance). |
| `quality` | object | required | Data-quality validation state. Object with `contract_validated` (boolean) and `last_validated_at` (ISO 8601 string). |
| `created_at` | string | required | ISO 8601 timestamp of registry entry creation. |
| `updated_at` | string | required | ISO 8601 timestamp of last registry entry update. |
| `earliest_timestamp` | string&#124;null | conditional | Earliest record timestamp (ISO 8601). **Required for time-series datasets**; `null` otherwise. |
| `latest_timestamp` | string&#124;null | conditional | Latest record timestamp (ISO 8601). **Required for time-series datasets**; `null` otherwise. |
| `lineage` | object | optional | Upstream relationships and derivation. See [Data Lineage](#data-lineage). |
| `snapshot` | object&#124;null | optional | Set when a snapshot is published; immutable thereafter. Object with `snapshot_id`, `created_at`, `checksum`, `immutable`. |
| `tags` | array&lt;string&gt; | optional | Optional discovery tags. The registry is filterable by `tags`. |

## Data Lineage

**Data Lineage** describes *where a dataset comes from in terms of other registered datasets, and how it was derived*. It is recorded in the optional `lineage` object:

| Subfield | Type | Description |
|----------|------|-------------|
| `lineage.upstream` | array&lt;string&gt; | List of upstream `dataset_id`s this dataset is derived from. Each value MUST reference an already-registered dataset. |
| `lineage.transformation` | string | Description of, or code reference to, how the upstream data was transformed into this dataset. |

Lineage answers the question *"what did this come from?"*. It is distinct from provenance, which answers *"how do I reproduce this exact output?"*.

## Provenance

**Provenance** is the set of facts sufficient to **reproduce** the dataset. It is recorded in the required `provenance` object. Provenance is mandatory by [ROOT.md](../ROOT.md): every dataset records its source, generation code version, and parameters.

| Subfield | Type | Description |
|----------|------|-------------|
| `provenance.code_version` | string | Repo version (v-prefixed SemVer) that generated the data. |
| `provenance.params` | object | Parameters needed to reproduce the dataset. |
| `provenance.generated_by` | string | Process or agent that produced the data. |
| `provenance.checksum` | string | Content hash of the produced data. |

## Upstream Relationship

An **Upstream Relationship** is a reference, via `lineage.upstream`, from this dataset to one or more other **registered** `dataset_id`s that it is built from. Rules:

- Every value in `lineage.upstream` MUST be a `dataset_id` that already exists in [dataset_registry.json](../dataset_registry.json). The catalog and registry must never reference an unregistered dataset.
- Upstream relationships form the dependency graph used for lineage tracing and impact analysis (e.g. when an upstream dataset is `deprecated`).
- When `source.type` is `derived`, the `source.reference` and `lineage.upstream` together identify the originating dataset(s).

## Cross-References

- [ROOT.md](../ROOT.md) — supreme governance document.
- [DATA_CONTRACT.md](../DATA_CONTRACT.md) — authoritative per-dataset schema and quality rules; `schema_ref` points into it.
- [dataset_registry.json](../dataset_registry.json) — authoritative store where these metadata records physically live (`dataset_entry_schema`).
- [DATA_CATALOG.md](../DATA_CATALOG.md) — derived human-readable view; never authoritative.
- [docs/dataset_lifecycle.md](dataset_lifecycle.md) — definition of the `status` lifecycle states and transitions.
