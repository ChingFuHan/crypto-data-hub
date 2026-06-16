# Dataset Registry Standard

> Part of the Phase 1 Data Governance Foundation. Subordinate to [ROOT.md](../ROOT.md); if any rule here conflicts with ROOT.md, ROOT.md wins.

This document defines the structure, versioning, and discovery rules for the dataset registry. The registry is the **single source of truth**: a dataset is not real until it is registered. The machine-readable contract lives in [dataset_registry.json](../dataset_registry.json); this document is its prose specification. Per-field metadata semantics are defined in [docs/metadata_standard.md](metadata_standard.md); per-dataset schema and quality rules are authoritative in [DATA_CONTRACT.md](../DATA_CONTRACT.md). The human-readable view is [DATA_CATALOG.md](../DATA_CATALOG.md), which is **derived** from the registry and never authoritative.

Authority order (highest first): ROOT.md > DATA_CONTRACT.md > dataset_registry.json > DATA_CATALOG.md.

---

## The Registry as a Machine-Readable Contract

[dataset_registry.json](../dataset_registry.json) is not merely a list of datasets — it is a self-describing contract. Two blocks define the rules that every entry must obey:

- **`conventions`** — the shared rules (id pattern, version pattern, timestamp format, lifecycle/status vocabulary) that apply across all entries.
- **`dataset_entry_schema`** — the field-by-field schema that each member of `datasets[]` must satisfy (field name, type, whether required, and the field's semantics).

Tools and agents read these two blocks to validate entries and to discover what a valid dataset record looks like. The schema in the file is canonical; the descriptions below explain it but must not contradict it.

---

## Registry Structure (Top-Level Keys)

The registry file is a single JSON object with the following top-level keys:

| Key | Type | Purpose |
|-----|------|---------|
| `registry_version` | string (SemVer, v-prefixed) | Version of the registry **contract shape** itself. Currently `v0.2.0`. |
| `description` | string | One-line statement of what the registry is and its authority. |
| `updated_at` | string | Date the registry file was last updated. |
| `conventions` | object | Shared rules referenced by all entries (see below). |
| `dataset_entry_schema` | object | Field schema every `datasets[]` entry must satisfy. |
| `datasets` | array | The registered dataset entries. **Currently empty (`[]`)** — 0 datasets registered. |

### `conventions` block (actual current keys)

| Key | Value |
|-----|-------|
| `dataset_id_pattern` | `^[a-z0-9]+(?:[._-][a-z0-9]+)*$` |
| `dataset_id_recommended_form` | `<domain>.<entity>.<granularity>` e.g. `market.btc_usd.ohlcv_1h` |
| `version_pattern` | `^v[0-9]+\.[0-9]+\.[0-9]+$` |
| `timestamp_format` | ISO 8601 with timezone offset; stored in UTC |
| `default_timezone` | `UTC` |
| `lifecycle_states` | `["draft", "active", "deprecated", "archived"]` |
| `status_enum` | `["draft", "active", "deprecated", "archived"]` |

---

## Dataset Entry Structure

Each element of `datasets[]` is an object validated against `dataset_entry_schema`. The `dataset_id` is the unique discovery key. Field names are `snake_case`.

**The authoritative, prose definition of every field — its meaning, type vocabulary, and null policy — is [docs/metadata_standard.md](metadata_standard.md).** This document does not redefine those fields; it defines the container they live in. For convenience, the entry schema currently declares the following fields:

- **Required:** `dataset_id`, `dataset_name`, `description`, `version`, `status`, `owner`, `source` (`{type, reference}`), `timezone`, `update_frequency`, `schema_ref`, `primary_key`, `provenance` (`{code_version, params, generated_by, checksum}`), `quality` (`{contract_validated, last_validated_at}`), `created_at`, `updated_at`.
- **Conditionally required:** `earliest_timestamp`, `latest_timestamp` — required for time-series datasets, `null` otherwise.
- **Optional:** `lineage` (`{upstream[], transformation}`), `snapshot` (`{snapshot_id, created_at, checksum, immutable}`), `tags[]`.

Field type vocabulary used by the schema: `string`, `enum`, `object`, `array<string>`, `string|null`, `boolean`.

An entry is added to `datasets[]` **only after** DATA_CONTRACT validation passes (`quality.contract_validated = true`). Validation precedes registration; failure is loud, never patched silently. Whoever updates the registry MUST update [DATA_CATALOG.md](../DATA_CATALOG.md) in the same change.

---

## Versioning Rules

Two independent versions exist. Do not conflate them.

### Dataset version (`version` field, per entry)

Each dataset carries its own SemVer string matching `version_pattern` (`^v[0-9]+\.[0-9]+\.[0-9]+$`):

| Bump | Trigger | Compatibility |
|------|---------|---------------|
| **PATCH** (`v1.0.0` → `v1.0.1`) | Data correction — values fixed, no schema change. | Backward compatible. |
| **MINOR** (`v1.0.0` → `v1.1.0`) | Backward-compatible **schema addition** (e.g. a new nullable field). | Backward compatible; existing consumers unaffected. |
| **MAJOR** (`v1.0.0` → `v2.0.0`) | **Breaking** schema change (field removed/renamed/retyped, PK change). | Not backward compatible. |

A published snapshot stays bound to the contract version it passed (see [DATA_CONTRACT.md](../DATA_CONTRACT.md)). An active `dataset_id` is stable and is **never renamed** — deprecate it and create a new id instead.

### Registry version (`registry_version`, top-level)

`registry_version` versions the registry **contract shape** — the structure of `conventions` and `dataset_entry_schema` — not the datasets inside it. Bump it only when the registry's contract changes (e.g. adding or retyping an entry-schema field). Current value: `v0.2.0`.

---

## Dataset Discovery Rules

The registry is the index that tools query.

- **Primary discovery key:** `dataset_id` — unique across the registry; the canonical handle for every dataset.
- **Filterable dimensions:** `status`, `owner`, `tags[]`. For example, list all `active` datasets owned by a team, or all datasets tagged `ohlcv`.
- **Lineage traversal:** `lineage.upstream` lists upstream `dataset_id`s, enabling dependency and impact analysis across registered datasets.
- **Catalog rule:** [DATA_CATALOG.md](../DATA_CATALOG.md) must never list an unregistered dataset. If it appears in the catalog, it exists in the registry.

Discovery operates only over entries present in `datasets[]`. As the registry currently holds 0 datasets, all queries return empty until the first entry is registered.
