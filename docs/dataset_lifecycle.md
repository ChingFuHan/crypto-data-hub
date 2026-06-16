# Dataset Lifecycle Model

> Part of the Phase 1 Data Governance Foundation. Subordinate to [ROOT.md](../ROOT.md); if anything here conflicts with ROOT.md, ROOT.md wins.

This document defines the canonical lifecycle of every dataset in crypto-data-hub: the states a dataset may occupy, the transitions allowed between them, and the principles governing how state changes. It is the authoritative reference for the meaning of the `status` field in [dataset_registry.json](../dataset_registry.json).

A dataset's lifecycle state is recorded **only** in the `status` field of its registry entry. The allowed values are exactly: `draft`, `active`, `deprecated`, `archived` (see `conventions.lifecycle_states` and the `status` enum in the registry). No other state names exist.

---

## 1. Lifecycle States

| State | Meaning | Mutability | Trust |
|-------|---------|------------|-------|
| `draft` | Registered, schema defined, but **not yet** contract-validated or published. | Mutable | Not trusted |
| `active` | Contract-validated, published, and in use. | Effectively frozen except via versioned change | Authoritative |
| `deprecated` | Superseded or flagged; still readable, but no new consumers. | Frozen | Legacy / discouraged |
| `archived` | Frozen and retained for history and reproducibility. Terminal. | Immutable | Historical only |

**`draft`** — The dataset exists in the registry and has a defined schema, but it has **not** passed [DATA_CONTRACT.md](../DATA_CONTRACT.md) validation and has not been published. A draft is the only freely mutable state: its schema, parameters, and data may change while it is being developed. Draft data MUST NOT be relied upon as authoritative.

**`active`** — The dataset has passed contract validation (`quality.contract_validated = true`, with `quality.last_validated_at` recorded) and is published and in use. This is the trusted, authoritative state. Active data is changed only through the versioned publishing process; it is not silently rewritten in place.

**`deprecated`** — The dataset has been superseded by a newer dataset or version, or flagged for a quality or policy reason. It remains readable so existing consumers are not broken, but **no new consumers should adopt it**. Deprecation is a signal to migrate.

**`archived`** — The dataset is frozen and retained solely for history and reproducibility. This is a **terminal** state: a dataset does not progress further through the lifecycle. Archived entries support the Reproducibility principle by preserving the record of what once existed.

Immutability **increases monotonically** along the lifecycle: `draft` (mutable) → `active` (versioned change only) → `deprecated` (frozen) → `archived` (immutable, terminal).

---

## 2. State Transition Rules

The lifecycle is **forward-only by default**. Only the following transitions are permitted. **Any transition not listed below is forbidden.**

| From | To | Condition |
|------|----|-----------|
| `draft` | `active` | Passes [DATA_CONTRACT.md](../DATA_CONTRACT.md) validation **and** review. |
| `draft` | `archived` | Draft abandoned. |
| `active` | `deprecated` | Superseded, quality issue, or policy decision. |
| `deprecated` | `active` | Reinstated. Rare; **MUST be logged**. |
| `deprecated` | `archived` | Retention period elapsed; dataset frozen. |
| `archived` | — | Terminal. Exit only via an explicit, logged governance restore. |

### Transition diagram

```
                 validate + review
        draft ──────────────────────► active
          │                            │  ▲
          │ abandoned                  │  │ reinstated
          │                 superseded │  │ (rare, logged)
          ▼                  / policy  ▼  │
       archived ◄──────────────── deprecated
          ▲       retention elapsed
          │
          └─ (terminal; restore only by explicit, logged governance action)
```

Forbidden transitions include — but are not limited to — `draft → deprecated`, `active → archived` (an active dataset must be deprecated first), `active → draft`, and `archived → active`/`deprecated`/`draft` except by the explicit, logged governance restore noted above. A dataset never silently reverts to an earlier, more mutable state.

---

## 3. State Management Principles

- **Forward-only by default.** The lifecycle advances toward greater immutability. Backward moves (`deprecated → active`, or any restore out of `archived`) are exceptional, rare, and **must be logged**.
- **Every transition is logged.** No state change is silent. State history is append-only, consistent with the immutability-of-records rule in [ROOT.md](../ROOT.md).
- **Immutability increases along the lifecycle.** Once a dataset is active, its published data changes only via a new version; once deprecated it is frozen; once archived it is immutable.
- **Only the authoritative governance process changes state.** A dataset owner *proposes* a transition; the governance/validation process *gates* it. In particular, `draft → active` occurs **only after** [DATA_CONTRACT.md](../DATA_CONTRACT.md) validation passes — fail loud otherwise. The registry is updated only once validation succeeds.
- **The registry `status` field MUST equal the dataset's true lifecycle state.** The `status` value in [dataset_registry.json](../dataset_registry.json) is the single source of truth for lifecycle state. If recorded `status` and reality diverge, that is a governance defect to be corrected immediately; nothing else (including [DATA_CATALOG.md](../DATA_CATALOG.md)) overrides the registry's `status`.
- **Naming stability across the lifecycle.** A `dataset_id`, once active, is never renamed. To replace a dataset, deprecate the existing id and register a new id — see [docs/metadata_standard.md](metadata_standard.md) and the conventions in the registry.

---

## Cross-references

- [ROOT.md](../ROOT.md) — supreme governance document and data integrity principles.
- [DATA_CONTRACT.md](../DATA_CONTRACT.md) — schema and quality rules; gates the `draft → active` transition.
- [dataset_registry.json](../dataset_registry.json) — authoritative source of each dataset's `status` (lifecycle state).
- [DATA_CATALOG.md](../DATA_CATALOG.md) — human-readable, registry-derived view (never authoritative).
