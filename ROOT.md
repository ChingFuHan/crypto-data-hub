# ROOT.md

> **Highest-priority document in this repository.**
> If any rule, document, code comment, or agent instruction conflicts with this
> file, **ROOT.md wins**. Resolve every conflict in favor of ROOT.md.

---

## Mission

Build a long-term maintainable **crypto data platform repository** that serves
as the single, unified data infrastructure for the organization.

The Data Hub provides:

- **Dataset** — the curated data itself
- **Metadata** — descriptive information about each dataset
- **Registry** — the authoritative index of all datasets
- **Snapshot** — immutable, point-in-time captures of data
- **Documentation** — human- and agent-readable governance and usage docs

---

## Core Principles

These five principles drive every design and review decision. When trading off,
prefer the principle that protects long-term correctness over short-term speed.

1. **Maintainability** — Code and docs stay simple, readable, and discoverable.
   Optimize for the next maintainer (human or agent), not for cleverness.
2. **Reproducibility** — Any dataset or result can be regenerated from recorded
   inputs, code version, and config. No undocumented manual steps.
3. **Scalability** — Structure supports growth in dataset count, volume, and
   contributor count without rework of the foundation.
4. **Data Quality** — Data is validated against an explicit contract before it
   is trusted. Bad data fails loudly, never silently.
5. **Automation** — Repetitive work (validation, registry updates, snapshots,
   reporting) is scripted and CI-enforced, not done by hand.

---

## Rule Priority

When instructions conflict, follow this order (highest first):

1. **ROOT.md** (this file)
2. **DATA_CONTRACT.md** — data correctness and schema rules
3. **AGENTS.md** — current operating state and priorities
4. **HANDOFF.md** — architecture decisions and context
5. **README.md** / **QUICKSTART.md** — usage guidance
6. Inline code comments and ad-hoc instructions

A lower-priority document may add detail but must never contradict a
higher-priority one. If it does, the higher-priority document is correct and
the lower one must be fixed.

---

## Data Integrity Principles

- **Single source of truth** — `dataset_registry.json` is the authoritative
  index. No dataset is "real" until it is registered.
- **Contract before trust** — Every dataset conforms to `DATA_CONTRACT.md`.
  Data that fails the contract is rejected, not patched into compliance.
- **Provenance is mandatory** — Every dataset records its source, generation
  code version, and parameters so it can be reproduced.
- **Immutability of records** — Published snapshots and registry history are
  append-only. Correct forward with a new version; never silently rewrite.
- **Fail loud** — Validation errors stop the pipeline. Silent coercion,
  truncation, or `NaN`-filling of bad data is forbidden.

---

## Snapshot Principles

- A **snapshot** is an immutable, point-in-time capture of a dataset.
- Each snapshot is identified by `dataset_id` + version + timestamp and is
  never modified after creation.
- Snapshots are **content-addressable**: identical inputs and code produce an
  identical, verifiable snapshot (supports Reproducibility).
- A snapshot carries enough metadata (source, code version, params, checksum)
  to be regenerated and verified independently.
- Deleting a snapshot is an explicit, logged governance action — never an
  incidental side effect of normal operation.

---

## Handoff Principles

- The repository must be **self-onboarding**: any new agent or human becomes
  productive by reading docs in the prescribed order — no tribal knowledge.
- **Onboarding order:** `ROOT.md` → `AGENTS.md` → `HANDOFF.md` → `README.md`.
- `AGENTS.md` always reflects the **current** phase, status, and next actions.
- `HANDOFF.md` records **why** decisions were made, not only what was done.
- State is left **clean and documented** at every phase boundary: no
  half-finished work without a recorded note of its status and intent.

---

## Phased Delivery Governance

This repository follows **Architecture First → MVP First → Incremental
Delivery → Review Before Expansion**.

- Work proceeds in numbered **phases**.
- Each phase **stops on completion** and waits for review.
- An agent **must not** begin the next phase on its own initiative.
- Expansion of scope happens **only after review** of the prior phase.

The current phase and its status are always recorded in `AGENTS.md`.
