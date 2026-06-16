# Market Data Storage Policy

> Subordinate to [ROOT.md](../ROOT.md) and [DATA_CONTRACT.md](../DATA_CONTRACT.md).
> Defines where large market data lives, what may be committed, and how the
> repository stays clone-safe. Introduced in Phase 5.

---

## Why a separate policy

The governance/reference datasets (e.g. `reference.universe.metadata`) are small
and committed under `data/` for offline, deterministic validation. **Market data
is different**: a single interval of Binance USD-M Futures Klines spans hundreds
of symbols and years of history. Committing it would bloat the repository,
break clone times, and tie machine-specific run state into version control.

Therefore large market data is **never committed**. It lives only under
`local_data/`, which is git-ignored.

---

## Storage layout

```
local_data/binance_um_klines/interval=<INTERVAL>/
├── raw/
│   ├── monthly/<SYMBOL>/<SYMBOL>-<INTERVAL>-<YYYY-MM>.zip
│   └── daily/<SYMBOL>/<SYMBOL>-<INTERVAL>-<YYYY-MM-DD>.zip
├── checksums/
│   ├── monthly/<SYMBOL>/<...>.zip.CHECKSUM
│   └── daily/<SYMBOL>/<...>.zip.CHECKSUM
├── manifests/
│   ├── manifest.json        # main run manifest
│   └── files.jsonl          # per-file manifest
├── reports/
│   ├── coverage_summary.json
│   ├── missing_files.jsonl
│   ├── checksum_failures.jsonl
│   └── run_summary.md
├── catalog/
│   ├── archive_files.jsonl  # discovery inventory
│   ├── symbols.json
│   ├── discovery_summary.json
│   └── research_access.json # research-agent access metadata
└── tmp/                     # atomic-write scratch
```

Each Kline interval has its **own** `interval=<INTERVAL>/` root, so intervals
never collide on disk.

---

## What may be committed

Commits may contain only:

- code (`datahub/…`)
- docs (`docs/…`, top-level governance docs)
- tests and **small** fixtures (`tests/…`)
- registry / catalog / contract changes
- small committed reference metadata under `data/`

Commits must **not** contain full historical Kline data.

---

## `.gitignore` requirement

`.gitignore` **must** contain:

```
local_data/
```

This is enforced by the `binance-um-klines` validator (rule
`KL-LOCAL-DATA-IGNORED`).

---

## Before every commit

```bash
git status --short
```

Confirm:

- `local_data/` does **not** appear as staged
- `local_data/` does **not** appear as untracked
- only intended code / docs / tests / metadata are included

If `local_data/` appears in `git status`, **stop and fix `.gitignore`** before
committing.

---

## Provenance & checksums without committing data

Because the data is uncommitted, the registry entry for
`market.binance.um.klines` does **not** store a single content checksum. Instead:

- per-file SHA-256 checksums are downloaded from the archive (`.CHECKSUM`) and
  verified on download;
- the run manifest records per-file `checksum_status` and aggregate
  `checksum_failed_count`;
- reproducibility is via the documented archive source + the pipeline command,
  not via a committed artifact.

This is the deliberate Phase 5 decision recorded in
[HANDOFF.md](../HANDOFF.md): the registry contract is not bent to store
machine-specific local run checksums.
