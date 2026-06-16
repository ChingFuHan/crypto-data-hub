# Validation Framework

> Phase 3 Validation Foundation plus Phase 4 artifact integration. Subordinate
> to [ROOT.md](../ROOT.md) and [DATA_CONTRACT.md](../DATA_CONTRACT.md). If
> anything here conflicts with a higher-priority document, the higher-priority
> document wins.

---

## Validation Scope

Phase 3 turned core governance rules into executable checks. Phase 4 adds
Universe Metadata artifact validation. Current scope:

- `dataset_registry.json`
- Dataset entry schema rules declared in the registry
- Dataset lifecycle rules from `docs/dataset_lifecycle.md`
- Dataset-related naming rules from `docs/naming_convention.md`
- Universe Metadata artifact/fixture validation for Q1-Q6 plus point-in-time reconstruction
- Phase 5: Binance USD-M Klines `local_data` run-manifest validation (`binance-um-klines`)

Phase 4 adds a validated draft Universe Metadata artifact. Validation still does
not move Universe Metadata from `draft` to `active`. Phase 5 adds a manifest
validator for large `local_data` Kline runs that is **explicit-only** and never
required by the clone-safe `--all` default.

---

## Validation Architecture

Validation code lives in `datahub/validation/`:

```text
datahub/
├── __init__.py
└── validation/
    ├── __init__.py
    ├── __main__.py
    ├── cli.py
    ├── errors.py
    ├── lifecycle.py
    ├── naming.py
    ├── registry.py
    ├── result.py
    └── universe_metadata.py
```

Entry point:

```bash
python -m datahub.validation
```

`datahub/validation/__main__.py` calls `datahub.validation.cli.main()` and
returns the CLI exit code to the shell. `scripts/validate.py` is a small wrapper
for automation that wants a script path.

Rule organization:

- `registry.py` validates the registry file and dataset entry metadata.
- `lifecycle.py` validates lifecycle state and state-specific requirements.
- `naming.py` validates dataset ids, versions, references, paths, and timestamps.
- `universe_metadata.py` validates Universe Metadata artifacts/fixtures.
- `result.py` defines the result/report model shared by all validators.

---

## Validation Result Model

Every rule returns a `ValidationCheck` with:

- `rule_id`
- `severity`
- `status`
- `message`
- `file`
- `dataset_id`
- `field`
- `location`
- `details`

Supported severities:

- `error`
- `warning`
- `info`

Supported statuses:

- `passed`
- `failed`
- `skipped`

`ValidationReport` aggregates checks and exposes:

- total checks
- passed checks
- failed checks
- warning checks
- skipped checks
- affected files
- affected dataset ids
- error summary

---

## Validation Error Model

Validation failures are represented as checks with `status = failed` and
`severity = error`. The CLI exits `1` when any such check exists.

Invalid CLI use or missing required CLI inputs raise `ValidationCommandError`;
the CLI catches it, prints a concise command error, and exits `2`.

Validation does not coerce bad data. Bad inputs fail loud and leave source files
unchanged.

---

## How To Run Validation

From repo root:

```bash
python -m datahub.validation
python -m datahub.validation --target registry
python -m datahub.validation --all
python -m datahub.validation --target universe-metadata --fixture tests/fixtures/universe_metadata/valid_universe_metadata.json
python -m datahub.validation --target universe-metadata --fixture data/reference/universe_metadata/reference.universe.metadata.json
python -m unittest discover tests
```

On systems where the Python launcher is named `python3`, use the same commands
with `python3 -m ...`.

---

## CLI Usage

Default target:

```bash
python -m datahub.validation
```

Equivalent to:

```bash
python -m datahub.validation --target registry
```

Registry target:

```bash
python -m datahub.validation --target registry
```

Runs registry, lifecycle, and naming checks against repo files.

Universe Metadata artifact/fixture target:

```bash
python -m datahub.validation --target universe-metadata --fixture tests/fixtures/universe_metadata/valid_universe_metadata.json
```

Validates one artifact or fixture file.

Binance USD-M Klines target (Phase 5):

```bash
python -m datahub.validation --target binance-um-klines --interval 1d \
  --manifest local_data/binance_um_klines/interval=1d/manifests/manifest.json
```

Validates one machine-specific `local_data` Kline run manifest. `--manifest` is
**required**; invoking the target without it returns exit `2`. Checks (rule ids
`KL-*`): manifest exists and is valid JSON, interval supported, manifest interval
matches the CLI interval, `dataset_id = market.binance.um.klines`,
`dataset_variant_id = market.binance.um.klines.<INTERVAL>`, primary key
`[symbol, interval, open_time]`, `checksum_failed_count = 0`, file manifest /
coverage report / research-access manifest exist, verified files exist on disk,
daily archive policy recorded, and `.gitignore` excludes `local_data/`.

All target:

```bash
python -m datahub.validation --all
```

Runs registry checks plus the default Universe Metadata artifact/fixture.
If `data/reference/universe_metadata/reference.universe.metadata.json` exists,
`--all` validates that Phase 4 artifact; otherwise it falls back to
`tests/fixtures/universe_metadata/valid_universe_metadata.json`.

**Clone-safe rule.** `--all` must never require `local_data/` to exist. It
validates the default Kline manifest
(`local_data/binance_um_klines/interval=1d/manifests/manifest.json`) **only if it
is present**; otherwise `KL-MANIFEST-EXISTS` is recorded as `skipped` with a
clear message. A fresh clone therefore passes `--all` and
`python -m unittest discover tests` with no `local_data/`.

---

## Exit Codes

- `0` — all checks passed
- `1` — validation failed
- `2` — validation command error / invalid invocation

---

## Test Fixtures And Artifact

Phase 4 artifact:

```text
data/reference/universe_metadata/reference.universe.metadata.json
```

Raw source snapshot and manifest:

```text
data/raw/reference/universe_metadata/
data/manifests/reference/universe_metadata/manifest.json
```

Universe Metadata fixtures live under:

```text
tests/fixtures/universe_metadata/
```

Current fixtures:

- `valid_universe_metadata.json`
- `duplicate_instrument_id.json`
- `invalid_timestamp_order.json`
- `invalid_active_delisted_invariant.json`
- `invalid_successor_reference.json`
- `cyclic_successor_graph.json`
- `broken_point_in_time_reconstruction.json`

Fixtures are synthetic and local. The Phase 4 artifact is generated from the
committed raw Binance USD-M Futures `exchangeInfo` snapshot.

---

## Current Coverage

Registry validation:

- JSON validity
- `registry_version` format
- `conventions` presence
- `dataset_entry_schema` presence
- `datasets` array presence
- required fields
- type checks
- `dataset_id` pattern and uniqueness
- dataset version format
- status enum
- owner/source/timezone/update frequency formats
- primary key format
- schema ref format
- `created_at` / `updated_at` format and order
- quality and provenance structure
- lineage upstream references

Lifecycle validation:

- valid lifecycle state
- allowed transition table
- draft contract/catalog/metadata/quality requirements
- active/deprecated/archived skeleton checks, including future active data
  location / snapshot availability

Naming validation:

- dataset id naming
- dataset version naming
- registry version naming
- metadata reference naming
- timestamp string format
- snapshot path naming skeleton

Universe Metadata validation:

- `UM-Q1` required fields
- `UM-Q2` instrument, symbol-era, and active-symbol uniqueness
- `UM-Q3` active/delisted/successor lifecycle invariants
- `UM-Q4` timestamp validity, order, and ingestion-time bound
- `UM-Q5` contract information rules
- `UM-Q6` successor reference and acyclic graph rules
- `UM-PIT` symbol-era interval overlap and rename handoff rule

Phase 4 ingestion integration:

- `python -m datahub.ingestion.universe_metadata --fetch`
- `python -m datahub.ingestion.universe_metadata --normalize`
- `python -m datahub.ingestion.universe_metadata --all`
- `python -m datahub.ingestion.universe_metadata --offline --all`
- raw snapshot reuse by checksum
- deterministic artifact and manifest regeneration from committed raw snapshot

---

## Known Gaps

- Only the first draft Universe Metadata artifact exists; dataset lifecycle
  remains `draft`.
- Active/deprecated/archived lifecycle checks are skeletons until later phases
  create those states.
- No JSON Schema file exists for `dataset_registry.json`.
- No CI workflow runs validation yet.
- `DATA_CATALOG.md` is not generated from the registry yet.
- Snapshot validation awaits snapshot implementation.
- Validation report files under `reports/` are not generated yet.
- Historical delisted, renamed, and merged Universe Metadata rows are not
  ingested yet.

---

## Future Work

- Expand Universe Metadata ingestion to historical lifecycle coverage.
- Decide whether/when Universe Metadata can move `draft → active` after review.
- Add JSON Schema and CI.
- Generate `DATA_CATALOG.md` from `dataset_registry.json`.
- Add snapshot creation, checksums, and snapshot validation.
- Emit persisted validation reports under `reports/`.
