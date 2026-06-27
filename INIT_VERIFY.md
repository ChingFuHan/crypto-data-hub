# INIT_VERIFY.md

# crypto-data-hub 初始化驗收清單（mandatory acceptance checklist）

本檔案是 `INIT.md` 的**強制驗收清單**。初始化或重建 `local_data/` 之後，
必須逐項通過本清單，才能繼續 migration、push，或讓 research agent 使用資料。

適用情境：

    new machine setup
    new VM setup
    disaster recovery
    local_data rebuild
    current dataset layout migration
    live-update tooling changes

規則優先順序見 `INIT.md`。本清單只負責驗收，不重複解釋任務規則。

> **Critical failure 處理。** 本清單任一 section 出現 critical failure 時：
>
>     不得繼續 migration（dry-run 或 execute 皆停）。
>     不得 commit、不得 push。
>     不得讓 research agent 使用該資料。
>     停止，回報失敗的 section 與輸出，等待使用者決策。

---

## A. Primary universe verification

確認本專案真正的 primary research / trading universe：

    venue       = Binance USDⓈ-M Futures (USD-M Futures)
    contract    = PERPETUAL
    quote_asset = USDT
    history     = include delisted USDT perpetual contracts（歷史研究用）

確認排除（**不屬於** primary universe）：

    quote_asset != USDT
    USDC quote pairs（如 KAITOUSDC、BTCUSDC、SOLUSDC、DOGEUSDC）
    BUSD quote pairs
    delivery contracts（如 BTCUSDT_230630）
    SETTLED symbols（如 CVXUSDTSETTLED）
    non-ASCII symbols（如 龙虾USDT、币安人生USDT）

> **重要澄清。** Binance UM / USDⓈ-M Futures **不等於只有 USDT pairs**。
> USDⓈ-M Futures 同時包含 USDT、USDC、BUSD quote pairs，以及 delivery /
> settled / special symbols。primary universe 只取 `quote_asset = USDT` 的
> PERPETUAL（含已下市的 USDT 永續）。完整定義見 `DATA_CONTRACT.md` →
> *Primary Universe Policy*。

Pass criteria:

- primary universe 定義與上述一致。
- 非 primary 資料若已存在於 `local_data/`，採 inventory / quarantine，
  **不得直接刪除**（見 section E、`DATA_CONTRACT.md`）。

---

## B. Git cleanliness

Commands:

    git status --short
    git diff --check

Pass criteria（不得出現）:

    no local_data
    no parquet
    no jsonl
    no backup/stage（_layout_migration_stage / _layout_migration_backup）
    no runtime state（live_update state / closed_buffer / latest / rejects）
    git diff --check clean（無 whitespace / conflict marker）

任一項出現即 critical failure。

---

## C. Python / validation

Commands:

    .venv/bin/python -m py_compile scripts/live_update.py datahub/live_update.py

    .venv/bin/python -m unittest discover tests

    .venv/bin/python -m datahub.validation --all

Pass criteria:

    py_compile OK
    unittest OK
    datahub.validation failed = 0

> `datahub.validation --all` 是 clone-safe global validation
> （registry / governance + 若本地存在則驗證 kline manifest），**不**代表已
> 驗證所有 interval 的 local_data。全 interval local_data 驗證須依
> `planning/tasks/task_rebuild_all_klines_verify.md`。

---

## D. Current layout audit

Command:

    .venv/bin/python scripts/live_update.py \
      --interval 1m \
      --audit-current-layout | tee /tmp/init_verify_current_layout_audit.json

Pass criteria:

    no bad symbols
    只允許預期的 mixed symbols
    current expected mixed symbols:
      - BTCUSDT
      - ETHUSDT

任何非預期 mixed / bad symbol 即 critical failure。

---

## E. Source parquet readability warning

INIT_VERIFY 必須注意：

- `--audit-current-layout` **只檢查 layout**（year-only / year-month / mixed）。
- `--audit-current-layout` **不保證 parquet 可讀**（不驗 footer / magic bytes）。
- migration dry-run / execute 若 source parquet readability precheck 失敗，
  **不得繼續**。
- `KAITOUSDC` 是 **known quarantined symbol**，在 recovery policy 尚未存在前：
  不重跑 migration、不自動修復、不刪除。

已知 corrupt source parquet（範例）：

    local_data/binance_um_klines_current/interval=1m/parquet/symbol=KAITOUSDC/year=2025/part-000.parquet
      size   = 655360
      head4  = PAR1
      tail4  != PAR1
      pyarrow read failed:
        Parquet magic bytes not found in footer.
        Either the file is corrupted or this is not a parquet file.

KAITOUSDC 同時也是 USDC quote pair，本就不屬於 primary universe；此處再因
unreadable parquet 額外列為 quarantine。

---

## F. Migration planner verification

標準命令（`--quote-assets USDT` 為目標旗標，見下方 pending 註記）:

    .venv/bin/python scripts/live_update.py \
      --interval 1m \
      --plan-current-layout-migration-batches \
      --batch-size 10 \
      --max-row-count 300000 \
      --max-batches 2 \
      --quote-assets USDT \
      --exclude-delivery-contracts \
      --exclude-settled \
      --exclude-non-ascii \
      --exclude-symbols BTCUSDT ETHUSDT KAITOUSDC \
      --dry-run-batches

> **Pending implementation 註記。** `--quote-assets USDT` 目前**尚未實作**。
> 在實作前，暫時 workaround：手動排除非 USDT quote symbols（USDC / BUSD），
> 並用 `--exclude-symbols` 補上已知非 primary symbols（如 `KAITOUSDC`）。
> 其餘旗標（`--exclude-delivery-contracts` / `--exclude-settled` /
> `--exclude-non-ascii` / `--exclude-symbols`）已實作。

Pass criteria:

    read_only = true
    execute = false
    dry_run_batches = true
    selected symbols 不得包含：
      USDC / BUSD quote pairs
      delivery contracts
      SETTLED symbols
      non-ASCII symbols
      KAITOUSDC
    若任何 dry-run 結果出現 warning 或 source_parquet_unreadable，停止。

---

## 收尾

全部 section pass 後：

    git status --short
    git ls-files local_data | wc -l   # 預期 0

接受結果：ACCEPT / ACCEPT_WITH_WARNINGS。
任一 section critical failure：REJECT — 不 migration、不 push、不給 research agent。
