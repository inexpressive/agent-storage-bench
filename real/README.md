# Уровень 1 — настоящий код записи трёх сред

Обвязки в этой папке запускают **код хранилища самих сред** без LLM: N
процессов-агентов пишут так, как пишет ход агента, затем `run_real.py`
сверяет, что агент получил как «успех», с тем, что лежит в хранилище.
Итоги — `../results/REAL-SUMMARY.md`.

## Исходники сред (коммиты, на которых мерили 07.10.2026)

Клонировать в `~/bench-real-src` (или задать путь `BENCH_REAL_SRC`):

| Каталог | Репозиторий | Коммит | Лицензия |
|---|---|---|---|
| `OpenResearch` | https://github.com/alphaXiv/OpenResearch | `b9ce4f3` | MIT |
| `OpenScience` | https://github.com/synthetic-sciences/OpenScience | `44d0334` | Apache-2.0 |
| `tinyagents` | https://github.com/tinyhumansai/tinyagents | `2eb9148e` | GPL-3.0-only |

Обвязки (`*_bench_real.rs`, `openscience_agent.ts`) вызывают API хранилища сред и
не копируют их код. Собранный с ними тестовый бинарник — производная от исходника
среды и подчиняется её лицензии (для `tinyagents` — GPL-3.0); бинарники в репозиторий
не входят.

Версии Rust и bun на замере 07.10 не записаны; минимум по исходникам — Rust 1.88
у `tinyagents`, `bun@1.3.14` у OpenScience (`packageManager`).

## Установка обвязок

- **OpenResearch** (Rust): `openresearch_bench_real.rs` → `src/bench_real.rs`,
  в `src/main.rs` после списка модулей добавить
  `#[cfg(test)] mod bench_real;`. Собрать тесты:
  `cargo test --release --no-run`; путь к бинарнику теста
  (`target/release/deps/orx-<hash>`) — в `OR_TEST_BIN`.
- **OpenScience** (TypeScript, bun): `openscience_agent.ts` →
  `backend/cli/bench-real/openscience_agent.ts`; `bun install` в `backend/cli`.
  `run_real.py` ищет bun в `~/.bun/bin/bun`.
- **OpenHuman** (Rust, крейт `tinyagents-session`): `openhuman_bench_real.rs` →
  `crates/tinyagents-session/tests/bench_real.rs`, затем
  `cargo test --release -p tinyagents-session --test bench_real --no-run`;
  путь к бинарнику (`target/release/deps/bench_real-<hash>`) — в `OH_TEST_BIN`.

## Запуск

Из папки `real/`:

```bash
export OR_TEST_BIN=… OH_TEST_BIN=…
python3 run_real.py --env openresearch --fs container --dir ~/bench-state/tmp --ns 2,5,10 --reps 1
bash matrix.sh        # полная матрица, ~1,5 ч; замеры строго по очереди
python3 summarize.py  # сводная таблица
```

`--fs` — только метка в результатах: сама ФС определяется каталогом
`--dir` (`macos-bind` у нас — папка macOS, примонтированная в контейнер
Docker Desktop).
