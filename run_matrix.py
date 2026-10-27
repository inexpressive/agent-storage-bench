#!/usr/bin/env python3
"""Прогон матрицы: режимы × N для одной файловой системы.

Пример:
  python3 run_matrix.py --fs container --dir ~/bench-state/tmp
  python3 run_matrix.py --fs macos-bind --dir ./tmp-db

Результаты: results/<дата>-<fs>/summary.csv, summary.json, raw/<точка>.csv.gz.
Сводка дописывается после каждой точки — прерванный прогон не теряет готовое.

Этап 2 (журнал событий) — только явно, python-ом из ~/bench-tools/env
и с запущенным брокером (./brokers.sh start redis|kafka):
  python run_matrix.py --fs container --dir ~/bench-state/tmp \
      --configs event-log-redis --ns 1,5,10,20,50,100,150,200 \
      --out results/<дата>-container-eventlog-redis

Окно пачки компактора (гипотеза H3) — список значений, мс; метка точки
получает суффикс -w<W>:
  python run_matrix.py --fs container --dir ~/bench-state/tmp \
      --configs event-log-redis --ns 20,50,100,200 --batch-window-ms 0,25,75 \
      --out results/<дата>-container-eventlog-redis-batch
"""
import argparse
import csv
import datetime
import json
import os
import time

from bench import run_point

CONFIGS = [  # (mode, interval_ms)
    ("one-file", 75),
    ("one-file-append", 75),
    ("one-file-threads", 75),
    ("file-per-session", 75),
    ("json-rename", 250),
    ("json-rename", 75),
    ("json-shared-rmw", 75),
]
# Этап 2: в «all» не входят — нужен брокер и клиенты из окружения tools/env.
EVENTLOG_CONFIGS = [("event-log-redis", 75), ("event-log-kafka", 75)]


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser()
    ap.add_argument("--fs", required=True, help="метка ФС, попадает в имя каталога")
    ap.add_argument("--dir", required=True, help="где создавать базы/файлы")
    ap.add_argument("--ns", default="1,5,10,20,50,100")
    ap.add_argument("--duration", type=float, default=30.0)
    ap.add_argument("--warmup", type=float, default=5.0)
    ap.add_argument("--keep-corrupt", default=os.path.expanduser("~/bench-state/corrupt"),
                    help="куда копировать базы, не прошедшие integrity_check")
    ap.add_argument("--configs", default="all",
                    help="подмножество: one-file,one-file-append,one-file-threads,file-per-session,json-rename-250,json-rename-75")
    ap.add_argument("--out", help="каталог результатов (по умолчанию results/<дата>-<fs>)")
    ap.add_argument("--batch-window-ms", default=None,
                    help="только event-log-*: окна пачки компактора через запятую, мс "
                         "(например 0,25,75); без флага — 0 и прежние метки")
    ap.add_argument("--wal-autocheckpoints", default=None, help="H4, список PRAGMA wal_autocheckpoint")
    ap.add_argument("--kafka-acks", default="1")
    ap.add_argument("--kafka-linger-ms", default="0")
    ap.add_argument("--kafka-wait", default="each")
    a = ap.parse_args()

    out = a.out or os.path.join(here, "results",
                                f"{datetime.date.today().isoformat()}-{a.fs}")
    os.makedirs(os.path.join(out, "raw"), exist_ok=True)
    workdir = os.path.abspath(a.dir)
    os.makedirs(workdir, exist_ok=True)
    ns = [int(x) for x in a.ns.split(",")]
    rows = []
    t0 = time.time()
    windows = ([float(x) for x in a.batch_window_ms.split(",")]
               if a.batch_window_ms else [None])
    wal_values = [int(x) for x in a.wal_autocheckpoints.split(",")] if a.wal_autocheckpoints else [None]
    kafka_values = [(ack, int(linger), wait) for ack in a.kafka_acks.split(",")
                    for linger in a.kafka_linger_ms.split(",") for wait in a.kafka_wait.split(",")]
    points = []
    for mode, iv in CONFIGS + EVENTLOG_CONFIGS:
        base = mode if mode != "json-rename" else f"json-rename-{iv}"
        if a.configs == "all":
            if mode.startswith("event-log") or mode in ("one-file-append", "one-file-threads", "json-shared-rmw"):
                continue
        elif base not in a.configs.split(",") and not (mode == "json-shared-rmw" and
                any(x.startswith("json-shared-rmw-") for x in a.configs.split(","))):
            continue
        for w in (windows if mode.startswith("event-log") else [None]):
            for ac in (wal_values if mode in ("one-file", "one-file-threads", "file-per-session") else [None]):
                for kafka in (kafka_values if mode == "event-log-kafka" else [None]):
                    label = base if w is None else f"{base}-w{w:g}"
                    if ac is not None: label += f"-ac{ac}"
                    if kafka is not None: label = f"event-log-kafka-a{kafka[0]}-l{kafka[1]}-{kafka[2]}"
                    if mode == "json-shared-rmw":
                        for lock in ("none", "excl"):
                            rmw_label = f"json-shared-rmw-{lock}"
                            if a.configs != "all" and rmw_label not in a.configs.split(","):
                                continue
                            points.append((mode, iv, rmw_label, w, ac, kafka))
                    else:
                        points.append((mode, iv, label, w, ac, kafka))
    for n in ns:  # по N снаружи: если прервать, будут все режимы для малых N
        for mode, iv, label, w, ac, kafka in points:
            raw = os.path.join(out, "raw", f"{label}-n{n}.csv.gz")
            for attempt in (1, 2):
                if mode.startswith("event-log"):
                    import eventlog  # redis-py / confluent-kafka — только здесь
                    s = eventlog.run_point(mode.rsplit("-", 1)[1], n, a.fs, workdir,
                                           interval_ms=iv, duration=a.duration,
                                           warmup=a.warmup, label=label, raw_csv=raw,
                                           keep_dir=a.keep_corrupt,
                                           batch_window_ms=w or 0.0,
                                           kafka_acks=kafka[0] if kafka else "1",
                                           kafka_linger_ms=kafka[1] if kafka else 0,
                                           kafka_wait=kafka[2] if kafka else "each")
                else:
                    s = run_point(mode, n, a.fs, workdir, interval_ms=iv,
                                  duration=a.duration, warmup=a.warmup, label=label,
                                  raw_csv=raw, keep_dir=a.keep_corrupt,
                                  wal_autocheckpoint=ac,
                                  rmw_lock="excl" if label.endswith("-excl") else "none")
                # Пауза VM / сон мака: разрыв >5 с или расхождение часов >1 с.
                suspect = s["gap_max_ms"] > 5000 or abs(s["clock_drift_s"]) > 1.0
                # Этап 2: сквозная задержка по monotonic и по time.time() разошлась > 1 с.
                suspect = suspect or s.get("clock_mono_wall_max_ms", 0) > 1000
                s["suspect_pause"] = suspect
                s["attempt"] = attempt
                if not suspect:
                    break
                print(f"  ! {label} N={n}: gap_max={s['gap_max_ms']:.0f} мс, "
                      f"drift={s['clock_drift_s']} с — перезапуск", flush=True)
            rows.append(s)
            print(f"[{time.time()-t0:7.0f}s] {a.fs} {label:18s} N={n:3d} "
                  f"p50={s['write_p50_ms']:.2f} p99={s['write_p99_ms']:.2f} "
                  f"max={s['write_max_ms']:.0f} late75={s['late75_frac']:.4f} "
                  f"rate={s['rate_ratio']:.3f} locked={s['locked_errors']} "
                  f"corrupt={s['corrupt_errors']} bad_db={s['integrity_bad_dbs']} "
                  + (f"append_bad={s['append_state_mismatches']} "
                     if "append_state_mismatches" in s else "") +
                  f"err={s['other_errors']} gapmax={s['gap_max_ms']:.0f} "
                  f"stalls={s['sync_stalls']} drift={s['clock_drift_s']}"
                  + (f" | e2e p50={s['e2e_p50_ms']} p99={s['e2e_p99_ms']} "
                     f"max={s['e2e_max_ms']} drain={s['drain_ms']} "
                     f"lagmax={s['lag_events_max']} state_bad={s['state_mismatch']} "
                     f"comp_ok={s['compactor_ok']} ev/batch={s['comp_events_per_batch']} "
                     f"commits={s['comp_commits_win']} busy={s['comp_busy_frac']}"
                     if "e2e_p50_ms" in s else ""),
                  flush=True)
            with open(os.path.join(out, "summary.json"), "w") as f:
                json.dump(rows, f, ensure_ascii=False, indent=1)
            with open(os.path.join(out, "summary.csv"), "w", newline="") as f:
                fields = list(dict.fromkeys(key for row in rows for key in row))
                w = csv.DictWriter(f, fieldnames=fields)
                w.writeheader()
                w.writerows(rows)
    # Каталог точки удаляет сам run_point; чужое в --dir не трогаем.
    print(f"done in {time.time()-t0:.0f}s -> {out}", flush=True)


if __name__ == "__main__":
    main()
