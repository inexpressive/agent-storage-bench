"""Сводка уровня 1: по каждой среде, ФС и N — медиана p99, потери, порча базы, падения агентов.
Берёт все results/real-<env>-<fs>-*/summary.csv; повторы одной точки из разных каталогов складываются."""
import csv, glob, os, re, statistics
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
pts = defaultdict(list)
for f in sorted(glob.glob(os.path.join(HERE, "..", "results", "real-*", "summary.csv"))):
    env, fs = re.match(r"real-(\w+)-(container|macos-bind)-", os.path.basename(os.path.dirname(f))).groups()
    for r in csv.DictReader(open(f)):
        pts[(env, fs, int(r["n"]))].append(r)

num = lambda v: float(v) if v not in ("", None) else None
print("| среда | ФС | N | повторов | p99, мс (медиана) | потеряно / подтверждено | база битая | упавших агентов | ошибок записи |")
print("|---|---|---|---|---|---|---|---|---|")
for (env, fs, n), rs in sorted(pts.items()):
    p99 = [num(r["p99_ms"]) for r in rs if num(r["p99_ms"]) is not None]
    lost = sum(int(r["missing"]) + int(r["stale"]) for r in rs)
    acked = sum(int(r["acked"]) for r in rs)
    if env == "openscience":  # у OpenScience общий файл — отдельная сверка
        lost += sum(int(r.get("shared_lost") or 0) for r in rs)
        acked += sum(int(r.get("shared_acked") or 0) for r in rs)
    bad = sum(r["integrity"] != "ok" for r in rs)
    print(f"| {env} | {fs} | {n} | {len(rs)} | {statistics.median(p99):.0f} | {lost} / {acked} | {bad} из {len(rs)} | "
          f"{sum(int(r['crashed']) for r in rs)} | {sum(int(r['errors']) for r in rs)} |" if p99 else
          f"| {env} | {fs} | {n} | {len(rs)} | — | {lost} / {acked} | {bad} из {len(rs)} | {sum(int(r['crashed']) for r in rs)} | {sum(int(r['errors']) for r in rs)} |")
