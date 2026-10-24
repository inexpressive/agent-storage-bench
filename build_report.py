#!/usr/bin/env python3
"""Собирает results/report.html — самодостаточный отчёт этапов 1–2 (графики инлайн-SVG).

Источник — results/*/summary.json. Запуск: python3 build_report.py
"""
import glob
import html
import json
import math
import os
import statistics

HERE = os.path.dirname(os.path.abspath(__file__))
RES = os.path.join(HERE, "results")


def load(sub):
    p = os.path.join(RES, sub, "summary.json")
    return json.load(open(p)) if os.path.exists(p) else []


container = load("2026-10-04-container")
recheck = load("2026-10-04-container-sqlite-recheck")
bind = load("2026-10-04-macos-bind")
trunc = load("2026-10-04-macos-bind-truncate")
ev = {(fs, b): load(f"2026-10-04-{fs}-eventlog-{b}")
      for fs in ("container", "macos-bind") for b in ("redis", "kafka")}
append = {fs: load(f"2026-10-04-{fs}-append") for fs in ("container", "macos-bind")}
threads = load("2026-10-04-macos-bind-threads")
batchw = {fs: load(f"2026-10-04-{fs}-eventlog-redis-batch") for fs in ("container", "macos-bind")}
repeats = []
for p in sorted(glob.glob(os.path.join(RES, "2026-10-04-container-repeats", "r*", "summary.json"))):
    repeats += json.load(open(p))


def pick(rows, label):
    return sorted((r for r in rows if r["label"] == label), key=lambda r: r["n"])


def corrupt_share(r):
    return (r.get("corrupt_errors") or 0) / r["flushes"] if r["flushes"] else 0


# ---------- SVG-график: p99 от N, лог-шкала по обеим осям ----------
COLORS = {
    "one-file": "var(--c1)",
    "file-per-session": "var(--c2)",
    "json-rename-75": "var(--c3)",
    "json-rename-250": "var(--c4)",
    "one-file TRUNCATE": "var(--c5)",
    "event-log-redis": "var(--c6)",
    "event-log-kafka": "var(--c7)",
    "one-file-append": "var(--c8)",
}
NAMES = {
    "one-file": "один файл (как OpenResearch)",
    "file-per-session": "файл на сессию",
    "json-rename-75": "JSON + rename, 75 мс",
    "json-rename-250": "JSON + rename, 250 мс (как OpenScience)",
    "one-file TRUNCATE": "один файл, TRUNCATE (откат OpenHuman)",
    "event-log-redis": "журнал событий, Redis Streams (публикация)",
    "event-log-kafka": "журнал событий, Kafka (публикация)",
    "one-file-append": "один файл, дельты (INSERT приращения)",
}
DASH = {"event-log-redis": "6 3", "event-log-kafka": "6 3"}


def chart(series, title, ns, ymin=0.5, ymax=5000, marks=None, scatter=None,
          field="write_p99_ms", ylabel="p99 записи, мс", colors=None, names=None, dash=None):
    colors, names, dash = colors or COLORS, names or NAMES, DASH if dash is None else dash
    W, H, L, R, T, B = 640, 360, 56, 16, 28, 44
    x0, x1 = math.log10(min(ns)), math.log10(max(ns))
    y0, y1 = math.log10(ymin), math.log10(ymax)
    X = lambda n: L + (math.log10(n) - x0) / (x1 - x0) * (W - L - R)
    Y = lambda v: T + (1 - (math.log10(max(min(v, ymax), ymin)) - y0) / (y1 - y0)) * (H - T - B)
    o = [f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="{html.escape(title)}">']
    for v in (1, 10, 100, 1000):
        if ymin <= v <= ymax:
            o.append(f'<line x1="{L}" x2="{W-R}" y1="{Y(v):.1f}" y2="{Y(v):.1f}" class="grid"/>'
                     f'<text x="{L-6}" y="{Y(v)+4:.1f}" class="ax" text-anchor="end">{v}</text>')
    for n in ns:
        o.append(f'<text x="{X(n):.1f}" y="{H-B+18}" class="ax" text-anchor="middle">{n}</text>')
    o.append(f'<text x="{(L+W-R)/2}" y="{H-6}" class="ax" text-anchor="middle">агентов (процессов)</text>')
    o.append(f'<text x="14" y="{(T+H-B)/2}" class="ax" text-anchor="middle" transform="rotate(-90 14 {(T+H-B)/2})">{ylabel}</text>')
    o.append(f'<line x1="{L}" x2="{W-R}" y1="{Y(75):.1f}" y2="{Y(75):.1f}" class="limit"/>'
             f'<text x="{W-R-4}" y="{Y(75)-6:.1f}" class="lim" text-anchor="end">75 мс — интервал сброса</text>')
    for key, rows in series:
        rows = [r for r in rows if r.get(field) == r.get(field)]  # без NaN
        pts = [(X(r["n"]), Y(r[field])) for r in rows]
        if not pts:
            continue
        c = colors[key]
        da = f' stroke-dasharray="{dash[key]}"' if key in dash else ""
        o.append(f'<polyline points="{" ".join(f"{a:.1f},{b:.1f}" for a, b in pts)}" fill="none" stroke="{c}" stroke-width="2.5"{da}/>')
        for (a, b), r in zip(pts, rows):
            tip = f'{names[key]}: N={r["n"]}, {ylabel} {r[field]:.1f}'
            o.append(f'<circle cx="{a:.1f}" cy="{b:.1f}" r="4" fill="{c}"><title>{html.escape(tip)}</title></circle>')
    for n, vals in (scatter or {}).items():
        for v in vals:
            o.append(f'<circle cx="{X(n):.1f}" cy="{Y(v):.1f}" r="3" fill="none" stroke="var(--c1)" stroke-width="1.5"><title>повтор: N={n}, p99 {v:.1f} мс</title></circle>')
    for n, text in (marks or {}).items():
        o.append(f'<text x="{X(n):.1f}" y="{T+12}" class="warn" text-anchor="middle">{html.escape(text)}</text>')
    o.append("</svg>")
    return "".join(o)


def legend(keys, colors=None, names=None):
    colors, names = colors or COLORS, names or NAMES
    return '<div class="legend">' + "".join(
        f'<span><i style="background:{colors[k]}"></i>{html.escape(names[k])}</span>' for k in keys) + "</div>"


labels = ["one-file", "file-per-session", "json-rename-75", "json-rename-250"]

# повторы one-file на ФС контейнера: основная матрица + recheck + 3 повтора
rep = {}
for r in pick(container, "one-file") + pick(recheck, "one-file") + pick(repeats, "one-file"):
    rep.setdefault(r["n"], []).append(r["write_p99_ms"])

c_series = [(k, pick(container, k)) for k in labels]
cont_ns = [1, 5, 10, 20, 50, 100, 150, 200]
c_series[0] = ("one-file", sorted(
    [r for r in pick(container, "one-file")] + [r for r in pick(recheck, "one-file") if r["n"] > 100],
    key=lambda r: r["n"]))
c_series += [(k, pick(ev[("container", k.rsplit("-", 1)[1])], k))
             for k in ("event-log-redis", "event-log-kafka")]
c_series += [("one-file-append", pick(append["container"], "one-file-append"))]
chart_c = chart(c_series, "ФС контейнера", cont_ns,
                scatter={n: v for n, v in rep.items() if len(v) > 1})

b_series = [(k, pick(bind, k)) for k in labels] + [("one-file TRUNCATE", pick(trunc, "one-file"))]
b_series += [(k, pick(ev[("macos-bind", k.rsplit("-", 1)[1])], k))
             for k in ("event-log-redis", "event-log-kafka")]
b_series += [("one-file-append", pick(append["macos-bind"], "one-file-append"))]
bind_marks = {r["n"]: f'порча {corrupt_share(r)*100:.0f}%' for r in pick(bind, "one-file") if corrupt_share(r) > 0.01}
chart_b = chart(b_series, "Папка мака (bind-mount)", [1, 5, 10, 20, 50, 100], marks=bind_marks)

# таблица повторов
rep_rows = "".join(
    f"<tr><td>{n}</td><td>{len(v)}</td><td>{min(v):.0f}</td><td>{statistics.median(v):.0f}</td><td>{max(v):.0f}</td></tr>"
    for n, v in sorted(rep.items()) if n >= 20)

corr_rows = "".join(
    f"<tr><td>{r['n']}</td><td>{corrupt_share(r)*100:.0f}%</td><td>{r.get('integrity_bad_dbs', '—')}</td>"
    f"<td>{r['write_p99_ms']:.0f}</td></tr>" for r in pick(bind, "one-file"))
trunc_rows = "".join(
    f"<tr><td>{r['n']}</td><td>{r['write_p99_ms']:.0f}</td><td>{r['rate_ratio']*100:.0f}%</td><td>{r['locked_errors']}</td></tr>"
    for r in pick(trunc, "one-file"))

# ---------- H1: дельты; H2: потоки против процессов ----------
def h1_rows():
    out = []
    for fs, base, extra in (("container", container, recheck + repeats), ("macos-bind", bind, [])):
        of = {}
        for r in pick(base, "one-file") + pick(extra, "one-file"):
            of.setdefault(r["n"], []).append(r)
        fps = {r["n"]: r for r in pick(base, "file-per-session") + pick(recheck if fs == "container" else [], "file-per-session")}
        for r in pick(append[fs], "one-file-append"):
            n = r["n"]
            o = of.get(n, [])
            o_p99 = fmt(statistics.median(x["write_p99_ms"] for x in o)) if o else "—"
            o_cor = (f"{statistics.median(corrupt_share(x) for x in o)*100:.0f}%" if o and fs == "macos-bind" else "")
            out.append(f"<tr><td>{FS_RU0[fs]}</td><td>{n}</td><td>{o_p99}{' · порча ' + o_cor if o_cor else ''}</td>"
                       f"<td>{fmt(r['write_p99_ms'], 1)}</td><td>{fmt(r['late75_frac']*100, 2)}%</td>"
                       f"<td>{fmt(r['rate_ratio']*100)}%</td><td>{dmg_share(r)*100:.1f}%</td>"
                       f"<td>{r['integrity_bad_dbs']}</td><td>{r.get('append_state_mismatches', '—')}</td>"
                       f"<td>{fmt(fps[n]['write_p99_ms'], 1) if n in fps else '—'}</td></tr>")
    return "".join(out)


def dmg_share(r):
    """append: other_errors в этих прогонах — только «file is not a database» (проверено по raw)."""
    return ((r.get("corrupt_errors") or 0) + (r.get("other_errors") or 0)) / r["flushes"] if r["flushes"] else 0


FS_RU0 = {"container": "контейнер", "macos-bind": "папка мака"}


def h2_rows():
    proc = by_n(bind, "one-file")
    out = []
    for r in pick(threads, "one-file-threads"):
        p = proc.get(r["n"])
        out.append(f"<tr><td>{r['n']}</td>"
                   + (f"<td>{corrupt_share(p)*100:.0f}% ({p['corrupt_errors']})</td><td>{p['integrity_bad_dbs']}</td>"
                      if p else "<td>—</td><td>—</td>")
                   + f"<td>{corrupt_share(r)*100:.0f}% ({r['corrupt_errors']})</td><td>{r['integrity_bad_dbs']}</td>"
                   f"<td>{r['locked_errors'] + r['other_errors']}</td><td>{fmt(r['rate_ratio']*100)}%</td></tr>")
    return "".join(out)


# ---------- этап 2: журнал событий ----------
E2E_COLORS = {"redis container": "var(--c6)", "kafka container": "var(--c7)",
              "redis macos-bind": "var(--c6)", "kafka macos-bind": "var(--c7)"}
E2E_NAMES = {"redis container": "Redis, ФС контейнера", "kafka container": "Kafka, ФС контейнера",
             "redis macos-bind": "Redis, база компактора на папке мака",
             "kafka macos-bind": "Kafka, база компактора на папке мака"}
E2E_DASH = {"redis macos-bind": "6 3", "kafka macos-bind": "6 3"}
e2e_series = [(f"{b} {fs}", pick(ev[(fs, b)], f"event-log-{b}"))
              for fs in ("container", "macos-bind") for b in ("redis", "kafka")]
chart_e2e = chart(e2e_series, "Сквозная задержка", cont_ns, ymin=5, ymax=1000,
                  field="e2e_p99_ms", ylabel="p99 «опубликовал → в SQLite», мс",
                  colors=E2E_COLORS, names=E2E_NAMES, dash=E2E_DASH)


def fmt(v, d=0):
    if v is None or v != v:
        return "—"
    return f"{v:.{d}f}"


def by_n(rows, label):
    return {r["n"]: r for r in pick(rows, label)}


# сравнение на одинаковых N: p99 задержки агента и сквозная
cmp_rows = {}
for fs, base, extra in (("container", container, recheck), ("macos-bind", bind, [])):
    of = {}
    for r in pick(base, "one-file") + pick(extra, "one-file") + (pick(repeats, "one-file") if fs == "container" else []):
        of.setdefault(r["n"], []).append(r["write_p99_ms"])
    fps = {}
    for r in pick(base, "file-per-session") + pick(extra, "file-per-session"):
        fps.setdefault(r["n"], []).append(r["write_p99_ms"])
    rd, kf = by_n(ev[(fs, "redis")], "event-log-redis"), by_n(ev[(fs, "kafka")], "event-log-kafka")
    out = []
    for n in sorted(set(of) | set(fps) | set(rd) | set(kf)):
        cell = lambda d: fmt(statistics.median(d[n])) if n in d else "—"
        out.append(f"<tr><td>{n}</td><td>{cell(of)}</td><td>{cell(fps)}</td>"
                   f"<td>{fmt(rd[n]['write_p99_ms'], 1) if n in rd else '—'}</td>"
                   f"<td>{fmt(kf[n]['write_p99_ms'], 1) if n in kf else '—'}</td>"
                   f"<td>{fmt(rd[n]['e2e_p99_ms']) if n in rd else '—'}</td>"
                   f"<td>{fmt(kf[n]['e2e_p99_ms']) if n in kf else '—'}</td></tr>")
    cmp_rows[fs] = "".join(out)

lag_rows = "".join(
    f"<tr><td>{'контейнер' if fs == 'container' else 'папка мака'}</td><td>{b}</td><td>{r['n']}</td>"
    f"<td>{fmt(r['e2e_p50_ms'], 1)}</td><td>{fmt(r['e2e_p99_ms'])}</td><td>{fmt(r['e2e_max_ms'])}</td>"
    f"<td>{fmt(r['lag_events_max'])}</td><td>{fmt(r['drain_ms'])}</td>"
    f"<td>{fmt(r['comp_events_per_batch'], 1)}</td><td>{fmt(r['comp_busy_frac'] * 100, 0)}%</td>"
    f"<td>{r['state_mismatch']}</td><td>{r['other_errors']}</td></tr>"
    for fs in ("container", "macos-bind") for b in ("redis", "kafka")
    for r in pick(ev[(fs, b)], f"event-log-{b}"))

# ---------- H3: окно пачки компактора (Redis) ----------
FS_RU = {"container": "ФС контейнера", "macos-bind": "папка мака"}
W_NS = [20, 50, 100, 200]
W_COLORS = {20: "var(--c2)", 50: "var(--c4)", 100: "var(--c7)", 200: "var(--c1)"}


def wgrid(rows):
    """{(n, W): row}"""
    return {(r["n"], r.get("batch_window_ms", 0)): r for r in rows}


def wchart(fs):
    g = wgrid(batchw[fs])
    ws = sorted({w for _, w in g})
    if not ws:
        return ""
    W, H, L, R, T, B = 300, 220, 44, 12, 22, 40
    y0, y1 = math.log10(5), math.log10(10000)
    X = lambda w: L + (ws.index(w) / max(1, len(ws) - 1)) * (W - L - R)
    Y = lambda v: T + (1 - (math.log10(max(min(v, 10000), 5)) - y0) / (y1 - y0)) * (H - T - B)
    o = [f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="e2e p99 от окна пачки, {FS_RU[fs]}">',
         f'<text x="{L}" y="14" class="ax">{FS_RU[fs]}</text>']
    for v in (10, 100, 1000, 10000):
        o.append(f'<line x1="{L}" x2="{W-R}" y1="{Y(v):.1f}" y2="{Y(v):.1f}" class="grid"/>'
                 f'<text x="{L-6}" y="{Y(v)+4:.1f}" class="ax" text-anchor="end">{v}</text>')
    o.append(f'<line x1="{L}" x2="{W-R}" y1="{Y(75):.1f}" y2="{Y(75):.1f}" class="limit"/>')
    for w in ws:
        o.append(f'<text x="{X(w):.1f}" y="{H-B+16}" class="ax" text-anchor="middle">{w:g}</text>')
    o.append(f'<text x="{(L+W-R)/2}" y="{H-4}" class="ax" text-anchor="middle">окно пачки W, мс</text>')
    for n in W_NS:
        pts = [(X(w), Y(g[(n, w)]["e2e_p99_ms"]), g[(n, w)]) for w in ws if (n, w) in g]
        if not pts:
            continue
        o.append(f'<polyline points="{" ".join(f"{a:.1f},{b:.1f}" for a, b, _ in pts)}" fill="none" stroke="{W_COLORS[n]}" stroke-width="2.5"/>')
        for a, b, r in pts:
            tip = (f'N={n}, W={r.get("batch_window_ms", 0):g} мс: e2e p99 {r["e2e_p99_ms"]:.0f} мс, '
                   f'в коммите {r["comp_busy_frac"]*100:.0f}%, {r["comp_events_per_batch"]:.1f} соб./пачку')
            o.append(f'<circle cx="{a:.1f}" cy="{b:.1f}" r="4" fill="{W_COLORS[n]}"><title>{html.escape(tip)}</title></circle>')
    o.append("</svg>")
    return "".join(o)


def wtable():
    out = []
    for fs in ("container", "macos-bind"):
        g = wgrid(batchw[fs])
        ws = sorted({w for _, w in g})
        for n in W_NS:
            cells = []
            for w in ws:
                r = g.get((n, w))
                if not r:
                    cells.append("<td>—</td>" * 3)
                    continue
                cells.append(f"<td>{fmt(r['e2e_p99_ms'])}</td><td>{fmt(r['comp_busy_frac'] * 100)}%</td>"
                             f"<td>{fmt(r['comp_events_per_batch'], 1)}</td>")
            if any(c != "<td>—</td>" * 3 for c in cells):
                out.append(f"<tr><td>{FS_RU[fs]}</td><td>{n}</td>{''.join(cells)}</tr>")
    return "".join(out)


W_ALL = sorted({r.get("batch_window_ms", 0) for fs in batchw for r in batchw[fs]})
w_head = "".join(f'<th colspan="3">W = {w:g} мс</th>' for w in W_ALL)
w_sub = "".join("<th>e2e p99, мс</th><th>в коммите</th><th>соб./пачку</th>" for _ in W_ALL)
w_legend = '<div class="legend">' + "".join(
    f'<span><i style="background:{W_COLORS[n]}"></i>N = {n}</span>' for n in W_NS) + "</div>"
h3_block = "" if not W_ALL else f"""
<h2>H3: окно пачки компактора</h2>
<p>Компактор после первого события пачки ещё W мс добирает журнал и только потом коммитит; на каждую строку в пачке пишется последний текст. Только Redis Streams, точка 30 с, по одному прогону.</p>
<div class="card"><div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:12px">{wchart("container")}{wchart("macos-bind")}</div>{w_legend}
<p class="note">Сквозная задержка p99 («опубликовал → в SQLite»), лог-шкала; пунктир — 75 мс. Наведите на точку: доля времени компактора в коммитах и событий на пачку.</p></div>
<div class="card scroll"><table><tr><th rowspan="2">ФС базы</th><th rowspan="2">N</th>{w_head}</tr><tr>{w_sub}</tr>{wtable()}</table></div>
<p><b>Окно пачки.</b> Ёмкость компактора (событий на секунду коммита) растёт в 1,5–2,9 раза при W = 75 мс, доля времени в коммитах падает. Это спасает единственную перегруженную точку — папка мака, 200 агентов: без окна очередь дорастает до 16,6 тыс. событий и p99 «в базе» 6,8 с, с окном 25–75 мс компактор успевает, p99 0,86–1,0 с. Там, где компактор успевал и без окна (до 100 агентов), окно только добавляет задержку: на ФС контейнера предсказуемо (p50 ≈ W), на папке мака — нет, крупная пачка даёт долгий коммит. Один прогон на точку; подробности и оговорки — <code>SUMMARY.md</code>, раздел H3.</p>
"""

H1_TEXT = """<p><b>Дельты.</b> На ФС контейнера запись приращения вместо полного текста отодвигает предел одного файла: при 200 агентах p99 146 мс и 96% сбросов против 2,7 с и 36% у полной перезаписи (один прогон каждой); при 20–100 агентах p99 6–38 мс — ниже медианы прежних прогонов полной перезаписи (14–46 мс) и сравнимо с файлом на сессию (7–36 мс); при 150–200 хуже файла на сессию (109–146 против 65–84 мс). Для 150 и 200 агентов у полной перезаписи один прогон. На папке мака дельты не спасают: при 5 и 20 агентах 2,9% записей падают с «file is not a database», битые базы при 20 и 100 агентах, склейка чанков не совпала с текстом у 2–77% агентов; при 50–100 агентах p99 349–650 мс — та же ФС, что тормозит и файл на сессию. Порчи меньше, чем у полной перезаписи (58–98%), но она есть.</p>"""
H2_TEXT = """<p><b>Потоки.</b> Те же 5, 10 и 20 писателей одного файла в WAL на папке мака, но в одном процессе — ноль ошибок порчи и ноль битых баз; процессами при тех же N — 58–95% записей с порчей и битая база в каждой точке. Это сильный довод, что ломается именно межпроцессная общая память WAL (<code>-shm</code>) на этой ФС: внутри процесса SQLite делит одно отображение <code>-shm</code> между соединениями. При 20 потоках выполнено 91% сбросов — GIL и один процесс снижают реальную конкуренцию; при 5 и 10 — 100%, а порчи всё равно нет.</p>"""

h12_block = "" if not (append["container"] or append["macos-bind"] or threads) else f"""
<h2>H1: дельты вместо полной перезаписи</h2>
<p>Режим <code>one-file-append</code>: та же общая SQLite в WAL, но каждый сброс — INSERT только приращения (200–500 Б) в таблицу чанков вместо UPDATE полного текста (до 64 КБ); один коммит на сброс. Линия «один файл, дельты» — на обоих графиках выше. Точка 30 с, один прогон.</p>
<div class="card scroll"><table><tr><th>ФС</th><th>N</th><th>один файл, p99, мс (медиана прогонов)</th><th>дельты, p99, мс</th><th>&gt;75 мс</th><th>выполнено</th><th>ошибки порчи*</th><th>битых баз</th><th>склейка ≠ (агентов)</th><th>файл на сессию, p99</th></tr>{h1_rows()}</table><p class="note">* «database disk image is malformed» + «file is not a database», доля записей. Один файл на папке мака — p99 по записям, 58–98% которых вернули ошибку порчи сразу, поэтому быстрые.</p></div>
{H1_TEXT}
<h2>H2: потоки против процессов</h2>
<p>Режим <code>one-file-threads</code>: схема одного файла (UPDATE полного текста, WAL), но N агентов — потоки одного процесса, у каждого своё соединение. Только папка мака. Задержки из-за GIL не сравниваем — смотрим порчу.</p>
<div class="card scroll"><table><tr><th rowspan="2">N</th><th colspan="2">процессы (этап 1)</th><th colspan="2">потоки</th><th rowspan="2">locked/др.</th><th rowspan="2">выполнено</th></tr><tr><th>записей с порчей</th><th>битых баз</th><th>записей с порчей</th><th>битых баз</th></tr>{h2_rows()}</table></div>
{H2_TEXT}
"""

CSS = """
:root{--bg:#f7f6f2;--card:#fff;--ink:#1d1d1b;--mute:#6b6a64;--line:#e4e2db;
--c1:#d1495b;--c2:#2e86ab;--c3:#9b6a1f;--c4:#3f8f5a;--c5:#7b4fa0;--c6:#0f766e;--c7:#c2410c;--c8:#57534e;--warn:#b3261e}
@media (prefers-color-scheme:dark){:root:not([data-theme=light]){--bg:#151514;--card:#1f1f1d;--ink:#ecebe6;
--mute:#a3a29b;--line:#34332f;--c1:#ff6b7f;--c2:#5fb6dd;--c3:#e0a548;--c4:#6cc58b;--c5:#b58ae0;--c6:#4fd1c5;--c7:#fb923c;--c8:#d6d3d1;--warn:#ff8a80}}
:root[data-theme=dark]{--bg:#151514;--card:#1f1f1d;--ink:#ecebe6;--mute:#a3a29b;--line:#34332f;
--c1:#ff6b7f;--c2:#5fb6dd;--c3:#e0a548;--c4:#6cc58b;--c5:#b58ae0;--c6:#4fd1c5;--c7:#fb923c;--c8:#d6d3d1;--warn:#ff8a80}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);
font:16px/1.55 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif}
main{max-width:880px;margin:0 auto;padding:32px 16px 64px}
h1{font-size:30px;line-height:1.2;margin:0 0 6px}h2{font-size:21px;margin:40px 0 10px}
.sub{color:var(--mute);margin:0 0 24px}
.hero{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;margin:20px 0}
.hero div{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px 16px}
.hero b{display:block;font-size:28px;line-height:1.1}.hero span{color:var(--mute);font-size:14px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:16px;margin:12px 0}
svg{width:100%;height:auto;display:block}.grid{stroke:var(--line)}.ax{fill:var(--mute);font-size:12px}
.limit{stroke:var(--ink);stroke-dasharray:5 4;opacity:.6}.lim{fill:var(--mute);font-size:12px}
.warn{fill:var(--warn);font-size:11px;font-weight:600}
.legend{display:flex;flex-wrap:wrap;gap:6px 16px;font-size:13px;color:var(--mute);margin-top:8px}
.legend i{display:inline-block;width:12px;height:3px;margin-right:6px;vertical-align:middle}
table{border-collapse:collapse;width:100%;font-size:14px}th,td{padding:6px 8px;border-bottom:1px solid var(--line);text-align:right}
th:first-child,td:first-child{text-align:left}th{color:var(--mute);font-weight:500}
.scroll{overflow-x:auto}.note{color:var(--mute);font-size:14px}
ol li,ul li{margin:6px 0}code{font-size:.92em}
"""

N5 = pick(bind, "one-file")
page = f"""<!doctype html><html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Один файл, много агентов</title>
<style>{CSS}</style></head><body><main>
<h1>Один файл, много агентов</h1>

<div class="hero">
<div><b>5 агентов</b><span>и общая SQLite в WAL на папке мака портится: 58–98% записей «database disk image is malformed»</span></div>
<div><b>100–150 агентов</b><span>предел одного файла на ФС контейнера: до 100 медиана p99 по 5 прогонам ~43 мс, к 200 — 2,7 с и треть сбросов</span></div>
<div><b>244 мс</b><span>p99 уже при 10 агентах после отката на TRUNCATE: порча уходит, темп тоже</span></div>
<div><b>≤ 19 мс</b><span>p99 публикации события при любом N на обеих ФС (Redis); общий файл в тех же условиях — до 2,7 с или порча</span></div>
<div><b>0 ошибок</b><span>«database is locked» на ФС контейнера до 200 агентов: busy_timeout превращает конкуренцию в задержку</span></div>
</div>

<h2>Что моделируем</h2>
<p>N отдельных процессов-«агентов». Каждый раз в 75 мс целиком перезаписывает растущее сообщение (1 КБ → до 64 КБ) — так OpenResearch сбрасывает поток вывода в общий <code>orx.db</code> (WAL, busy_timeout 5 с). Сравниваем с дроблением хранилища: файл SQLite на сессию и JSON-файлы с атомарным rename (как OpenScience, 250 мс). Точка — 30 с, первые 5 отброшены. Две файловые системы: собственная ФС контейнера и папка macOS, проброшенная в Docker Desktop.</p>

<h2>ФС контейнера</h2>
<div class="card">{chart_c}{legend(labels + ["one-file-append", "event-log-redis", "event-log-kafka"])}
<p class="note">Полые точки — повторные прогоны одного файла (до 5 на точку). Два первых прогона дали выбросы 161–261 мс при N=50–100; три повтора — 14–49 мс. По медиане один файл держит 75 мс до 100 агентов, ломается между 100 и 200.</p></div>
<div class="card scroll"><table><tr><th>один файл, N</th><th>прогонов</th><th>p99 min, мс</th><th>медиана</th><th>max</th></tr>{rep_rows}</table></div>

<h2>Папка мака</h2>
<div class="card">{chart_b}{legend(labels + ["one-file TRUNCATE", "one-file-append", "event-log-redis", "event-log-kafka"])}
<p class="note">Красные подписи — доля записей с ошибкой порчи у общего файла в WAL. Файл на сессию и JSON на той же ФС ошибок не дали.</p></div>
<div class="card scroll"><table><tr><th>один файл WAL, N</th><th>записей с порчей</th><th>битых баз после точки</th><th>p99, мс</th></tr>{corr_rows}</table></div>
<div class="card scroll"><table><tr><th>один файл TRUNCATE, N</th><th>p99, мс</th><th>выполнено сбросов</th><th>locked</th></tr>{trunc_rows}</table></div>

<h2>Этап 2 — журнал событий</h2>
<p>Агенты не трогают файл: каждый сброс — событие с полным текстом в брокер (Redis Streams или Kafka, один узел на той же машине). Один процесс-компактор читает журнал пачками, оставляет последнюю версию каждой строки и пишет в SQLite одной транзакцией. Пунктир на графиках выше — время публикации у агента: то, что его тормозит. Ниже — сколько ждёт свежесть состояния: от начала публикации до коммита компактора.</p>
<div class="card">{chart_e2e}{legend(["redis container", "kafka container"], E2E_COLORS, E2E_NAMES)}
<p class="note">Сплошные линии — база компактора на ФС контейнера, пунктир — на папке мака. Данные брокеров в обоих случаях на ФС контейнера.</p></div>
<div class="card scroll"><p class="note">ФС контейнера: p99, мс, на одинаковых N. Один файл и файл на сессию — медиана прогонов этапа 1.</p>
<table><tr><th>N</th><th>один файл</th><th>файл на сессию</th><th>Redis, публикация</th><th>Kafka, публикация</th><th>Redis, сквозная</th><th>Kafka, сквозная</th></tr>{cmp_rows["container"]}</table></div>
<div class="card scroll"><p class="note">Папка мака: то же. У одного файла здесь 58–98% записей — ошибка порчи.</p>
<table><tr><th>N</th><th>один файл</th><th>файл на сессию</th><th>Redis, публикация</th><th>Kafka, публикация</th><th>Redis, сквозная</th><th>Kafka, сквозная</th></tr>{cmp_rows["macos-bind"]}</table></div>
<div class="card scroll"><p class="note">Компактор: сквозная задержка, макс. очередь в журнале, дозапись после остановки агентов, событий на пачку, доля времени в коммитах, строк с расхождением итогового состояния, ошибок публикации.</p>
<table><tr><th>ФС базы</th><th>брокер</th><th>N</th><th>e2e p50</th><th>p99</th><th>max</th><th>очередь max</th><th>дозапись, мс</th><th>соб./пачку</th><th>в коммите</th><th>расхожд.</th><th>ошибки</th></tr>{lag_rows}</table></div>
<p><b>Журнал событий.</b> Публикация события не тормозит агента ни на одной ФС: p99 у Redis ≤ 15,3 мс до 200 агентов на ФС контейнера и ≤ 19 мс на папке мака, у Kafka ≤ 14 мс до 100 агентов и 60–76 мс при 150–200. Ни одной ошибки, ни одной битой базы, итоговое состояние в SQLite совпало с последним опубликованным текстом во всех 28 точках. Цена переезжает в свежесть: на ФС контейнера сквозная задержка p99 держится в 11–27 мс до 50 агентов у обоих брокеров (у Redis — до 150), на папке мака растёт до 167–202 мс при 50 агентах и 349–521 мс при 100, а компактор при 100 агентах сидит в коммитах 79% времени — следующий шаг нагрузки упрётся в него. Схлопывание почти не работает: на ФС контейнера компактор забирает 1–4 события за раз (18 у Redis при 200 агентах), на одну записанную строку приходится ≤ 1,04 события; на папке мака при 100 агентах — 1,3–1,4. Т. е. выигрыш даёт сам один писатель, а не пакетирование.</p>

{h3_block}
{h12_block}
<h2>Что это значит для доклада</h2>
<ol>
<li><b>Дробление лечит конкуренцию.</b> На ФС контейнера файл на сессию держит 200 агентов с p99 84 мс, общий файл — 2,7 с и лишь 36% сбросов.</li>
<li><b>Хранилище ломает среда.</b> Тот же код на папке мака даёт порчу общей базы уже при 5 процессах — живая иллюстрация истории OpenHuman (PR #2455, откат с WAL после десятков тысяч ошибок -shm на Windows и macOS).</li>
<li><b>Откат стоит темпа.</b> TRUNCATE убирает порчу, но при 10 агентах p99 уже 244 мс, при 50 выполняется четверть сбросов.</li>
<li><b>Журнал событий выносит хранилище из пути агента.</b> Тот же поток, опубликованный в брокер и записанный в SQLite одним компактором, на папке мака даёт ноль порчи и p99 публикации ≤ 19 мс; платим свежестью состояния — до полсекунды при 100 агентах.</li>
<li><b>Частота записи — тоже рычаг.</b> На папке мака JSON на 250 мс держит 100 агентов (p99 22 мс), на 75 мс — нет (324 мс).</li>
</ol>

<h2>Чего данные не показывают</h2>
<ul class="note">
<li>Это не OpenResearch: схема, размеры сообщений и читатели (<code>orx serve</code>) не воспроизведены; клиент на Python, а не на Rust.</li>
<li>Процессов больше, чем ядер (8) при N ≥ 50: часть задержки даёт планировщик; контроль — файл на сессию на тех же N.</li>
<li>Механизм порчи на bind-mount точно не установлен: простые проверки fcntl, mmap и pwrite/pread между процессами прошли; H2 (потоки без порчи) указывает на межпроцессную общую память WAL, но это одна серия по одному прогону.</li>
<li>Docker Desktop на macOS, одна машина. На другом железе цифры будут другими, форма зависимости — вопрос повторения.</li>
<li>Этап 2: брокер, компактор и агенты на одной машине; Redis без персистентности, Kafka без fsync на сообщение; компактор один, на Python; событие несёт полный текст (~29 КБ в среднем, ~76 МБ/с при 200 агентах), дельты не мерились; чтение состояния интерфейсом и отказ брокера/компактора не проверялись. Один прогон на точку.</li>
<li>Мак один раз засыпал во время прогонов: первая матрица выброшена, все точки проверены на разрывы > 5 с и расхождение часов.</li>
</ul>
<p class="note">Данные и код: этот репозиторий · подробные выводы: <code>results/SUMMARY.md</code> · этап 2 — <code>eventlog.py</code>, <code>brokers.sh</code>.</p>
</main></body></html>"""

out = os.path.join(RES, "report.html")
open(out, "w").write(page)
print(out)
