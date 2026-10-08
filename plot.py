#!/usr/bin/env python3
"""SVG-график без matplotlib: строки — файловые системы, столбцы —
(1) задержка записи p50 (пунктир) и p99 (сплошная), лог-шкала;
(2) доля сбросов, чья запись не уложилась в 75 мс.
Одна линия на режим. Наведение на точку — подсказка с числом (<title>).

  python3 plot.py results/2026-10-04-container results/2026-10-04-macos-bind
  -> results/latency-vs-n.svg
"""
import json
import math
import os
import sys
from xml.sax.saxutils import escape

MODES = [  # фиксированный порядок категориальных слотов
    ("one-file", "one-file (общий orx.db)", "--s1"),
    ("file-per-session", "file-per-session", "--s2"),
    ("json-rename-250", "json-rename, 250 мс", "--s3"),
    ("json-rename-75", "json-rename, 75 мс", "--s4"),
]
FS_TITLES = {"container": "ФС контейнера (ext4 в VM Docker Desktop)",
             "macos-bind": "bind-mount с macOS (Docker Desktop)"}

W, H = 1120, 905
PAD_L, PAD_R, PAD_T = 70, 30, 175
COL_GAP, ROW_GAP = 90, 120
PW = (W - PAD_L - PAD_R - COL_GAP) / 2
PH = (H - PAD_T - 60 - ROW_GAP) / 2


def load(d):
    with open(os.path.join(d, "summary.json")) as f:
        return json.load(f)


def main():
    dirs = sys.argv[1:]
    here = os.path.dirname(os.path.abspath(__file__))
    rows = []
    for d in dirs:
        data = load(d)
        rows.append((data[0]["fs"] if data else os.path.basename(d), data))
    ns = sorted({r["n"] for _, data in rows for r in data})
    xs_idx = {n: i for i, n in enumerate(ns)}

    all_lat = [r[k] for _, data in rows for r in data
               for k in ("write_p50_ms", "write_p99_ms") if r[k] > 0]
    y_min = 10 ** math.floor(math.log10(min(all_lat)))
    y_max = 10 ** math.ceil(math.log10(max(all_lat)))
    late_max = max([r["late75_frac"] for _, data in rows for r in data] + [0.01])
    late_top = next(v for v in (0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0) if v >= late_max)

    out = []
    a = out.append
    a(f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" '
      f'width="{W}" height="{H}" font-family="system-ui,-apple-system,Segoe UI,Roboto,sans-serif">')
    a("""<style>
svg{--bg:#fcfcfb;--ink:#1f1f1e;--ink2:#5f5e58;--grid:#e6e5e0;--ref:#9a998f;
--s1:#2a78d6;--s2:#eb6834;--s3:#1baf7a;--s4:#eda100}
@media (prefers-color-scheme: dark){svg{--bg:#1a1a19;--ink:#ffffff;--ink2:#c3c2b7;
--grid:#33332f;--ref:#7d7c74;--s1:#3987e5;--s2:#d95926;--s3:#199e70;--s4:#c98500}}
.bg{fill:var(--bg)} .t{fill:var(--ink)} .t2{fill:var(--ink2)}
.grid{stroke:var(--grid);stroke-width:1} .ref{stroke:var(--ref);stroke-width:1.5;stroke-dasharray:6 4}
.ln{fill:none;stroke-width:2;stroke-linejoin:round;stroke-linecap:round}
.dash{stroke-dasharray:5 4}
.pt{stroke:var(--bg);stroke-width:2}
</style>""")
    a(f'<rect class="bg" width="{W}" height="{H}"/>')
    a('<text class="t" x="70" y="34" font-size="20" font-weight="600">'
      'Запись стриминг-вывода: задержка и опоздания против числа агентов N</text>')
    a('<text class="t2" x="70" y="56" font-size="13">Синтетика по шаблону OpenResearch '
      '(сброс полного сообщения раз в 75 мс). Сплошная — p99, пунктир — p50. '
      'Серая штриховая — 75 мс.</text>')
    # легенда
    lx = 70
    for key, name, var in MODES:
        a(f'<line x1="{lx}" y1="82" x2="{lx+24}" y2="82" class="ln" style="stroke:var({var})"/>')
        a(f'<circle cx="{lx+12}" cy="82" r="4" class="pt" style="fill:var({var})"/>')
        a(f'<text class="t" x="{lx+32}" y="86" font-size="13">{escape(name)}</text>')
        lx += 32 + 8 * len(name) + 30

    def xpos(x0, n):
        return x0 + 20 + (PW - 40) * xs_idx[n] / max(1, len(ns) - 1)

    for ri, (fs, data) in enumerate(rows):
        y0 = PAD_T + ri * (PH + ROW_GAP)
        for ci in range(2):
            x0 = PAD_L + ci * (PW + COL_GAP)
            title = ("Задержка записи, мс (лог. шкала)" if ci == 0
                     else "Доля записей дольше 75 мс")
            if ci == 0:
                a(f'<text class="t" x="{x0}" y="{y0-58}" font-size="16" font-weight="700">'
                  f'{escape(FS_TITLES.get(fs, fs))}</text>')
                bad = [r for r in data if r.get("corrupt_errors", 0) > 0]
                if bad:
                    fr = [r["corrupt_errors"] / max(1, r["flushes"]) for r in bad]
                    labels = sorted({r["label"] for r in bad})
                    note = (f"⚠ {', '.join(labels)}: {min(fr)*100:.0f}–{max(fr)*100:.0f} % записей "
                            f"при N≥{min(r['n'] for r in bad)} — «database disk image is malformed»; "
                            f"их задержки не сравнимы (ошибка возвращается быстро)")
                    a(f'<text class="t" x="{x0}" y="{y0-36}" font-size="13" '
                      f'font-weight="600">{escape(note)}</text>')
            a(f'<text class="t" x="{x0}" y="{y0-12}" font-size="14" font-weight="600">{escape(title)}</text>')
            if ci == 0:
                ly0, ly1 = math.log10(y_min), math.log10(y_max)

                def yp(v):
                    v = max(v, y_min)
                    return y0 + PH - PH * (math.log10(v) - ly0) / (ly1 - ly0)
                ticks = [10 ** e for e in range(int(ly0), int(ly1) + 1)]
                fmt = lambda v: (f"{v:g}")
            else:
                def yp(v):
                    return y0 + PH - PH * v / late_top
                ticks = [late_top * i / 4 for i in range(5)]
                fmt = lambda v: f"{v*100:g}%"
            for t in ticks:
                y = yp(t)
                a(f'<line class="grid" x1="{x0}" x2="{x0+PW}" y1="{y:.1f}" y2="{y:.1f}"/>')
                a(f'<text class="t2" x="{x0-8}" y="{y+4:.1f}" font-size="12" text-anchor="end">{fmt(t)}</text>')
            if ci == 0 and y_min <= 75 <= y_max:
                y = yp(75)
                a(f'<line class="ref" x1="{x0}" x2="{x0+PW}" y1="{y:.1f}" y2="{y:.1f}"/>')
                a(f'<text class="t2" x="{x0+PW-4}" y="{y-6:.1f}" font-size="12" text-anchor="end">75 мс</text>')
            for n in ns:
                a(f'<text class="t2" x="{xpos(x0, n):.1f}" y="{y0+PH+18}" font-size="12" text-anchor="middle">{n}</text>')
            a(f'<text class="t2" x="{x0+PW/2}" y="{y0+PH+36}" font-size="12" text-anchor="middle">N агентов</text>')

            metrics = ([("write_p99_ms", ""), ("write_p50_ms", " dash")] if ci == 0
                       else [("late75_frac", "")])
            for key, name, var in MODES:
                pts = sorted((r for r in data if r["label"] == key), key=lambda r: r["n"])
                if not pts:
                    continue
                for m, cls in metrics:
                    path = " ".join(f"{'M' if i == 0 else 'L'}{xpos(x0, r['n']):.1f},{yp(r[m]):.1f}"
                                    for i, r in enumerate(pts))
                    a(f'<path class="ln{cls}" style="stroke:var({var})" d="{path}"/>')
                    if cls:
                        continue
                    for r in pts:
                        v = r[m]
                        tip = (f"{name}, N={r['n']}: p50 {r['write_p50_ms']:.1f} мс, "
                               f"p99 {r['write_p99_ms']:.1f} мс, max {r['write_max_ms']:.0f} мс, "
                               f">75 мс {r['late75_frac']*100:.2f}%, locked {r['locked_errors']}, "
                               f"malformed {r.get('corrupt_errors', 0)} из {r['flushes']}")
                        a(f'<circle class="pt" cx="{xpos(x0, r["n"]):.1f}" cy="{yp(v):.1f}" r="4.5" '
                          f'style="fill:var({var})"><title>{escape(tip)}</title></circle>')
    a("</svg>")
    dest = os.path.join(here, "results", "latency-vs-n.svg")
    with open(dest, "w") as f:
        f.write("\n".join(out))
    print(dest)


if __name__ == "__main__":
    main()
