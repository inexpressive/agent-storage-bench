#!/usr/bin/env python3
"""Уровень 1: замер настоящего кода записи трёх сред, агент без LLM.

N процессов-агентов одновременно, каждый — обвязка над кодом среды
(openresearch_bench_real.rs, openscience_agent.ts, openhuman_agent.rs).
После точки — сверка с хранилищем: что агент получил как «успех» и что лежит.

  python3 run_real.py --env openresearch --fs container --dir ~/bench-state/tmp \
      --ns 2,5,10,20,50,100,150,200 --dur 30 --reps 3

Пишет results/real-<env>-<fs>-<дата>/summary.csv (+ raw JSON агентов в .tar.gz).
"""
import argparse, csv, datetime, glob, json, os, shutil, sqlite3, statistics, subprocess, sys, tarfile, tempfile, time

HERE = os.path.dirname(os.path.abspath(__file__))
REAL = os.path.expanduser(os.environ.get("BENCH_REAL_SRC", "~/bench-real-src"))
WARMUP = 5.0


def pct(xs, q):
    if not xs:
        return None
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))]


def launch_openresearch(i, d, dur, out):
    tb = os.environ["OR_TEST_BIN"]
    env = dict(os.environ, BENCH_ID=f"a{i}", BENCH_DUR=str(dur), BENCH_OUT=out,
               ORX_DATA_DIR=os.path.join(d, "data"), XDG_CONFIG_HOME=os.path.join(d, "cfg"), HOME=os.path.join(d, "home"))
    return subprocess.Popen([tb, "bench_real::tests::agent", "--exact", "--ignored", "--nocapture", "--test-threads=1"],
                            env=env, stdout=subprocess.DEVNULL, stderr=open(out + ".err", "w"))


def launch_openscience(i, d, dur, out):
    cli = os.path.join(REAL, "OpenScience", "backend", "cli")
    env = dict(os.environ, OPENSCIENCE_DATA_DIR=os.path.join(d, "data"), OPENSCIENCE_CONFIG_DIR=os.path.join(d, "cfg"),
               OPENSCIENCE_TEST_HOME=os.path.join(d, "home"))
    return subprocess.Popen([os.path.expanduser("~/.bun/bin/bun"), "run", "bench-real/openscience_agent.ts", f"a{i}", str(dur), out],
                            cwd=cli, env=env, stdout=subprocess.DEVNULL, stderr=open(out + ".err", "w"))


def launch_openhuman(i, d, dur, out):
    tb = os.environ["OH_TEST_BIN"]
    env = dict(os.environ, BENCH_ID=f"a{i}", BENCH_DUR=str(dur), BENCH_OUT=out, BENCH_WS=os.path.join(d, "data"))
    return subprocess.Popen([tb, "agent", "--exact", "--ignored", "--nocapture", "--test-threads=1"],
                            env=env, stdout=subprocess.DEVNULL, stderr=open(out + ".err", "w"))


def check_openresearch(d, agents):
    db = os.path.join(d, "data", "orx.db")
    con = sqlite3.connect(db)
    try:
        integrity = con.execute("PRAGMA integrity_check").fetchone()[0]
        rows = {r[0]: r[1] for r in con.execute("SELECT id, parts_json FROM chat_messages")}
    except sqlite3.DatabaseError as e:
        # база на папке мака бывает битой целиком (7.10, N = 2): точка — порча,
        # подтверждённое считается потерянным, прогон идёт дальше
        integrity, rows = f"unreadable: {e}", {}
    con.close()
    acked = missing = stale = 0
    for a in agents:
        for mid, ln in a["last"].items():
            acked += 1
            if mid not in rows:
                missing += 1
            elif len(json.loads(rows[mid])[0].get("text", "")) < ln:
                stale += 1
    return dict(integrity=integrity, acked=acked, missing=missing, stale=stale)


def check_openscience(d, agents):
    st = os.path.join(d, "data", "storage")
    acked = missing = stale = 0
    for a in agents:
        acked += 1
        f = os.path.join(st, "part", f"m_{a['id']}_{a['msg']}", f"p_{a['id']}_{a['msg']}.json")
        if not os.path.exists(f):
            missing += 1
        elif len(json.load(open(f))["text"]) < a["lastLen"]:
            stale += 1
    shared = json.load(open(os.path.join(st, "bench", "shared.json"))) if os.path.exists(os.path.join(st, "bench", "shared.json")) else {}
    lost_shared = sum(max(0, a["sharedAcked"] - shared.get(a["id"], 0)) for a in agents)
    return dict(integrity="ok", acked=acked, missing=missing, stale=stale,
                shared_acked=sum(a["sharedAcked"] for a in agents), shared_lost=lost_shared)


def check_openhuman(d, agents):
    sys.path.insert(0, HERE)
    from openhuman_check import check  # noqa: E402 — схема зависит от их миграций, сверка вынесена
    return check(d, agents)


ENVS = {
    "openresearch": (launch_openresearch, check_openresearch),
    "openscience": (launch_openscience, check_openscience),
    "openhuman": (launch_openhuman, check_openhuman),
}


def lat_of(env, a, dur):
    """Задержки после прогрева: [(t, ms)] у Rust-обвязок, у OpenScience — ms подряд."""
    if env == "openscience":
        xs = a["partLat"]
        return xs[int(len(xs) * WARMUP / dur):], a.get("sharedLat", [])
    return [ms for t, ms in a["lat"] if t >= WARMUP], []


def point(env, n, base, dur):
    launch, check = ENVS[env]
    d = tempfile.mkdtemp(prefix=f"{env}-{n}-", dir=base)
    for sub in ("data", "cfg", "home"):
        os.makedirs(os.path.join(d, sub), exist_ok=True)
    outs = [os.path.join(d, f"a{i}.json") for i in range(n)]
    t0 = time.time()
    ps = [launch(i, d, dur, o) for i, o in enumerate(outs)]
    for p in ps:
        try:
            p.wait(timeout=dur + 180)
        except subprocess.TimeoutExpired:
            p.kill()
    agents, crashed = [], 0
    for o in outs:
        try:
            agents.append(json.load(open(o)))
        except Exception:
            crashed += 1
    res = check(d, agents) if agents else {}
    lat, shared = [], []
    errs = 0
    for a in agents:
        l, s = lat_of(env, a, dur)
        lat += l; shared += s
        errs += len(a.get("errs", [])) + a.get("partErr", 0) + a.get("sharedErr", 0)
    flushes_expected = n * (dur - WARMUP) / (0.075 if env == "openresearch" else 0.25)
    row = dict(env=env, n=n, wall_s=round(time.time() - t0, 1), crashed=crashed, errors=errs,
               flushes=len(lat), flush_share=round(len(lat) / flushes_expected, 3) if env != "openhuman" else None,
               p50_ms=round(pct(lat, .5), 1) if lat else None, p99_ms=round(pct(lat, .99), 1) if lat else None,
               max_ms=round(max(lat), 1) if lat else None,
               shared_p99_ms=round(pct(shared, .99), 1) if shared else None, **res)
    return row, d


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", required=True, choices=ENVS)
    ap.add_argument("--fs", required=True)
    ap.add_argument("--dir", required=True)
    ap.add_argument("--ns", default="2,5,10,20,50,100,150,200")
    ap.add_argument("--dur", type=float, default=30)
    ap.add_argument("--reps", type=int, default=3)
    a = ap.parse_args()
    base = os.path.expanduser(a.dir)
    os.makedirs(base, exist_ok=True)
    stamp = datetime.datetime.now().strftime("%Y-%m-%d-%H%M")
    res_dir = os.path.join(HERE, "..", "results", f"real-{a.env}-{a.fs}-{stamp}")
    os.makedirs(res_dir, exist_ok=True)
    rows = []
    with tarfile.open(os.path.join(res_dir, "raw.tar.gz"), "w:gz") as tar:
        for r in range(a.reps):
            for n in [int(x) for x in a.ns.split(",")]:
                row, d = point(a.env, n, base, a.dur)
                row["rep"] = r + 1
                rows.append(row)
                print(json.dumps(row, ensure_ascii=False), flush=True)
                for f in glob.glob(os.path.join(d, "a*.json*")):
                    tar.add(f, arcname=f"r{r + 1}/n{n}/{os.path.basename(f)}")
                if row.get("integrity") not in (None, "ok"):
                    shutil.copytree(d, os.path.join(os.path.expanduser("~/bench-state/corrupt"), os.path.basename(d)), dirs_exist_ok=True)
                shutil.rmtree(d, ignore_errors=True)
                with open(os.path.join(res_dir, "summary.csv"), "w", newline="") as f:
                    keys = list(dict.fromkeys(k for x in rows for k in x))
                    w = csv.DictWriter(f, fieldnames=keys); w.writeheader(); w.writerows(rows)


if __name__ == "__main__":
    main()
