#!/usr/bin/env python3
"""Этап 2: «журнал событий» вместо общего файла.

N агентов (процессов) с тем же шаблоном, что в bench.py (сброс каждые 75 мс,
растущее сообщение, новое сообщение каждые 160 сбросов), но сброс — это не
запись в SQLite, а СОБЫТИЕ в брокер с полным текстом сообщения. Один
процесс-компактор читает журнал пачками, схлопывает события одной строки
(последнее побеждает) и пишет состояние в SQLite одной транзакцией на пачку —
единственный писатель, конкуренции за файл нет.

Брокеры:
  event-log-redis  Redis Streams: XADD (агент), XREAD BLOCK пачками
                   (компактор), XTRIM MINID после коммита — журнал подрезается.
  event-log-kafka  Kafka (KRaft, один узел), один топик с одной партицией на
                   точку; продюсер librdkafka (confluent-kafka), acks=1,
                   linger.ms=0, агент ждёт подтверждения доставки.

Метрики:
  агент     — write_* (= время публикации до подтверждения брокера), те же поля,
              что у bench.summarize: p50/p99, late75, rate_ratio, долг, ошибки;
  сквозная  — e2e_*: от начала публикации до COMMIT компактора в SQLite
              (CLOCK_MONOTONIC общий для процессов одной машины; время по
              time.time() записывается параллельно — расхождение = контроль часов);
  компактор — пачки, события/строки на пачку, доля времени в коммите,
              drain_ms — сколько компактор дописывал после остановки агентов,
              lag_events_max — максимум неразобранных событий (опрос раз в 0,25 с).

Окно пачки (--batch-window-ms W, гипотеза H3): компактор, получив первое
событие пачки, ещё W мс добирает журнал и только потом коммитит; в пачке на
каждую строку (session, turn) пишется только последний текст. W = 0 — прежнее
поведение: коммит сразу после одного чтения журнала.

Нужны redis-py и confluent-kafka — запускать python-ом из
~/bench-tools/env (см. README.md, «Этап 2»).
"""
import argparse
import gzip
import json
import multiprocessing as mp
import os
import random
import shutil
import sqlite3
import sys
import time
import uuid

import bench

REDIS_HOST, REDIS_PORT = "127.0.0.1", 6379
KAFKA_BOOTSTRAP = "127.0.0.1:9092"


# ---------------------------------------------------------------- издатели

class RedisPub:
    def __init__(self, stream):
        import redis
        self.r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
        self.r.ping()
        self.stream = stream

    def publish(self, fields, body):
        f = dict(fields)
        f["b"] = body
        self.r.xadd(self.stream, f)

    def close(self):
        self.r.close()


class KafkaPub:
    def __init__(self, topic, cfg):
        from confluent_kafka import Producer
        self.p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "acks": cfg["kafka_acks"],
                           "linger.ms": cfg["kafka_linger_ms"], "message.max.bytes": 1000000,
                           "enable.idempotence": False})
        self.topic = topic
        self.p.list_topics(topic, timeout=10)  # метаданные заранее, до старта
        self.err = None
        self.delivered = False
        self.wait_mode = cfg["kafka_wait"]

    def _cb(self, err, msg):
        if err is not None or self.wait_mode == "each":  # в async ошибка не затирается
            self.err = err
        self.delivered = True

    def publish(self, fields, body):
        hdr = json.dumps(fields, separators=(",", ":")).encode()
        if self.wait_mode == "each":
            self.err = None
        self.delivered = False
        while True:  # async: локальная очередь продюсера может переполниться
            try:
                self.p.produce(self.topic, value=hdr + b"\n" + body.encode(),
                               key=fields["s"].encode(), on_delivery=self._cb)
                break
            except BufferError:
                self.p.poll(0.01)
        if self.wait_mode == "each":
            left = self.p.flush(10)
            if left:
                raise TimeoutError("kafka delivery timeout")
            if self.err is not None:
                raise IOError(str(self.err))
        else:
            # async: агент не ждёт подтверждения — только обслуживает колбэки.
            # Время публикации = вызов produce; e2e по-прежнему до COMMIT
            # компактора. Ошибку доставки увидим на одном из следующих сбросов.
            self.p.poll(0)
            if self.err:
                raise IOError(str(self.err))

    def close(self):
        self.p.flush(5)


def make_pub(broker, chan, cfg):
    return RedisPub(chan) if broker == "redis" else KafkaPub(chan, cfg)


# ---------------------------------------------------------------- агент

def agent(idx, cfg, start_at, out_q):
    rnd = random.Random(cfg["seed"] * 1000 + idx)
    session = f"s{idx:03d}"
    pub = make_pub(cfg["broker"], cfg["chan"], cfg)
    alphabet = "abcdefghijklmnopqrstuvwxyz      .,\n"
    pool = "".join(rnd.choice(alphabet) for _ in range(cfg["cap"] + 4096))
    interval = cfg["interval_ms"] / 1000.0
    t0 = start_at + rnd.uniform(0, interval)
    end_at = start_at + cfg["duration"]
    recs = []
    turn, size, flushes_in_turn, n_flush, n_ok, debt_acc = 0, cfg["start_size"], 0, 0, 0, 0.0
    last_size = {}  # turn -> размер последнего успешно опубликованного текста
    while time.monotonic() < t0:
        time.sleep(min(0.05, max(0.0, t0 - time.monotonic())))
    prev_start, next_at, first_start = None, t0, None
    while True:
        now = time.monotonic()
        if now >= end_at:
            break
        if now < next_at:
            time.sleep(next_at - now)
        ts = time.monotonic()
        if first_start is None:
            first_start = ts
        new_turn = (turn == 0) or (flushes_in_turn >= cfg["turn_every"])
        if new_turn:
            turn += 1
            size = cfg["start_size"]
            flushes_in_turn = 0
        else:
            size = min(cfg["cap"], size + rnd.randint(cfg["grow_min"], cfg["grow_max"]))
        off = rnd.randrange(0, 4096)
        body = pool[off:off + size]
        err = ""
        fields = {"s": session, "t": turn, "q": n_flush, "tm": repr(ts), "tw": repr(time.time())}
        try:
            pub.publish(fields, body)
            last_size[turn] = size
            n_ok += 1
        except Exception as e:  # брокер недоступен / OOM / таймаут доставки
            err = "pub:" + type(e).__name__
        te = time.monotonic()
        flushes_in_turn += 1
        n_flush += 1
        gap = (ts - prev_start) * 1000 if prev_start is not None else ""
        prev_start = ts
        if ts - start_at >= cfg["warmup"]:
            recs.append((round(ts - start_at, 4), round((te - ts) * 1000, 3),
                         round(gap, 3) if gap != "" else "",
                         "insert" if new_turn else "update", err, size))
        ideal = next_at + interval
        if te > ideal:
            debt_acc += te - ideal
            next_at = te
        else:
            next_at = ideal
    pub.close()
    out_q.put({"idx": idx, "recs": recs, "n_flush": n_flush, "n_ok": n_ok,
               "errors": n_flush - n_ok, "debt_ms": debt_acc * 1000,
               "last_size": {f"{session}:{t}": s for t, s in last_size.items()},
               "active_s": (prev_start - first_start) if prev_start and first_start else 0})


# ---------------------------------------------------------------- компактор

def db_init(path):
    con = sqlite3.connect(path, isolation_level=None, timeout=5)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=5000")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("CREATE TABLE IF NOT EXISTS messages (session TEXT, turn INTEGER,"
                " body TEXT, updated_at REAL, PRIMARY KEY(session, turn))")
    return con


UPSERT = ("INSERT INTO messages(session, turn, body, updated_at) VALUES (?,?,?,?) "
          "ON CONFLICT(session, turn) DO UPDATE SET body=excluded.body, "
          "updated_at=excluded.updated_at")


class RedisSub:
    def __init__(self, stream, count):
        import redis
        self.r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
        self.stream, self.count, self.last = stream, count, "0-0"

    def read(self, block_ms):
        res = self.r.xread({self.stream: self.last}, count=self.count, block=block_ms)
        out = []
        for _, entries in res or []:
            for eid, f in entries:
                hdr = {"s": f[b"s"].decode(), "t": int(f[b"t"]), "q": int(f[b"q"]),
                       "tm": float(f[b"tm"]), "tw": float(f[b"tw"])}
                out.append((hdr, f[b"b"].decode()))
                self.last = eid
        return out

    def after_commit(self):
        # Журнал подрезается: всё, что легло в SQLite, из потока удаляется.
        if self.last != "0-0":
            self.r.xtrim(self.stream, minid=self.last, approximate=False)

    def backlog(self):
        return max(0, self.r.xlen(self.stream) - 1)

    def close(self):
        self.r.close()


class KafkaSub:
    def __init__(self, topic, count):
        from confluent_kafka import Consumer, TopicPartition
        self.c = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP,
                           "group.id": "compactor-" + topic,
                           "auto.offset.reset": "earliest",
                           "enable.auto.commit": False,
                           "fetch.max.bytes": 52428800,
                           "max.partition.fetch.bytes": 10485760,
                           "fetch.wait.max.ms": 10})
        self.tp = TopicPartition(topic, 0, 0)
        self.c.assign([self.tp])
        self.count = count
        self.pos = 0
        self.TopicPartition = TopicPartition

    def read(self, block_ms):
        # consume(n, timeout) ждёт полную пачку или таймаут — это добавило бы
        # компактору искусственную задержку. Поэтому: ждём первое событие,
        # затем забираем всё, что уже пришло, без ожидания.
        first = self.c.poll(block_ms / 1000.0)
        if first is None:
            return []
        msgs = [first] + self.c.consume(num_messages=self.count - 1, timeout=0)
        out = []
        for m in msgs:
            if m.error():
                continue
            hdr_b, _, body = m.value().partition(b"\n")
            hdr = json.loads(hdr_b)
            hdr["tm"], hdr["tw"] = float(hdr["tm"]), float(hdr["tw"])
            out.append((hdr, body.decode()))
            self.pos = m.offset() + 1
        return out

    def after_commit(self):
        # Позиция компактора фиксируется в брокере после коммита в SQLite.
        if self.pos:
            self.c.commit(offsets=[self.TopicPartition(self.tp.topic, 0, self.pos)],
                          asynchronous=True)

    def backlog(self):
        _, hi = self.c.get_watermark_offsets(self.tp, timeout=1, cached=False)
        return max(0, hi - self.pos)

    def close(self):
        self.c.close()


def compactor(cfg, start_at, expected, out_q, ready):
    sub = (RedisSub if cfg["broker"] == "redis" else KafkaSub)(cfg["chan"], cfg["batch"])
    con = db_init(cfg["db"])
    ready.set()
    end_at = start_at + cfg["duration"]
    hard_deadline = end_at + cfg["drain_timeout"]
    ev_recs = []     # (t_pub_rel, e2e_mono_ms, e2e_wall_ms, batch_events)
    batches = []     # (t_rel, events, rows, commit_ms)
    lag_samples = []  # (t_rel, backlog_events)
    consumed, errors, last_commit, commits = 0, 0, None, 0
    busy_s = 0.0
    next_lag = [start_at]

    def sample_lag(now):
        # Раз в 0,25 с: неразобранные (для Redis — ещё не закоммиченные) события.
        # Опрашивается и внутри окна пачки, иначе замер попадал бы только на
        # момент сразу после коммита и занижал лаг при W > 0.
        if now >= next_lag[0] and now <= end_at + 1:
            try:
                lag_samples.append((round(now - start_at, 3), sub.backlog()))
            except Exception:
                pass
            next_lag[0] = now + 0.25

    while True:
        now = time.monotonic()
        exp = expected.value
        if exp >= 0 and consumed >= exp:
            break
        if now > hard_deadline:
            break
        sample_lag(now)
        try:
            evs = sub.read(50)
        except Exception:
            errors += 1
            time.sleep(0.05)
            continue
        if not evs:
            continue
        if cfg["batch_window_ms"] > 0:
            # Добираем журнал до конца окна, считая от прихода первого события
            # пачки. block >= 1 мс: у XREAD block=0 значит «ждать вечно».
            win_end = time.monotonic() + cfg["batch_window_ms"] / 1000.0
            while len(evs) < cfg["batch_max"]:
                now = time.monotonic()
                left_ms = (win_end - now) * 1000
                if left_ms < 1:
                    break
                sample_lag(now)
                try:
                    evs += sub.read(int(left_ms))
                except Exception:
                    errors += 1
                    break
        tb = time.monotonic()
        latest = {}
        for hdr, body in evs:
            latest[(hdr["s"], hdr["t"])] = (body, hdr["tw"])
        try:
            con.execute("BEGIN")
            con.executemany(UPSERT, [(s, t, b, tw) for (s, t), (b, tw) in latest.items()])
            con.execute("COMMIT")
        except sqlite3.DatabaseError:
            errors += 1
            try:
                con.execute("ROLLBACK")
            except sqlite3.DatabaseError:
                pass
            continue  # события пачки потеряны для состояния — увидим в state_mismatch
        tc_mono, tc_wall = time.monotonic(), time.time()
        sub.after_commit()
        busy_s += tc_mono - tb
        commits += 1
        consumed += len(evs)
        last_commit = tc_mono
        batches.append((round(tb - start_at, 4), len(evs), len(latest),
                        round((tc_mono - tb) * 1000, 3)))
        for hdr, _ in evs:
            ev_recs.append((round(hdr["tm"] - start_at, 4),
                            round((tc_mono - hdr["tm"]) * 1000, 3),
                            round((tc_wall - hdr["tw"]) * 1000, 3), len(evs)))
    sub.close()
    con.close()
    out_q.put({"compactor": True, "ev": ev_recs, "batches": batches, "lag": lag_samples,
               "consumed": consumed, "errors": errors, "busy_s": busy_s, "commits": commits,
               "drain_ms": max(0.0, (last_commit - end_at) * 1000) if last_commit else None,
               "finished": expected.value >= 0 and consumed >= expected.value})


# ---------------------------------------------------------------- точка

def setup_chan(broker, chan):
    if broker == "redis":
        import redis
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
        r.delete(chan)
        r.close()
        return
    from confluent_kafka.admin import AdminClient, NewTopic
    a = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    fs = a.create_topics([NewTopic(chan, num_partitions=1, replication_factor=1)])
    for f in fs.values():
        f.result(30)
    for _ in range(100):  # дождаться лидера партиции
        md = a.list_topics(chan, timeout=5)
        t = md.topics.get(chan)
        if t and not t.error and t.partitions and t.partitions[0].leader >= 0:
            return
        time.sleep(0.1)


def teardown_chan(broker, chan):
    try:
        if broker == "redis":
            import redis
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
            r.delete(chan)
            r.close()
        else:
            from confluent_kafka.admin import AdminClient
            a = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
            for f in a.delete_topics([chan]).values():
                f.result(30)
    except Exception as e:
        print(f"  teardown {chan}: {e}", file=sys.stderr)


def run_point(broker, n, fs_label, workdir, interval_ms=75, duration=30.0, warmup=5.0,
              start_size=1024, grow_min=200, grow_max=500, cap=64 * 1024,
              turn_every=160, seed=1, batch=500, drain_timeout=120.0,
              batch_window_ms=0.0, batch_max=20000,
              raw_csv=None, keep_dir=None, label=None, kafka_acks="1",
              kafka_linger_ms=0, kafka_wait="each"):
    label = label or f"event-log-{broker}"
    tag = uuid.uuid4().hex[:6]
    point_dir = os.path.join(workdir, f"{label}-n{n}-{tag}")
    os.makedirs(point_dir)
    chan = f"bench-{label}-n{n}-{tag}"
    db = os.path.join(point_dir, "state.db")
    cfg = dict(mode=label, label=label, broker=broker, n=n, fs=fs_label, chan=chan, db=db,
               interval_ms=interval_ms, duration=duration, warmup=warmup,
               start_size=start_size, grow_min=grow_min, grow_max=grow_max, cap=cap,
               turn_every=turn_every, seed=seed, batch=batch, drain_timeout=drain_timeout,
               batch_window_ms=batch_window_ms, batch_max=batch_max,
               kafka_acks=str(kafka_acks), kafka_linger_ms=int(kafka_linger_ms), kafka_wait=kafka_wait)
    setup_chan(broker, chan)
    ctx = mp.get_context("fork")
    q, cq = ctx.Queue(), ctx.Queue()
    expected = ctx.Value("q", -1)
    ready = ctx.Event()
    # Продюсерам Kafka нужно время на метаданные — старт позже, чем в этапе 1.
    start_at = time.monotonic() + 2.0 + 0.04 * n
    comp = ctx.Process(target=compactor, args=(cfg, start_at, expected, cq, ready), daemon=True)
    comp.start()
    ready.wait(30)
    procs = [ctx.Process(target=agent, args=(i, cfg, start_at, q), daemon=True) for i in range(n)]
    t_wall, wall0 = time.monotonic(), time.time()
    for p in procs:
        p.start()
    results = []
    deadline = start_at + duration + 60
    while len(results) < n and time.monotonic() < deadline:
        try:
            results.append(q.get(timeout=5))
        except Exception:
            if not any(p.is_alive() for p in procs) and q.empty():
                break
    for p in procs:
        p.join(timeout=5)
        if p.is_alive():
            p.kill()
            p.join()
    expected.value = sum(r["n_ok"] for r in results)
    comp_res = None
    try:
        comp_res = cq.get(timeout=drain_timeout + 30)
    except Exception:
        pass
    comp.join(timeout=10)
    if comp.is_alive():
        comp.kill()
        comp.join()
    wall = time.monotonic() - t_wall
    clock_drift = (time.time() - wall0) - wall

    s = bench.summarize(cfg, results, wall)
    s["journal"] = "WAL (компактор)"
    s["broker"] = broker
    s["batch_window_ms"] = batch_window_ms
    if broker == "kafka":
        s.update(kafka_acks=str(kafka_acks), kafka_linger_ms=int(kafka_linger_ms), kafka_wait=kafka_wait)
    s["clock_drift_s"] = round(clock_drift, 3)
    s["agents_reported"] = len(results)
    s["published"] = expected.value
    s.update(compactor_summary(cfg, comp_res, expected.value))
    s.update(state_check(db, results))
    s["integrity_bad_dbs"] = bench.check_integrity(point_dir, "one-file", keep_dir)
    if raw_csv:
        opener = gzip.open if raw_csv.endswith(".gz") else open
        with opener(raw_csv, "wt") as f:
            f.write("agent,t_rel_s,write_ms,gap_ms,kind,err,size_bytes\n")
            for res in results:
                for r in res["recs"]:
                    f.write(f"{res['idx']}," + ",".join(str(x) for x in r) + "\n")
        if comp_res:
            base = raw_csv[:-len(".csv.gz")] if raw_csv.endswith(".csv.gz") else raw_csv
            with gzip.open(base + "-e2e.csv.gz", "wt") as f:
                f.write("t_pub_rel_s,e2e_ms,e2e_wall_ms,batch_events\n")
                for r in comp_res["ev"]:
                    f.write(",".join(str(x) for x in r) + "\n")
            with gzip.open(base + "-batches.csv.gz", "wt") as f:
                f.write("t_rel_s,events,rows,commit_ms\n")
                for r in comp_res["batches"]:
                    f.write(",".join(str(x) for x in r) + "\n")
    teardown_chan(broker, chan)
    shutil.rmtree(point_dir, ignore_errors=True)
    return s


def compactor_summary(cfg, c, expected):
    if not c:
        return {"compactor_ok": False}
    lo, hi = cfg["warmup"], cfg["duration"]
    win = [r for r in c["ev"] if lo <= r[0] < hi]
    e2e = sorted(r[1] for r in win)
    drift = sorted(abs(r[2] - r[1]) for r in win)
    pct = bench.pct
    batches = [b for b in c["batches"] if lo <= b[0] < hi]
    meas = hi - lo
    ev_in = sum(b[1] for b in batches)
    rows_in = sum(b[2] for b in batches)
    commit_ms = sorted(b[3] for b in batches)
    lag_win = [l for t, l in c["lag"] if lo <= t < hi]
    return {
        "compactor_ok": bool(c["finished"]),
        "compacted": c["consumed"],
        "e2e_p50_ms": round(pct(e2e, 50), 2),
        "e2e_p90_ms": round(pct(e2e, 90), 2),
        "e2e_p99_ms": round(pct(e2e, 99), 2),
        "e2e_max_ms": round(e2e[-1], 1) if e2e else float("nan"),
        "e2e_gt75_frac": round(sum(1 for x in e2e if x > 75) / len(e2e), 5) if e2e else float("nan"),
        "e2e_gt1s_frac": round(sum(1 for x in e2e if x > 1000) / len(e2e), 5) if e2e else float("nan"),
        "clock_mono_wall_p99_ms": round(pct(drift, 99), 3),
        "clock_mono_wall_max_ms": round(drift[-1], 3) if drift else float("nan"),
        "comp_commits": c.get("commits"),           # все коммиты точки, вкл. прогрев и дозапись
        "comp_commits_win": len(batches),          # коммиты в окне измерения
        "comp_batches_per_s": round(len(batches) / meas, 2),
        "comp_events_per_batch": round(ev_in / len(batches), 2) if batches else float("nan"),
        "comp_coalesce": round(ev_in / rows_in, 3) if rows_in else float("nan"),
        "comp_rows_per_batch": round(rows_in / len(batches), 2) if batches else float("nan"),
        "comp_commit_p50_ms": round(pct(commit_ms, 50), 3),
        "comp_commit_p99_ms": round(pct(commit_ms, 99), 3),
        "comp_busy_frac": round(c["busy_s"] / (meas + (c["drain_ms"] or 0) / 1000 + lo), 4),
        "comp_events_per_s": round(ev_in / meas, 1),
        "lag_events_p50": round(pct(sorted(lag_win), 50), 1) if lag_win else float("nan"),
        "lag_events_p99": round(pct(sorted(lag_win), 99), 1) if lag_win else float("nan"),
        "lag_events_max": max(lag_win) if lag_win else float("nan"),
        "drain_ms": round(c["drain_ms"], 1) if c["drain_ms"] is not None else float("nan"),
        "compactor_errors": c["errors"],
    }


def state_check(db, results):
    """Совпадает ли итоговое состояние в SQLite с последним опубликованным текстом."""
    want = {}
    for r in results:
        want.update(r["last_size"])
    try:
        con = sqlite3.connect(db, timeout=5)
        got = {f"{s}:{t}": ln for s, t, ln in
               con.execute("SELECT session, turn, length(body) FROM messages")}
        con.close()
    except sqlite3.DatabaseError:
        return {"state_rows": -1, "state_mismatch": len(want)}
    bad = sum(1 for k, v in want.items() if got.get(k) != v)
    return {"state_rows": len(got), "state_mismatch": bad}


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--broker", required=True, choices=["redis", "kafka"])
    ap.add_argument("-n", type=int, default=5)
    ap.add_argument("--dir", required=True, help="где компактор держит state.db")
    ap.add_argument("--fs", default="unknown")
    ap.add_argument("--duration", type=float, default=30.0)
    ap.add_argument("--warmup", type=float, default=5.0)
    ap.add_argument("--batch", type=int, default=500, help="событий за одно чтение журнала")
    ap.add_argument("--batch-window-ms", type=float, default=0.0,
                    help="окно пачки компактора: сколько мс добирать журнал после "
                         "первого события перед коммитом (0 — коммит сразу)")
    ap.add_argument("--kafka-acks", choices=["0", "1", "all"], default="1")
    ap.add_argument("--kafka-linger-ms", type=int, default=0)
    ap.add_argument("--kafka-wait", choices=["each", "async"], default="each")
    a = ap.parse_args()
    os.makedirs(a.dir, exist_ok=True)
    s = run_point(a.broker, a.n, a.fs, a.dir, duration=a.duration, warmup=a.warmup,
                  batch=a.batch, batch_window_ms=a.batch_window_ms,
                  kafka_acks=a.kafka_acks, kafka_linger_ms=a.kafka_linger_ms,
                  kafka_wait=a.kafka_wait)
    json.dump(s, sys.stdout, ensure_ascii=False, indent=1)
    print()


if __name__ == "__main__":
    main()
