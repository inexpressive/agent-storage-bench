#!/usr/bin/env python3
"""Синтетический бенчмарк записи стриминг-вывода агентов (этап 1: SQLite / файлы).

N агентов = N отдельных процессов. Каждый — «стриминговая сессия»: сообщение
растёт, и не чаще раза в INTERVAL мс агент сбрасывает его ЦЕЛИКОМ в хранилище
(шаблон OpenResearch maybe_flush / OpenScience coalescer, см. README.md).

Режимы:
  one-file          все процессы пишут в одну SQLite (WAL, busy_timeout=5000,
                    synchronous=NORMAL), UPDATE полного текста своей строки;
                    каждые --turn-every сбросов — INSERT нового сообщения.
  one-file-append   общая SQLite, но после INSERT пишет только новые байты
                    сообщения в таблицу chunks.
  one-file-threads  та же схема сообщений, но агенты — потоки одного процесса.
  file-per-session  то же, но у каждого агента своя SQLite.
  json-rename       JSON-файл на сообщение: lock-файл O_EXCL, запись во
                    временный файл, os.replace (шаблон OpenScience storage.ts).

Расписание сброса — «по прошедшему времени», как в maybe_flush: следующий
сброс не раньше, чем через INTERVAL после начала предыдущего; если запись
затянулась, сброс идёт сразу после неё. Отставание от идеального графика
(k * INTERVAL) — накопленный «долг».

Только стандартная библиотека Python 3.
"""
import argparse
import gzip
import hashlib
import json
import multiprocessing as mp
import os
import random
import shutil
import sqlite3
import statistics
import sys
import time
import uuid

LOCK_ERRORS = ("locked", "busy")


# ---------------------------------------------------------------- хранилища

JOURNAL_MODE = os.environ.get("BENCH_JOURNAL", "WAL")  # WAL | TRUNCATE | DELETE


def sqlite_connect(path, busy_ms, wal_autocheckpoint=None):
    con = sqlite3.connect(path, timeout=busy_ms / 1000.0, isolation_level=None)
    con.execute(f"PRAGMA journal_mode={JOURNAL_MODE}")
    con.execute(f"PRAGMA busy_timeout={busy_ms}")
    con.execute("PRAGMA synchronous=NORMAL")
    if wal_autocheckpoint is not None:
        con.execute(f"PRAGMA wal_autocheckpoint={int(wal_autocheckpoint)}")
    return con


def sqlite_init(path, busy_ms, wal_autocheckpoint=None):
    con = sqlite_connect(path, busy_ms, wal_autocheckpoint)
    con.execute(
        "CREATE TABLE IF NOT EXISTS messages ("
        " id INTEGER PRIMARY KEY, session TEXT, turn INTEGER,"
        " body TEXT, updated_at REAL)"
    )
    con.close()


class SqliteStore:
    def __init__(self, path, session, busy_ms, wal_autocheckpoint=None):
        self.con = sqlite_connect(path, busy_ms, wal_autocheckpoint)
        self.session = session
        self.row = None

    def new_message(self, turn, body):
        cur = self.con.execute(
            "INSERT INTO messages(session, turn, body, updated_at) VALUES (?,?,?,?)",
            (self.session, turn, body, time.time()),
        )
        self.row = cur.lastrowid

    def flush(self, body):
        self.con.execute(
            "UPDATE messages SET body=?, updated_at=? WHERE id=?",
            (body, time.time(), self.row),
        )

    def close(self):
        self.con.close()


class SqliteAppendStore(SqliteStore):
    """Дельты: сброс = INSERT приращения в chunks. Обе записи сброса (чанк и
    updated_at сообщения) — одна транзакция, т. е. один коммит на сброс, как
    у one-file; иначе режим платил бы двумя коммитами и сравнение было бы
    нечестным."""

    def __init__(self, path, session, busy_ms, wal_autocheckpoint=None):
        super().__init__(path, session, busy_ms, wal_autocheckpoint)
        self.seq = 0

    def _tx(self, fn):
        self.con.execute("BEGIN IMMEDIATE")
        try:
            fn()
            self.con.execute("COMMIT")
        except BaseException:
            if self.con.in_transaction:
                self.con.execute("ROLLBACK")
            raise

    def _chunk(self, chunk):
        self.con.execute(
            "INSERT INTO chunks(session, msg_id, seq, chunk) VALUES (?,?,?,?)",
            (self.session, self.row, self.seq, chunk),
        )
        self.con.execute(
            "UPDATE messages SET updated_at=? WHERE id=?",
            (time.time(), self.row),
        )

    def new_message(self, turn, body):
        def tx():
            cur = self.con.execute(
                "INSERT INTO messages(session, turn, body, updated_at) VALUES (?,?,?,?)",
                (self.session, turn, "", time.time()),
            )
            self.row = cur.lastrowid
            self.seq = 0
            self._chunk(body)
        self._tx(tx)
        self.seq = 1

    def flush(self, chunk):
        self._tx(lambda: self._chunk(chunk))
        self.seq += 1


class JsonRenameStore:
    """Шаблон OpenScience: <target>.lock через O_EXCL, tmp-файл, rename."""

    def __init__(self, root, session, fsync):
        self.dir = os.path.join(root, session)
        os.makedirs(self.dir, exist_ok=True)
        self.session = session
        self.target = None
        self.turn = 0
        self.fsync = fsync

    def _lock(self):
        lock = self.target + ".lock"
        deadline = time.monotonic() + 10.0
        while True:
            try:
                fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
                os.write(fd, str(os.getpid()).encode())
                os.close(fd)
                return lock
            except FileExistsError:
                if time.monotonic() > deadline:
                    raise TimeoutError("lock timeout")
                time.sleep(random.uniform(0.010, 0.030))

    def _write(self, body):
        lock = self._lock()
        try:
            tmp = f"{self.target}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
            data = json.dumps({"session": self.session, "turn": self.turn,
                               "updated_at": time.time(), "text": body})
            with open(tmp, "w") as f:
                f.write(data)
                if self.fsync:
                    f.flush()
                    os.fsync(f.fileno())
            os.replace(tmp, self.target)
        finally:
            os.unlink(lock)

    def new_message(self, turn, body):
        self.turn = turn
        self.target = os.path.join(self.dir, f"msg-{turn:05d}.json")
        self._write(body)

    def flush(self, body):
        self._write(body)

    def close(self):
        pass


class JsonSharedRmwStore:
    """Общий JSON: read-modify-write с необязательным O_EXCL lock."""
    def __init__(self, root, session, lock_mode):
        self.target = os.path.join(root, "state.json")
        self.session = session
        self.lock_mode = lock_mode
        self.parse_errors = 0

    def flush(self):
        lock = self.target + ".lock"
        if self.lock_mode == "excl":
            deadline = time.monotonic() + 10.0
            while True:
                try:
                    fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
                    os.close(fd)
                    break
                except FileExistsError:
                    if time.monotonic() > deadline:
                        raise TimeoutError("lock timeout")
                    time.sleep(random.uniform(.001, .005))
        try:
            try:
                with open(self.target) as f:
                    state = json.load(f)
                if not isinstance(state, dict):
                    raise ValueError("JSON root is not an object")
            except FileNotFoundError:
                state = {}
            except (json.JSONDecodeError, ValueError):
                self.parse_errors += 1
                return False
            state[self.session] = state.get(self.session, 0) + 1
            tmp = f"{self.target}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
            with open(tmp, "w") as f:
                json.dump(state, f)
            os.replace(tmp, self.target)
            return True
        finally:
            if self.lock_mode == "excl" and os.path.exists(lock):
                os.unlink(lock)

    def close(self):
        pass


# ---------------------------------------------------------------- агент

def agent(idx, cfg, start_at, out_q):
    rnd = random.Random(cfg["seed"] * 1000 + idx)
    session = f"s{idx:03d}"
    mode = cfg["mode"]
    acked = {}
    if mode in ("one-file", "one-file-threads"):
        store = SqliteStore(os.path.join(cfg["dir"], "orx.db"), session, cfg["busy_ms"], cfg.get("wal_autocheckpoint"))
    elif mode == "one-file-append":
        store = SqliteAppendStore(os.path.join(cfg["dir"], "orx.db"), session, cfg["busy_ms"], cfg.get("wal_autocheckpoint"))
    elif mode == "file-per-session":
        p = os.path.join(cfg["dir"], f"{session}.db")
        sqlite_init(p, cfg["busy_ms"])
        store = SqliteStore(p, session, cfg["busy_ms"], cfg.get("wal_autocheckpoint"))
    elif mode == "json-shared-rmw":
        store = JsonSharedRmwStore(cfg["dir"], session, cfg["rmw_lock"])
    else:
        store = JsonRenameStore(cfg["dir"], session, cfg["fsync"])

    # Неповторяющийся текст: SQLite не сжимает, но пусть страницы будут честными.
    alphabet = "abcdefghijklmnopqrstuvwxyz      .,\n"
    pool = "".join(rnd.choice(alphabet) for _ in range(cfg["cap"] + 4096))

    interval = cfg["interval_ms"] / 1000.0
    duration = cfg["duration"]
    warmup = cfg["warmup"]
    # Случайная фаза: сессии не синхронизированы между собой.
    t0 = start_at + rnd.uniform(0, interval)
    end_at = start_at + duration

    recs = []  # (t_rel_start, dur_ms, gap_ms, kind, err)
    turn = 0
    size = cfg["start_size"]
    body = ""
    previous_body = ""  # one-file-append: текст, уже лежащий в базе
    flushes_in_turn = 0
    errors_total = 0
    n_flush = 0
    rmw_acked = 0
    debt_acc = 0.0

    while time.monotonic() < t0:
        time.sleep(min(0.05, max(0.0, t0 - time.monotonic())))

    prev_start = None
    next_at = t0
    first_start = None
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
        if mode == "one-file-append":
            if new_turn:
                body = pool[off:off + size]
            else:
                body += pool[off:off + max(0, size - len(body))]
        else:
            body = pool[off:off + size]
        err = ""
        try:
            if mode == "json-shared-rmw":
                ok = store.flush()
                if ok:
                    rmw_acked += 1
            elif new_turn:
                store.new_message(turn, body)
            else:
                store.flush(body[len(previous_body):] if mode == "one-file-append" else body)
            previous_body = body  # только после успешной записи: иначе дельта потеряется
            if mode in ("one-file", "one-file-threads", "file-per-session"):
                acked[store.row] = (len(body), hashlib.sha1(body.encode()).hexdigest())
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            err = "locked" if any(k in msg for k in LOCK_ERRORS) else "sqlite:" + msg[:40]
            errors_total += 1
            if new_turn:  # строка не создана — повторим INSERT на следующем сбросе
                turn -= 1
                flushes_in_turn = cfg["turn_every"]
        except sqlite3.DatabaseError as e:
            # «database disk image is malformed» и прочая порча — агент не
            # падает, считаем ошибку и продолжаем писать (как живой сервис).
            msg = str(e).lower()
            err = "corrupt" if "malformed" in msg or "corrupt" in msg else "dberr:" + msg[:40]
            errors_total += 1
            if new_turn:
                turn -= 1
                flushes_in_turn = cfg["turn_every"]
        except (TimeoutError, OSError) as e:
            err = "io:" + type(e).__name__
            errors_total += 1
        te = time.monotonic()
        flushes_in_turn += 1
        n_flush += 1
        gap = (ts - prev_start) * 1000 if prev_start is not None else ""
        prev_start = ts
        if ts - start_at >= warmup:
            recs.append((round(ts - start_at, 4), round((te - ts) * 1000, 3),
                         round(gap, 3) if gap != "" else "",
                         "insert" if new_turn else "update", err, size))
        # Якорный график: джиттер sleep не накапливается. Если запись вылезла
        # за следующий слот — сброс сразу после неё, без «догоняющей» пачки;
        # переплаченное время копится как долг.
        ideal = next_at + interval
        if te > ideal:
            debt_acc += te - ideal
            next_at = te
        else:
            next_at = ideal

    store.close()
    # Долг: суммарное отставание от графика из-за записей, вылезших за слот.
    debt_ms = debt_acc * 1000
    res = {"idx": idx, "recs": recs, "n_flush": n_flush,
           "errors": errors_total, "debt_ms": debt_ms,
           "active_s": (prev_start - first_start) if prev_start and first_start else 0}
    if mode == "one-file-append":  # для сверки склейки чанков после точки
        res.update(last_turn=turn, last_body=previous_body)
    if mode in ("one-file", "one-file-threads", "file-per-session"):
        res["acked"] = acked
    if mode == "json-shared-rmw":
        res.update(rmw_acked=rmw_acked, json_parse_errors=store.parse_errors)
    out_q.put(res)


# ---------------------------------------------------------------- агрегирование

def pct(sorted_vals, p):
    if not sorted_vals:
        return float("nan")
    k = (len(sorted_vals) - 1) * p / 100.0
    lo = int(k)
    hi = min(lo + 1, len(sorted_vals) - 1)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo)


def sync_stalls(results, n):
    """Сколько окон по 0,25 с, где запись >250 мс закончилась у >=50% агентов сразу.

    Одновременный стопор всех процессов (в т.ч. в file-per-session, где им
    нечего делить) — признак паузы VM / сброса ФС хоста, а не конкуренции
    за один файл."""
    if n < 5:
        return 0
    buckets = {}
    for res in results:
        for r in res["recs"]:
            if r[1] > 250:
                b = int((r[0] + r[1] / 1000) / 0.25)
                buckets.setdefault(b, set()).add(res["idx"])
    return sum(1 for v in buckets.values() if len(v) >= max(3, n // 2))


def summarize(cfg, results, wall_s):
    recs = [r for res in results for r in res["recs"]]
    durs = sorted(r[1] for r in recs)
    gaps = sorted(r[2] for r in recs if r[2] != "")
    interval = cfg["interval_ms"]
    measured_s = cfg["duration"] - cfg["warmup"]
    n = len(recs)
    locked = sum(1 for r in recs if r[4] == "locked")
    corrupt = sum(1 for r in recs if r[4] == "corrupt")
    other_err = sum(1 for r in recs if r[4] and r[4] not in ("locked", "corrupt"))
    target_rate = cfg["n"] * 1000.0 / interval  # сбросов/с в сумме
    achieved_rate = n / measured_s if measured_s > 0 else float("nan")
    debts = sorted(res["debt_ms"] for res in results)
    return {
        "mode": cfg["mode"], "label": cfg["label"], "fs": cfg["fs"], "n": cfg["n"],
        "journal": JOURNAL_MODE if cfg["mode"] not in ("json-rename", "json-shared-rmw") else "",
        "interval_ms": interval, "duration_s": cfg["duration"], "warmup_s": cfg["warmup"],
        "flushes": n,
        "write_p50_ms": round(pct(durs, 50), 3),
        "write_p90_ms": round(pct(durs, 90), 3),
        "write_p99_ms": round(pct(durs, 99), 3),
        "write_max_ms": round(durs[-1], 3) if durs else float("nan"),
        "write_mean_ms": round(statistics.fmean(durs), 3) if durs else float("nan"),
        # Доля сбросов, чья запись сама не уложилась в 75 мс (окно OpenResearch).
        "late75_frac": round(sum(1 for d in durs if d > 75.0) / n, 5) if n else float("nan"),
        # Доля сбросов, не уложившихся в собственный интервал режима.
        "late_own_frac": round(sum(1 for d in durs if d > interval) / n, 5) if n else float("nan"),
        "gap_p50_ms": round(pct(gaps, 50), 3),
        "gap_p99_ms": round(pct(gaps, 99), 3),
        "rate_ratio": round(achieved_rate / target_rate, 4) if target_rate else float("nan"),
        "debt_p50_ms": round(pct(debts, 50), 1),
        "debt_max_ms": round(debts[-1], 1) if debts else float("nan"),
        "locked_errors": locked, "corrupt_errors": corrupt, "other_errors": other_err,
        "gap_max_ms": round(gaps[-1], 1) if gaps else float("nan"),
        "sync_stalls": sync_stalls(results, cfg["n"]),
        "wall_s": round(wall_s, 1),
        "loadavg_1m": round(os.getloadavg()[0], 2),
    }


# ---------------------------------------------------------------- прогон точки

def check_integrity(point_dir, mode, keep_dir):
    """PRAGMA integrity_check по всем базам точки после прогона.
    Испорченные базы копируются в keep_dir как улика. Возвращает число плохих."""
    if mode == "json-rename":
        bad = 0
        for root, _, files in os.walk(point_dir):
            for fn in files:
                if fn.endswith(".json"):
                    try:
                        with open(os.path.join(root, fn)) as f:
                            json.load(f)
                    except Exception:
                        bad += 1
        return bad
    bad = 0
    for fn in sorted(os.listdir(point_dir)):
        if not fn.endswith(".db"):
            continue
        path = os.path.join(point_dir, fn)
        try:
            con = sqlite3.connect(path, timeout=5)
            res = con.execute("PRAGMA integrity_check").fetchall()
            con.close()
            ok = res == [("ok",)]
        except sqlite3.DatabaseError:
            ok = False
        if not ok:
            bad += 1
            if keep_dir:
                dst = os.path.join(keep_dir, os.path.basename(point_dir))
                os.makedirs(dst, exist_ok=True)
                for ext in ("", "-wal", "-shm"):
                    if os.path.exists(path + ext):
                        shutil.copy2(path + ext, dst)
    return bad


def check_append_state(point_dir, results):
    """Сверить склейку чанков последнего сообщения каждого агента."""
    con = sqlite3.connect(os.path.join(point_dir, "orx.db"), timeout=5)
    bad = 0
    for res in results:
        row = con.execute(
            "SELECT id FROM messages WHERE session=? AND turn=?",
            (f"s{res['idx']:03d}", res["last_turn"]),
        ).fetchone()
        chunks = ("" if row is None else "".join(r[0] for r in con.execute(
            "SELECT chunk FROM chunks WHERE session=? AND msg_id=? ORDER BY seq",
            (f"s{res['idx']:03d}", row[0]),
        )))
        if row is None or chunks != res["last_body"]:
            bad += 1
    con.close()
    return bad


def acked_state_check(point_dir, mode, results):
    """Сравнить строки с последними подтверждёнными телами агентов."""
    wants = {}
    for res in results:
        path = (os.path.join(point_dir, "orx.db") if mode != "file-per-session"
                else os.path.join(point_dir, f"s{res['idx']:03d}.db"))
        for row_id, state in res.get("acked", {}).items():
            wants[(path, row_id)] = state
    missing = stale = 0
    for (path, row_id), (length, digest) in wants.items():
        try:
            uri = "file:" + os.path.abspath(path) + "?mode=ro"
            con = sqlite3.connect(uri, uri=True, timeout=5)
            try:
                row = con.execute("SELECT body FROM messages WHERE id=?", (row_id,)).fetchone()
            finally:
                con.close()
            if row is None:
                missing += 1
            elif row[0] is None or len(row[0]) != length or hashlib.sha1(row[0].encode()).hexdigest() != digest:
                stale += 1
        except (sqlite3.DatabaseError, OSError):
            missing += 1
    total = len(wants)
    return {"acked_rows": total, "acked_missing": missing, "acked_stale": stale,
            "silent_loss_frac": round((missing + stale) / total, 5) if total else float("nan")}


def run_point(mode, n, fs_label, workdir, interval_ms=75, duration=30.0, warmup=5.0,
              start_size=1024, grow_min=200, grow_max=500, cap=64 * 1024,
              turn_every=160, busy_ms=5000, fsync=False, seed=1, label=None,
              raw_csv=None, keep_dir=None, rmw_lock="none", wal_autocheckpoint=None):
    label = label or (mode if mode != "json-rename" else f"json-rename-{interval_ms}")
    point_dir = os.path.join(workdir, f"{label}-n{n}-{uuid.uuid4().hex[:6]}")
    os.makedirs(point_dir)
    cfg = dict(mode=mode, n=n, fs=fs_label, label=label, dir=point_dir,
               interval_ms=interval_ms, duration=duration, warmup=warmup,
               start_size=start_size, grow_min=grow_min, grow_max=grow_max,
               cap=cap, turn_every=turn_every, busy_ms=busy_ms, fsync=fsync,
               seed=seed, rmw_lock=rmw_lock, wal_autocheckpoint=wal_autocheckpoint)
    if mode in ("one-file", "one-file-threads", "one-file-append"):
        sqlite_init(os.path.join(point_dir, "orx.db"), busy_ms, wal_autocheckpoint)
    if mode == "one-file-append":
        con = sqlite3.connect(os.path.join(point_dir, "orx.db"), isolation_level=None)
        con.execute("CREATE TABLE chunks (session TEXT, msg_id INTEGER, seq INTEGER, chunk TEXT)")
        con.close()

    start_at = time.monotonic() + 1.0 + 0.02 * n
    wal_samples, stop_sampler = [], None
    if wal_autocheckpoint is not None and mode in ("one-file", "one-file-threads", "one-file-append"):
        import threading
        stop_sampler = threading.Event()
        wal_path = os.path.join(point_dir, "orx.db-wal")
        # WAL после checkpoint не усыхает (SQLite пишет его заново с начала),
        # поэтому событие — смена соли в заголовке WAL (байты 16–24): её
        # меняет сброс WAL на первой записи после полного checkpoint.
        def sample_wal():
            prev = None
            while not stop_sampler.is_set():
                now = time.monotonic()
                try:
                    size_bytes = os.path.getsize(wal_path)
                    with open(wal_path, "rb") as f:
                        salt = f.read(24)[16:24]
                except OSError:
                    size_bytes, salt = 0, None
                event = (prev is not None and salt is not None and salt != prev)
                wal_samples.append((now, size_bytes, event))
                if salt:
                    prev = salt
                stop_sampler.wait(.05)
        sampler = threading.Thread(target=sample_wal, daemon=True)
        sampler.start()
    t_wall = time.monotonic()
    wall0 = time.time()
    results = []
    if mode == "one-file-threads":
        import queue
        import threading
        q = queue.Queue()
        workers = [threading.Thread(target=agent, args=(i, cfg, start_at, q), daemon=True)
                   for i in range(n)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(duration + 60 + busy_ms / 1000.0)
        results = [q.get_nowait() for _ in range(q.qsize())]
    else:
        ctx = mp.get_context("fork")
        q = ctx.Queue()
        procs = [ctx.Process(target=agent, args=(i, cfg, start_at, q), daemon=True)
                 for i in range(n)]
        for p in procs:
            p.start()
        deadline = start_at + duration + 60 + busy_ms / 1000.0
        try:
            while len(results) < n:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    results.append(q.get(timeout=min(remaining, 5)))
                except Exception:
                    if not any(p.is_alive() for p in procs) and q.empty():
                        break
        finally:
            for p in procs:
                p.join(timeout=5)
                if p.is_alive():
                    p.kill()
                    p.join()
    wall = time.monotonic() - t_wall
    if stop_sampler:
        stop_sampler.set()
        sampler.join()
    # Скачок часов / пауза VM: настенные часы и монотонные разошлись.
    clock_drift = (time.time() - wall0) - wall
    summary = summarize(cfg, results, wall)
    summary["clock_drift_s"] = round(clock_drift, 3)
    summary["agents_reported"] = len(results)
    if wal_autocheckpoint is not None:
        checkpoints = [t for t, _, event in wal_samples if event]
        stall_windows = {}
        for res in results:
            for r in res["recs"]:
                if r[1] > 250:
                    bucket = int((r[0] + r[1] / 1000) / .25)
                    stall_windows.setdefault(bucket, set()).add(res["idx"])
        threshold = max(3, n // 2)
        stall_starts = [start_at + bucket * .25 for bucket, agents in stall_windows.items()
                        if n >= 5 and len(agents) >= threshold]
        summary.update(wal_checkpoints=len(checkpoints),
                       wal_max_bytes=max((b for _, b, _ in wal_samples), default=0),
                       stalls_near_checkpoint=sum(1 for st in stall_starts if any(0 <= st-cp <= .25 for cp in checkpoints)),
                       stalls_total=len(stall_starts), wal_autocheckpoint=wal_autocheckpoint)
        if raw_csv:
            base = raw_csv[:-len(".csv.gz")] if raw_csv.endswith(".csv.gz") else raw_csv
            with gzip.open(base + "-wal.csv.gz", "wt") as f:
                f.write("t_mono,bytes\n")
                for t, b, _ in wal_samples:
                    f.write(f"{t:.6f},{b}\n")
    if raw_csv:
        opener = gzip.open if raw_csv.endswith(".gz") else open
        with opener(raw_csv, "wt") as f:
            f.write("agent,t_rel_s,write_ms,gap_ms,kind,err,size_bytes\n")
            for res in results:
                for r in res["recs"]:
                    f.write(f"{res['idx']}," + ",".join(str(x) for x in r) + "\n")
    summary["integrity_bad_dbs"] = check_integrity(point_dir, mode, keep_dir)
    if mode in ("one-file", "one-file-threads", "file-per-session"):
        summary.update(acked_state_check(point_dir, mode, results))
    if mode == "json-shared-rmw":
        try:
            with open(os.path.join(point_dir, "state.json")) as f:
                final_state = json.load(f)
            final_ok = isinstance(final_state, dict)
        except Exception:
            final_state, final_ok = {}, False
        acked = sum(r.get("rmw_acked", 0) for r in results)
        final = sum(v for v in final_state.values() if isinstance(v, int)) if final_ok else 0
        lost = acked - final
        summary.update(rmw_acked=acked, rmw_final=final, rmw_lost=lost,
                       rmw_lost_frac=round(lost / acked, 5) if acked else float("nan"),
                       json_parse_errors=sum(r.get("json_parse_errors", 0) for r in results),
                       final_parse_ok=final_ok)
    if mode == "one-file-append":
        summary["append_state_mismatches"] = check_append_state(point_dir, results)
    shutil.rmtree(point_dir, ignore_errors=True)
    return summary


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mode", required=True,
                    choices=["one-file", "one-file-append", "one-file-threads",
                             "file-per-session", "json-rename", "json-shared-rmw"])
    ap.add_argument("-n", type=int, default=5, help="число агентов-процессов")
    ap.add_argument("--dir", required=True, help="рабочий каталог для баз/файлов")
    ap.add_argument("--fs", default="unknown", help="метка файловой системы")
    ap.add_argument("--interval-ms", type=int, default=75)
    ap.add_argument("--duration", type=float, default=30.0)
    ap.add_argument("--warmup", type=float, default=5.0)
    ap.add_argument("--start-size", type=int, default=1024)
    ap.add_argument("--grow-min", type=int, default=200)
    ap.add_argument("--grow-max", type=int, default=500)
    ap.add_argument("--cap", type=int, default=64 * 1024)
    ap.add_argument("--turn-every", type=int, default=160,
                    help="через сколько сбросов начинается новое сообщение (INSERT)")
    ap.add_argument("--busy-ms", type=int, default=5000)
    ap.add_argument("--fsync", action="store_true", help="fsync tmp-файла в json-rename")
    ap.add_argument("--rmw-lock", choices=["none", "excl"], default="none")
    ap.add_argument("--wal-autocheckpoint", type=int, default=None)
    ap.add_argument("--raw-csv", help="сохранить посбросовые записи в CSV")
    a = ap.parse_args()
    os.makedirs(a.dir, exist_ok=True)
    s = run_point(a.mode, a.n, a.fs, a.dir, interval_ms=a.interval_ms,
                  duration=a.duration, warmup=a.warmup, start_size=a.start_size,
                  grow_min=a.grow_min, grow_max=a.grow_max, cap=a.cap,
                  turn_every=a.turn_every, busy_ms=a.busy_ms, fsync=a.fsync,
                  raw_csv=a.raw_csv, rmw_lock=a.rmw_lock,
                  wal_autocheckpoint=a.wal_autocheckpoint)
    json.dump(s, sys.stdout, ensure_ascii=False, indent=1)
    print()


if __name__ == "__main__":
    main()
